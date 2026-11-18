#!/usr/bin/env python3
"""Owner-authenticated Pebble audio and text receiver.

Audio is archived, transcribed by the configured provider and either forwarded to a
webhook or handed to the command pipeline. Provider credentials stay outside the
archive and are supplied by the service environment or credential files.
"""
from __future__ import annotations
import argparse
import contextlib
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import hmac
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
import uuid
import urllib.error
import urllib.request
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from intake import IntakeError, MIMES, parse_input
from process_io import ProcessError, run as run_bounded
import settings as settings_schema
import status_feed
import action_registry

BRIDGE = Path(__file__).with_name('bridge.mjs')
TERMINAL = ('forwarded', 'skipped')


class Failure(Exception):
    def __init__(self, code, retryable=False, response=None):
        super().__init__(code)
        self.code, self.retryable, self.response = code, retryable, response


def expand(value):
    return Path(value).expanduser()


def load_config(filename):
    # The operator selects this file. Nix/user-namespace ownership mappings and
    # file permissions are not config validity; validate its contents instead.
    with expand(filename).open('rb') as stream:
        raw = stream.read(65537)
    if len(raw) > 65536:
        raise Failure('config_too_large')
    config = json.loads(raw)
    base = {'version', 'listen', 'token_file', 'state_dir', 'max_body_bytes', 'max_pending', 'max_archive_bytes',
            'min_free_bytes', 'retry_delays_secs', 'ffmpeg', 'node', 'forward', 'chime'}
    fields = base | ({'mai'} if isinstance(config, dict) and config.get('version') == 1 else {'transcription'})
    if not isinstance(config, dict) or set(config) not in (fields, fields | {'commands'}):
        raise Failure('invalid_config_fields')
    try:
        config = settings_schema.normalize(config)
    except settings_schema.ConfigError as exc:
        raise Failure(exc.code) from exc
    if 'commands' in config:
        from command_pipeline import validate_settings
        validate_settings(config['commands'])
        # The launcher resolves the same local/package policy as Cacophony's
        # service. Keep an explicit operator config override authoritative.
        if config['commands']['caco']['binary'] == 'caco' and os.environ.get('PEBBLE_CACO_BIN'):
            config['commands']['caco']['binary'] = os.environ['PEBBLE_CACO_BIN']
        extraction = config['commands'].get('extractor')
        if extraction is not None and extraction['enabled'] and not extraction['base_url']:
            # Freeze the endpoint, but never the API key, with each new input.
            extraction['base_url'] = os.environ.get('OPENAI_BASE_URL', '')
        voices = config['commands']['speech']['voices']
        for key in voices:
            if key in os.environ:
                voices[key] = os.environ[key]
        validate_settings(config['commands'])
    sections = {
        'listen': {'host', 'port', 'request_timeout_secs', 'max_connections'},
        'chime': {'enabled', 'argv', 'server', 'pitch', 'duration', 'gain'},
    }
    for name, keys in sections.items():
        if not isinstance(config[name], dict) or set(config[name]) != keys:
            raise Failure('invalid_config_fields')
    if config['listen']['host'] != '127.0.0.1':
        raise Failure('receiver_must_bind_loopback')
    bounds = [(config['listen']['port'], 0, 65535), (config['listen']['max_connections'], 1, 8),
              (config['listen']['request_timeout_secs'], 1, 120), (config['max_body_bytes'], 1, 16 * 1024 * 1024),
              (config['max_pending'], 1, 1024), (config['max_archive_bytes'], 1, 1024 ** 4),
              (config['min_free_bytes'], 1, 1024 ** 4)]
    for value, low, high in bounds:
        if type(value) is not int or not low <= value <= high:
            raise Failure('invalid_config_limit')
    delays = config['retry_delays_secs']
    if not isinstance(delays, list) or not 1 <= len(delays) <= 5 or any(type(n) is not int or not 1 <= n <= 300 for n in delays):
        raise Failure('invalid_retry_policy')
    sound = config['chime']
    if type(sound['enabled']) is not bool or not isinstance(sound['argv'], list) or not 1 <= len(sound['argv']) <= 4:
        raise Failure('invalid_chime')
    if any(not isinstance(arg, str) or not arg for arg in sound['argv']):
        raise Failure('invalid_chime')
    if not 0 < sound['duration'] <= 1 or not 0 <= sound['gain'] <= 1 or not 40 <= sound['pitch'] <= 4000:
        raise Failure('invalid_chime')
    for value in (config['token_file'], config['state_dir'], config['ffmpeg'], config['node']):
        if not isinstance(value, str) or not value or len(value) > 4096:
            raise Failure('invalid_config_string')
    if not config.get('commands', {}).get('enabled') and not config['forward']['url']:
        raise Failure('forward_url_required')
    if not isinstance(sound['server'], str) or len(sound['server']) > 4096 or '\0' in sound['server']:
        raise Failure('invalid_config_string')
    return config


def read_secret(filename):
    # Thin operator-managed credential indirection is allowed, including SOPS.
    fd = os.open(expand(filename), os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        st = os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise Failure('unsafe_token_file')
        value = stream.read(16385).strip()
    if not value or len(value) > 16384:
        raise Failure('invalid_token_file')
    return value


def parse_capture(body, content_type):
    # Keep the legacy audio-only contract, sharing the binary-safe parser with
    # command mode. Index connection tests contain no audio and are harmless.
    try:
        metadata, audio = parse_input(body, content_type)
    except IntakeError as exc:
        raise Failure(exc.code) from exc
    if metadata['test']:
        return metadata, audio
    if not metadata['recordedAt']:
        raise Failure('invalid_recording_metadata')
    if audio is None:
        metadata['audio_mime'] = 'text/plain'
        audio = b''
    elif not audio:
        raise Failure('supported_audio_required')
    return {key: metadata[key] for key in ('recordedAt', 'client', 'transcription', 'audio_mime')}, audio


def run_process(argv, timeout, output_limit=0):
    """Bound process trees, time, and captured output; never retain stderr."""
    proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE if output_limit else subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    chunks, count = [], 0
    deadline = time.monotonic() + timeout
    settled = False
    try:
        if output_limit:
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise Failure('process_timeout', True)
                    for key, _ in selector.select(min(remaining, 0.5)):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:
                            selector.unregister(key.fileobj)
                            continue
                        count += len(data)
                        if count > output_limit:
                            raise Failure('process_output_limit')
                        chunks.append(data)
        try:
            code = proc.wait(timeout=max(0.01, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise Failure('process_timeout', True) from exc
        if code:
            raise Failure('process_failed')
        settled = True
        return b''.join(chunks)
    finally:
        if not settled:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
        if proc.poll() is None:
            proc.wait()
        if proc.stdout:
            proc.stdout.close()


def play_upload_chime(config):
    if not config['enabled']:
        return 'disabled'
    try:
        argv = [str(expand(p)) for p in config['argv']]
        if config['server']:
            argv += ['--server', config['server']]
        run_process(argv + ['--pitch', str(config['pitch']), '--duration', str(config['duration']), '--gain', str(config['gain'])], 3)
        return 'played'
    except (OSError, Failure):
        return 'unavailable'


def call_bridge(operation, config, input_path, directory):
    config_path = directory / 'bridge-config.json'
    config_path.write_text(json.dumps(config))
    config_path.chmod(0o600)
    timeout = config['transcription']['timeout_secs'] if operation == 'transcribe' else config['forward']['timeout_secs']
    result = run_process([str(expand(config['node'])), str(BRIDGE), operation, str(config_path), str(input_path)],
                         timeout + 10, 2 * 1024 * 1024)
    try:
        value = json.loads(result)
    except (ValueError, UnicodeError) as exc:
        raise Failure('invalid_adapter_response') from exc
    if not value.get('ok'):
        error = value.get('error', {})
        code = error.get('code', 'adapter_error')
        if not isinstance(code, str) or not code.replace('_', '').isalnum() or len(code) > 80:
            code = 'adapter_error'
        raise Failure(code, error.get('retryable') is True, value.get('response'))
    return value


def transcription_command(config, wav):
    settings = config['transcription']
    try:
        result = run_bounded([str(expand(settings['command'][0])), *settings['command'][1:]], wav,
                             timeout=settings['timeout_secs'], max_output=262144, env=os.environ.copy())
    except ProcessError as exc:
        retryable = exc.code in ('command_timeout', 'command_failed')
        raise Failure('transcription_' + exc.code.removeprefix('command_'), retryable) from exc
    try:
        raw = result.stdout.decode('utf-8')
        response = json.loads(raw) if settings['output'] == 'json' else {'text': raw}
    except (UnicodeError, ValueError) as exc:
        raise Failure('invalid_transcription_response') from exc
    text = response.get('text') if isinstance(response, dict) else None
    if not isinstance(text, str) or not text.strip() or len(text.encode()) > 16384:
        raise Failure('empty_or_oversized_transcript')
    return {'ok': True, 'text': text.strip(), 'model': settings['model'], 'response': response, 'elapsed_ms': result.elapsed_ms}


def transcribe(capture):
    config = settings_schema.normalize(json.loads(capture['settings']))
    with tempfile.TemporaryDirectory(prefix='pebble-stt-') as tmp:
        directory = Path(tmp)
        source, wav_path = directory / 'input', directory / 'audio.wav'
        source.write_bytes(capture['audio'])
        fmt = MIMES[capture['audio_mime']]
        args = [str(expand(config['ffmpeg'])), '-nostdin', '-v', 'error', '-threads', '1', '-max_alloc', '67108864',
                '-protocol_whitelist', 'file,pipe', '-f', fmt]
        if fmt == 'mov':
            args += ['-enable_drefs', '0', '-use_absolute_path', '0']
        args += ['-i', str(source), '-map', '0:a:0', '-map_metadata', '-1', '-vn', '-sn', '-dn', '-t', str(config['transcription']['max_audio_secs'] + 1),
                 '-ac', '1', '-ar', '24000', '-c:a', 'pcm_s16le', str(wav_path)]
        try:
            run_process(args, 30)
            with wave.open(str(wav_path), 'rb') as wav:
                if wav.getnframes() / wav.getframerate() > config['transcription']['max_audio_secs']:
                    raise Failure('audio_too_long')
        except (OSError, wave.Error) as exc:
            raise Failure('audio_conversion_failed') from exc
        if config['transcription']['provider'] == 'command':
            return transcription_command(config, wav_path.read_bytes())
        return call_bridge('transcribe', config, wav_path, directory)


def forward(capture):
    config = settings_schema.normalize(json.loads(capture['settings']))
    with tempfile.TemporaryDirectory(prefix='pebble-forward-') as tmp:
        directory = Path(tmp)
        payload = directory / 'payload.json'
        payload.write_bytes(capture['forward_body'])
        return call_bridge('forward', config, payload, directory)


class Archive:
    def __init__(self, config):
        from change_signal import ChangeSignal
        self.changes = ChangeSignal()
        self.config = config
        self.root = expand(config['state_dir'])
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        st = self.root.stat()
        if st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise Failure('unsafe_state_directory')
        self.filename = self.root / 'archive.sqlite3'
        if not self.filename.exists():
            with contextlib.suppress(FileExistsError):
                fd = os.open(self.filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                os.close(fd)
        st = self.filename.stat()
        if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise Failure('unsafe_archive_file')
        with self.db() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS captures (
                seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, identity TEXT UNIQUE NOT NULL,
                received REAL NOT NULL, updated REAL NOT NULL, recorded_at TEXT NOT NULL, client TEXT NOT NULL,
                audio_mime TEXT NOT NULL, audio BLOB NOT NULL, device_transcript TEXT NOT NULL,
                settings TEXT NOT NULL, phase TEXT NOT NULL, chime TEXT NOT NULL DEFAULT 'pending',
                transcribe_attempts INTEGER NOT NULL DEFAULT 0, forward_attempts INTEGER NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0, mai_text TEXT, mai_response TEXT,
                forward_body BLOB, forward_receipt TEXT, error TEXT);
              CREATE TABLE IF NOT EXISTS requests (
                seq INTEGER PRIMARY KEY, received REAL NOT NULL, capture_id TEXT, content_type TEXT NOT NULL,
                body BLOB NOT NULL, error TEXT);
              CREATE TABLE IF NOT EXISTS audit (
                seq INTEGER PRIMARY KEY, at REAL NOT NULL, capture_id TEXT, action TEXT NOT NULL);
              CREATE INDEX IF NOT EXISTS capture_phase ON captures(phase, seq);
            ''')
        for directory in (self.root, self.root.parent):
            fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    @contextlib.contextmanager
    def db(self):
        db = sqlite3.connect(self.filename, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        committed = False
        try:
            with db:
                yield db
            committed = db.total_changes > 0
        finally:
            db.close()
            if committed:
                self.changes.committed()

    def accept(self, body, content_type):
        started = time.monotonic()
        if not 0 < len(body) <= self.config['max_body_bytes']:
            raise Failure('body_size_invalid')
        error, metadata, audio = None, None, None
        try:
            metadata, audio = parse_capture(body, content_type)
        except (Failure, ValueError, UnicodeError, TypeError, RecursionError) as exc:
            error = exc.code if isinstance(exc, Failure) else 'malformed_payload'
        if metadata and metadata.get('test'):
            return {'id': None, 'phase': 'probe', 'test': True, 'archived': False, 'error': None, 'duplicate': False}
        identity = None
        text_input = bool(metadata and metadata.get('audio_mime') == 'text/plain')
        if metadata:
            content = metadata['transcription'] if text_input else hashlib.sha256(audio).hexdigest()
            identity_parts = [metadata['recordedAt'], metadata['client'], 'text', content] if text_input else [
                metadata['recordedAt'], metadata['client'], content]
            identity = hashlib.sha256(json.dumps(identity_parts).encode()).hexdigest()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT id,phase FROM captures WHERE identity=?', (identity,)).fetchone() if identity else None
            pending = db.execute("SELECT count(*) FROM captures WHERE phase NOT IN ('forwarded','skipped')").fetchone()[0]
            reserve = 2 * len(body) + (pending + (0 if old else 1)) * 3 * 1024 * 1024
            used = sum(p.stat().st_size for p in self.root.glob('archive.sqlite3*'))
            if used + reserve > self.config['max_archive_bytes']:
                raise Failure('archive_quota_reached')
            if shutil.disk_usage(self.root).free - reserve < self.config['min_free_bytes']:
                raise Failure('archive_disk_low')
            if metadata and not old and pending >= self.config['max_pending']:
                raise Failure('queue_full')
            capture_id = old['id'] if old else uuid.uuid4().hex if metadata else None
            now = time.time()
            if metadata and not old:
                forward_body = None
                if text_input:
                    forward_body = json.dumps({'transcription': metadata['transcription'], 'recordedAt': metadata['recordedAt'],
                                               'client': metadata['client'], 'audio_capture_id': capture_id,
                                               'transcript_source': 'client-text'}, ensure_ascii=False).encode()
                    if len(forward_body) > 32768:
                        raise Failure('forward_payload_too_large')
                db.execute('''INSERT INTO captures(id,identity,received,updated,recorded_at,client,audio_mime,audio,
                              device_transcript,settings,phase,mai_text,forward_body) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                           (capture_id, identity, now, now, metadata['recordedAt'], metadata['client'], metadata['audio_mime'],
                            audio, metadata['transcription'], json.dumps(self.config), 'admitted',
                            metadata['transcription'] if text_input else None, forward_body))
            cursor = db.execute('INSERT INTO requests(received,capture_id,content_type,body,error) VALUES(?,?,?,?,?)',
                                (now, capture_id, content_type, body, error))
            request_id = cursor.lastrowid
        return {'id': capture_id, 'request_id': request_id, 'duplicate': bool(old), 'input': 'text' if text_input else 'audio',
                'phase': old['phase'] if old else 'rejected' if error else 'admitted', 'error': error,
                'archive_ms': round((time.monotonic() - started) * 1000, 2)}

    def update(self, capture_id, **values):
        allowed = {'phase', 'chime', 'transcribe_attempts', 'forward_attempts', 'retry_at', 'mai_text', 'mai_response',
                   'forward_body', 'forward_receipt', 'error'}
        if not set(values) <= allowed:
            raise Failure('invalid_archive_update')
        values['updated'] = time.time()
        with self.db() as db:
            db.execute('UPDATE captures SET ' + ','.join(k + '=?' for k in values) + ' WHERE id=?', (*values.values(), capture_id))

    def get(self, capture_id):
        with self.db() as db:
            row = db.execute('SELECT * FROM captures WHERE id=?', (capture_id,)).fetchone()
        if not row:
            raise Failure('capture_not_found')
        return dict(row)

    def head(self):
        with self.db() as db:
            row = db.execute("SELECT * FROM captures WHERE phase NOT IN ('forwarded','skipped') ORDER BY seq LIMIT 1").fetchone()
        return dict(row) if row else None

    def recover(self):
        # Only the single worker owner calls this, before starting request threads.
        # Read-only transcription can be repeated; forwarding reuses the frozen
        # body, whose recordedAt/text/client identity the first hook deduplicates.
        with self.db() as db:
            db.execute("UPDATE captures SET phase='queued',chime='interrupted' WHERE phase='admitted'")
            db.execute("UPDATE captures SET phase='queued' WHERE phase='transcribing'")
            db.execute("UPDATE captures SET phase='ready' WHERE phase='forwarding'")

    def status(self):
        with self.db() as db:
            counts = dict(db.execute('SELECT phase,count(*) FROM captures GROUP BY phase'))
            row = db.execute("SELECT id,phase,error,retry_at,chime FROM captures WHERE phase NOT IN ('forwarded','skipped') ORDER BY seq LIMIT 1").fetchone()
            requests = db.execute('SELECT count(*) FROM requests').fetchone()[0]
        return {'captures': counts, 'requests': requests, 'head': dict(row) if row else None}

    def resolve(self, capture_id, action):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT phase,forward_body FROM captures WHERE id=?', (capture_id,)).fetchone()
            if not row or row['phase'] != 'blocked':
                raise Failure('only_blocked_capture_can_be_resolved')
            phase = 'skipped' if action == 'skip' else 'ready' if row['forward_body'] else 'queued'
            db.execute('UPDATE captures SET phase=?,error=NULL,retry_at=0,transcribe_attempts=0,forward_attempts=0 WHERE id=?', (phase, capture_id))
            db.execute('INSERT INTO audit(at,capture_id,action) VALUES(?,?,?)', (time.time(), capture_id, action))

    def export(self, capture_id, destination):
        capture = self.get(capture_id)
        destination.mkdir(mode=0o700, parents=True, exist_ok=False)
        suffix = 'm4a' if MIMES[capture['audio_mime']] == 'mov' else 'wav'
        (destination / ('audio.' + suffix)).write_bytes(capture.pop('audio'))
        (destination / 'pebble-transcript.txt').write_text(capture['device_transcript'])
        if capture['mai_text'] is not None:
            (destination / 'mai-transcript.txt').write_text(capture['mai_text'])
        if capture['mai_response'] is not None:
            (destination / 'mai-response.json').write_text(capture['mai_response'])
        forward_body = capture.pop('forward_body')
        if forward_body:
            (destination / 'forward-payload.json').write_bytes(forward_body)
        (destination / 'metadata.json').write_text(json.dumps(capture, indent=2))
        with self.db() as db:
            for request in db.execute('SELECT * FROM requests WHERE capture_id=? ORDER BY seq', (capture_id,)):
                (destination / f"request-{request['seq']}.body").write_bytes(request['body'])
                (destination / f"request-{request['seq']}.json").write_text(json.dumps({k: request[k] for k in request.keys() if k != 'body'}, indent=2))


def admit(archive, body, content_type, sound=play_upload_chime):
    receipt = archive.accept(body, content_type)
    if receipt['error'] or receipt['phase'] == 'probe':
        return receipt
    if not receipt['duplicate']:
        archive.update(receipt['id'], chime='attempting')
        result = sound(archive.config['chime'])
        phase = 'ready' if receipt.get('input') == 'text' else 'queued'
        archive.update(receipt['id'], chime=result, phase=phase)
        receipt['phase'] = phase
        receipt['chime'] = result
    return receipt


class StopEvent(threading.Event):
    """Linearize stop with claim/launch, not with external stage execution."""
    def __init__(self, on_stop=None):
        super().__init__()
        self.gate = threading.RLock()
        self.on_stop = on_stop

    def set(self):
        with self.gate:
            super().set()
        if self.on_stop:
            self.on_stop()


class Worker:
    def __init__(self, archive, transcriber=transcribe, forwarder=forward, processor=None):
        self.archive, self.transcriber, self.forwarder = archive, transcriber, forwarder
        self.processor = processor
        self.wake = threading.Event()
        from change_signal import for_archive
        self.stop = StopEvent(for_archive(archive).wake)
        self.fatal = None
        self.stage_workers = archive.config.get('commands', {}).get('stage_workers', 4) if processor is not None else 1
        self.activity = {'active_stages': 0, 'server_speech_active': False}
        self.thread = threading.Thread(target=self.run, name='pebble-audio-worker', daemon=True)

    def step(self):
        if self.processor is not None:
            return self.processor.step()
        capture = self.archive.head()
        if not capture or capture['phase'] in ('admitted', 'blocked') or capture['retry_at'] > time.time():
            return False
        config = settings_schema.normalize(json.loads(capture['settings']))
        # Pin routing/model choices, not executable store paths which may be
        # retired by a later Nix generation. Retrying uses the current runtime.
        config['node'] = self.archive.config['node']
        config['ffmpeg'] = self.archive.config['ffmpeg']
        settings_schema.refresh_transcription_runtime(config, self.archive.config)
        capture['settings'] = json.dumps(config)
        operation = 'transcribe' if capture['phase'] == 'queued' else 'forward'
        counter = operation + '_attempts'
        count = capture[counter] + 1
        if count > len(config['retry_delays_secs']):
            self.archive.update(capture['id'], phase='blocked', error='retry_budget_exhausted')
            return False
        self.archive.update(capture['id'], phase='transcribing' if operation == 'transcribe' else 'forwarding', **{counter: count})
        try:
            if operation == 'transcribe':
                result = self.transcriber(capture)
                text = result['text']
                if not isinstance(text, str) or not text.strip() or len(text.encode()) > 16384:
                    raise Failure('invalid_transcript')
                payload = json.dumps({'transcription': text, 'recordedAt': capture['recorded_at'], 'client': capture['client'],
                                      'audio_capture_id': capture['id'], 'transcript_source': result.get('model') or config['transcription']['model']}, ensure_ascii=False).encode()
                if len(payload) > 32768:
                    raise Failure('forward_payload_too_large')
                self.archive.update(capture['id'], phase='ready', mai_text=text, mai_response=json.dumps(result.get('response')),
                                    forward_body=payload, error=None, retry_at=0)
            else:
                result = self.forwarder(capture)
                self.archive.update(capture['id'], phase='forwarded', forward_receipt=json.dumps(result), error=None, retry_at=0)
        except Exception as exc:
            error = exc if isinstance(exc, Failure) else Failure(type(exc).__name__)
            retry = error.retryable and count < len(config['retry_delays_secs'])
            values = {'phase': ('queued' if operation == 'transcribe' else 'ready') if retry else 'blocked',
                      'retry_at': time.time() + config['retry_delays_secs'][count - 1] if retry else 0, 'error': error.code}
            if error.response is not None:
                if operation == 'transcribe':
                    values['mai_response'] = json.dumps(error.response)
                else:
                    values['forward_receipt'] = json.dumps({'ok': False, 'response': error.response, 'error': error.code})
            self.archive.update(capture['id'], **values)
        return True

    def fail(self):
        with self.stop.gate:
            self.fatal = 'worker_storage_failure'
            self.stop.set()
        self.wake.set()

    def run_commands(self):
        # The process-level worker.lock encloses this entire coordinator and
        # pool lifetime. There is no unbounded executor queue: at most N IDs are
        # claimed, and only this coordinator selects/releases their next stage.
        pool = ThreadPoolExecutor(max_workers=self.stage_workers, thread_name_prefix='pebble-stage')
        active = {}
        def publish():
            self.activity = {'active_stages': len(active), 'server_speech_active': any(speech for _, speech in active.values())}
        def completed(future):
            # Stop at the failure boundary, not only when the coordinator next
            # polls its futures (it may currently be selecting another input).
            if not future.cancelled() and future.exception() is not None:
                self.fail()
            else:
                self.wake.set()
        def stage(record):
            if not self.stop.is_set():
                return self.processor.step(record=record)
            return False  # Claimed before stop, but not yet begun: leave durable work pending.
        try:
            while not self.stop.is_set():
                self.wake.clear()
                for input_id, (future, _) in list(active.items()):
                    if future.done():
                        future.result()  # Persistence failure stops new admissions/claims.
                        del active[input_id]
                publish()
                while len(active) < self.stage_workers and not self.stop.is_set():
                    record = self.archive.next_work(active, server_speech_busy=self.activity['server_speech_active'])
                    if record is None:
                        break
                    speech = json.loads(record['metadata']).get('speech_owner', 'server') != 'client'
                    # stop.set() and launch share this tiny gate. It never spans
                    # SQLite, model, speech, Caco or process I/O.
                    with self.stop.gate:
                        if self.stop.is_set():
                            break
                        active[record['id']] = (None, speech)
                        publish()  # Claim is visible before a pool thread can run.
                        try:
                            future = pool.submit(stage, record)
                        except Exception:
                            del active[record['id']]
                            publish()
                            raise
                        active[record['id']] = (future, speech)
                    future.add_done_callback(completed)
                self.wake.wait(.1)
        except Exception:
            self.fail()
        finally:
            # All already-running stages retain their bounded helper timeouts.
            # Drain concurrently, not N serial timeout budgets. Do not release
            # worker.lock while a pool thread could still publish a receipt.
            pool.shutdown(wait=True, cancel_futures=True)
            for future, _ in active.values():
                if not future.cancelled() and future.exception() is not None:
                    self.fail()
            active.clear()
            publish()

    def run(self):
        try:
            if self.processor is not None:
                self.run_commands()
                return
            while not self.stop.is_set():
                self.wake.clear()
                if not self.step():
                    self.wake.wait(0.5)
        except Exception:
            self.fail()


class Server(ThreadingHTTPServer):
    daemon_threads = False
    request_queue_size = 16

    def __init__(self, archive, worker, address=None, sound=play_upload_chime, *, ingestor=None, routes=None, route_profiles=None):
        self.archive, self.worker, self.sound = archive, worker, sound
        self.ingestor = ingestor
        self.routes = routes or {'/': 'audio', '/hooks/global/pebble-audio': 'audio'}
        self.route_profiles = route_profiles or {}
        from triage_store import Turns
        self.triage = Turns(archive) if ingestor is not None else None
        self.wait_limit = max(0, min(4, archive.config['listen']['max_connections'] - 2))
        self.session_wait_limit = 2
        self.triage_slots = threading.BoundedSemaphore(self.wait_limit)
        self.wait_lock = threading.Lock()
        self.session_waits = {}
        self.stream_slots = threading.BoundedSemaphore(1)
        self.slots = threading.BoundedSemaphore(archive.config['listen']['max_connections'])
        super().__init__(address or (archive.config['listen']['host'], archive.config['listen']['port']), Handler)

    def acquire_wait(self, session):
        # Shared by native SSE, CAPI waits and legacy snapshot SSE. The lock is
        # only accounting; no database read or socket write occurs under it.
        with self.wait_lock:
            if self.session_waits.get(session, 0) >= self.session_wait_limit or not self.triage_slots.acquire(blocking=False):
                return False
            self.session_waits[session] = self.session_waits.get(session, 0) + 1
            return True

    def release_wait(self, session):
        with self.wait_lock:
            count = self.session_waits[session] - 1
            if count:
                self.session_waits[session] = count
            else:
                del self.session_waits[session]
            self.triage_slots.release()

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, address)
        except Exception:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()

    def handle_error(self, *_):
        pass


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        timeout = self.server.archive.config['listen']['request_timeout_secs']
        self.connection.settimeout(timeout)
        def expire():
            with contextlib.suppress(OSError):
                self.connection.shutdown(socket.SHUT_RDWR)
        self.deadline = threading.Timer(timeout, expire)
        self.deadline.daemon = True
        self.deadline.start()

    def finish(self):
        self.deadline.cancel()
        super().finish()

    def log_message(self, *_):
        pass

    def reply(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def authenticated(self):
        try:
            wanted = b'Bearer ' + read_secret(self.server.archive.config['token_file'])
        except (OSError, Failure):
            self.reply(503, {'error': 'credential_unavailable'})
            return False
        values = self.headers.get_all('Authorization', [])
        valid = len(values) == 1 and hmac.compare_digest(values[0].encode('latin1'), wanted)
        if not valid:
            self.reply(401, {'error': 'unauthorized'})
        return valid

    def do_GET(self):
        if not self.authenticated():
            return
        import triage_http
        if triage_http.handle(self, 'GET'):
            return
        from urllib.parse import urlsplit, parse_qs
        parsed = urlsplit(self.path)
        status_path = parsed.path.rstrip('/') or '/'
        if status_path in status_feed.STATUS_PATHS | status_feed.EVENT_PATHS:
            if self.server.ingestor is None:
                return self.reply(404, {'error': 'status_requires_command_pipeline'})
            try:
                query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True, max_num_fields=3)
                if set(query) - {'limit', 'include_text', 'seconds'} or any(len(v) != 1 for v in query.values()):
                    raise ValueError()
                limit = int(query.get('limit', ['5'])[0])
                include = query.get('include_text', ['false'])[0]
                seconds = int(query.get('seconds', ['25'])[0])
                if not 1 <= limit <= 10 or include not in ('true', 'false') or not 1 <= seconds <= 25:
                    raise ValueError()
            except ValueError:
                return self.reply(400, {'error': 'invalid_status_query'})
            if status_path in status_feed.EVENT_PATHS:
                return self.status_stream(limit, include == 'true', seconds)
            try:
                return self.reply(200, status_feed.snapshot(self.server.archive, self.server.worker, limit=limit, include_text=include == 'true'))
            except Exception:
                return self.reply(503, {'error': 'status_unavailable'})
        if self.path.startswith('/receipts/') and hasattr(self.server.archive, 'receipt'):
            input_id = self.path.removeprefix('/receipts/')
            if len(input_id) != 32 or any(char not in '0123456789abcdef' for char in input_id):
                return self.reply(404, {'error': 'not_found'})
            try:
                return self.reply(200, self.server.archive.receipt(input_id))
            except IntakeError:
                return self.reply(404, {'error': 'not_found'})
        if self.path != '/health':
            return self.reply(404, {'error': 'not_found'})
        result = self.server.archive.status()
        result.update(worker_alive=self.server.worker.thread.is_alive(), worker_error=self.server.worker.fatal,
                      azure_speech_auth=os.environ.get('AZURE_SPEECH_AUTH', 'key'),
                      stage_workers=self.server.worker.stage_workers, **self.server.worker.activity)
        if self.server.ingestor is not None:
            result['caco_binary'] = self.server.archive.config['commands']['caco']['binary']
            result['caco_capability_checks'] = 'none'
        self.reply(200, result)

    def status_stream(self, limit, include_text, seconds):
        # One bounded observer at a time; reserve the other connection slots for
        # intake. Slow/disconnected clients cannot own the worker or its lock.
        if self.server.archive.config['listen']['max_connections'] < 2 or not self.server.stream_slots.acquire(blocking=False):
            return self.reply(503, {'error': 'status_stream_busy'})
        shared = self.server.archive.config['listen']['max_connections'] >= 3
        if shared and not self.server.acquire_wait('__legacy_status__'):
            self.server.stream_slots.release()
            return self.reply(503, {'error': 'status_stream_busy'})
        try:
            try:
                current = status_feed.snapshot(self.server.archive, self.server.worker, limit=limit, include_text=include_text)
            except Exception:
                return self.reply(503, {'error': 'status_unavailable'})
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Accel-Buffering', 'no')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            self.wfile.write(b'retry: 5000\n\n' + status_feed.event(current))
            self.wfile.flush()
            revision = current['revision']
            deadline = time.monotonic() + min(seconds, max(1, self.server.archive.config['listen']['request_timeout_secs'] - 1))
            while time.monotonic() < deadline and not self.server.worker.stop.wait(min(1, max(0, deadline-time.monotonic()))):
                # A rotated/revoked bearer cannot retain an established stream.
                try:
                    wanted = b'Bearer ' + read_secret(self.server.archive.config['token_file'])
                    if not hmac.compare_digest(self.headers['Authorization'].encode('latin1'), wanted):
                        break
                except (OSError, Failure):
                    break
                current = status_feed.snapshot(self.server.archive, self.server.worker, limit=limit, include_text=include_text)
                self.wfile.write(status_feed.event(current) if current['revision'] != revision else b': heartbeat\n\n')
                self.wfile.flush()
                revision = current['revision']
        except (OSError, ValueError, sqlite3.Error):
            pass  # Closed observer; never changes worker/input/speech state.
        finally:
            if shared:
                self.server.release_wait('__legacy_status__')
            self.server.stream_slots.release()

    def do_POST(self):
        if not self.authenticated():
            return
        import triage_http
        if triage_http.handle(self, 'POST'):
            return
        from urllib.parse import urlsplit, parse_qs
        try:
            url = urlsplit(self.path)
            query = parse_qs(url.query, keep_blank_values=True, strict_parsing=True, max_num_fields=4)
            if set(query) - {'dry_run'}:
                raise ValueError('unknown_query')
            headers = self.headers.get_all('X-Pebble-Dry-Run', [])
            values = query.get('dry_run', [])
            if len(headers) > 1 or len(values) > 1:
                raise ValueError('duplicate_mode')
            modes = [value.strip().lower() for value in headers + values]
            if any(value not in ('true', 'false') for value in modes) or len(set(modes)) > 1:
                raise ValueError('invalid_mode')
            dry_run = 'true' in modes
        except ValueError:
            return self.reply(400, {'error': 'invalid_dry_run_mode'})
        path = url.path.rstrip('/') or '/'
        actions = None
        routes = self.server.routes
        if self.server.ingestor is not None:
            try:
                actions = action_registry.catalog(self.server.archive.config['commands'])
                routes = {**routes, **action_registry.routes(actions)}
            except action_registry.RegistryError as exc:
                return self.reply(503, {'error': str(exc)})
        if path not in routes:
            return self.reply(404, {'error': 'not_found'})
        if dry_run and self.server.ingestor is None:
            return self.reply(400, {'error': 'dry_run_requires_command_pipeline'})
        if self.server.worker.fatal:
            return self.reply(503, {'error': self.server.worker.fatal})
        try:
            if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
                return self.reply(411, {'error': 'content_length_required'})
            length = int(self.headers['Content-Length'])
            if not 0 < length <= self.server.archive.config['max_body_bytes']:
                return self.reply(413, {'error': 'body_too_large'})
            content_type = self.headers.get('Content-Type', '')
            if len(content_type) > 512 or any(c in content_type for c in '\r\n'):
                return self.reply(400, {'error': 'invalid_content_type'})
            body = self.rfile.read(length)
            if len(body) != length:
                return self.reply(400, {'error': 'incomplete_body'})
            if self.server.ingestor is None:
                receipt = admit(self.server.archive, body, content_type, self.server.sound)
            else:
                keys = self.headers.get_all('Idempotency-Key', [])
                if len(keys) > 1:
                    return self.reply(400, {'error': 'ambiguous_idempotency_key'})
                receipt = self.server.ingestor(body, content_type, routes[path], keys[0] if keys else None, dry_run=dry_run, actions=actions,
                                               profile=self.server.route_profiles.get(path, 'default'))
                if receipt['phase'] == 'probe':
                    receipt['endpoint_action'] = routes[path]
            if receipt['phase'] != 'probe':
                self.server.worker.wake.set()
            status = 409 if receipt['error'] == 'input_identity_conflict' else 400 if receipt['error'] else 200 if receipt['phase'] == 'probe' else 202
            self.reply(status, receipt)
        except (Failure, IntakeError) as exc:
            self.reply(503, {'error': exc.code})
        except (ValueError, TimeoutError):
            self.reply(400, {'error': 'invalid_request'})
        except Exception:
            self.reply(503, {'error': 'archive_unavailable'})


def health_check(config, wait_secs=0):
    deadline = time.monotonic() + wait_secs
    last_error = 'not_ready'
    while True:
        try:
            token = read_secret(config['token_file']).decode('utf-8')
            request = urllib.request.Request(
                f"http://127.0.0.1:{config['listen']['port']}/health",
                headers={'Authorization': 'Bearer ' + token})
            with urllib.request.urlopen(request, timeout=2) as response:
                data = json.loads(response.read(65536))
            if data.get('worker_alive') and not data.get('worker_error'):
                result = {'ready': True, 'worker_alive': True}
                if 'caco_capability_checks' in data:
                    result['caco_capability_checks'] = data['caco_capability_checks']
                if 'azure_speech_auth' in data:
                    result['azure_speech_auth'] = data['azure_speech_auth']
                return result
            last_error = 'worker_not_ready'
        except urllib.error.HTTPError as exc:
            last_error = f'http_{exc.code}'
            exc.close()
        except Exception as exc:
            last_error = exc.code if isinstance(exc, Failure) else type(exc).__name__
        if time.monotonic() >= deadline:
            raise Failure('health_not_ready_' + last_error)
        time.sleep(min(.25, max(0, deadline - time.monotonic())))


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', default=str(Path.home() / '.config/pebble-audio/config.json'))
    parser.add_argument('action', choices=['serve', 'health', 'status', 'export', 'retry', 'skip', 'cancel', 'validate'])
    parser.add_argument('--id')
    parser.add_argument('--output')
    parser.add_argument('--confirmed', action='store_true')
    parser.add_argument('--wait', type=int, default=0, help='health readiness deadline in seconds (0–120)')
    args = parser.parse_args()
    if not 0 <= args.wait <= 120:
        parser.error('--wait must be between 0 and 120 seconds')
    try:
        config = load_config(args.config)
    except Exception as exc:
        print(json.dumps({'error': exc.code if isinstance(exc, (Failure, IntakeError)) else type(exc).__name__, 'phase': 'configuration'}))
        raise SystemExit(78)
    if args.action == 'health':
        print(json.dumps(health_check(config, args.wait)))
        return
    command_mode = config.get('commands', {}).get('enabled') is True
    if args.action == 'validate':
        print(json.dumps({'valid': True, 'model': config['transcription']['model'], 'transcription': config['transcription']['provider'], 'bind': config['listen'],
                          'pipeline': 'commands' if command_mode else 'legacy'}))
        return
    archive = Archive(config)
    processor, routes, route_profiles = None, None, None
    if command_mode:
        from command_pipeline import CommandArchive, Processor
        archive = CommandArchive(archive)
        processor = Processor(archive, transcribe)
        routes = settings_schema.route_table(config['commands'])
        route_profiles = settings_schema.route_profiles(config['commands'])
    if args.action == 'status':
        print(json.dumps(archive.receipt(args.id) if command_mode and args.id else archive.status()))
    elif args.action == 'export':
        if not args.id or not args.output:
            parser.error('export requires --id and a new --output directory')
        archive.export(args.id, expand(args.output))
    elif args.action in ('retry', 'skip', 'cancel'):
        if not args.id or not args.confirmed:
            parser.error('retry/skip/cancel require --id and --confirmed after inspecting the capture')
        if args.action == 'cancel':
            if not command_mode:
                parser.error('cancel requires the shared command pipeline')
            with archive.db() as db:
                db.execute('BEGIN IMMEDIATE')
                archive.cancel(args.id, connection=db)
            print(json.dumps({'id': args.id, 'action': 'cancel', 'receipt': archive.receipt(args.id)}))
        else:
            archive.resolve(args.id, args.action)
            print(json.dumps({'id': args.id, 'action': args.action}))
    else:
        with (archive.root / 'worker.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            archive.recover()
            worker = Worker(archive, processor=processor)
            server = Server(archive, worker, ingestor=archive.accept if command_mode else None, routes=routes, route_profiles=route_profiles)
            server.timeout = 0.5
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, lambda *_: (worker.stop.set(), worker.wake.set()))
            worker.thread.start()
            print(json.dumps({'event': 'pebble_audio_listening', 'pipeline': 'commands' if command_mode else 'legacy',
                              'host': server.server_address[0], 'port': server.server_address[1]}), flush=True)
            try:
                while not worker.stop.is_set():
                    server.handle_request()
            finally:
                server.server_close()
                worker.stop.set()
                worker.wake.set()
                deadlines = [config['transcription']['timeout_secs'] + 40, config['forward']['timeout_secs'] + 10]
                if command_mode:
                    speech_timeout = config['commands']['speech']['timeout_secs']
                    jev_timeout = max([config['commands']['jev']['timeout_secs'], *(p.get('timeout_secs', 0) for p in config['commands']['jev_profiles'].values())])
                    deadlines += [speech_timeout + config['commands']['caco']['timeout_secs'] + 10,
                                  speech_timeout * 4 + jev_timeout + 10,
                                  config['commands'].get('extractor', {}).get('timeout_secs', 0) + 10,
                                  speech_timeout + 310]  # includes bounded custom commands
                worker.thread.join(timeout=max(deadlines) + 5)
            if worker.thread.is_alive():
                raise Failure('worker_shutdown_timeout')
            if worker.fatal:
                raise Failure(worker.fatal)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'error': exc.code if isinstance(exc, (Failure, IntakeError)) else type(exc).__name__}))
        raise SystemExit(1)
