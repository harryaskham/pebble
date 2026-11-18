import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import selectors
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import receiver
import funnel


def wav_bytes(seconds=0.1):
    output = io.BytesIO()
    with wave.open(output, 'wb') as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(24000)
        wav.writeframes(b'\x00\x00' * int(24000 * seconds))
    return output.getvalue()


def multipart(audio=None, boundary='pebble-boundary', text='device rough transcript', recorded='1770000000000', mime='audio/wav'):
    body = b''
    for name, value in [('transcription', text), ('recordedAt', recorded), ('client', 'ring')]:
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode()
    body += f'--{boundary}\r\nContent-Disposition: form-data; name="audio"; filename="../../never-use-this.wav"\r\nContent-Type: {mime}\r\n\r\n'.encode()
    body += (audio if audio is not None else wav_bytes()) + f'\r\n--{boundary}--\r\n'.encode()
    return body, 'multipart/form-data; boundary=' + boundary


def mai_form(headers, body):
    """Fields of a MAI-Transcribe request, which must be multipart/form-data
    (MAI-Audio asked for all traffic to move off JSON audio_url, Nov 2026)."""
    import email.parser
    import email.policy
    ctype = {k.lower(): v for k, v in headers.items()}.get('content-type', '')
    assert ctype.startswith('multipart/form-data'), f'MAI transcription must be multipart, got {ctype!r}'
    message = email.parser.BytesParser(policy=email.policy.HTTP).parsebytes(
        b'Content-Type: ' + ctype.encode() + b'\r\n\r\n' + body)
    fields = {}
    for part in message.iter_parts():
        data = part.get_payload(decode=True)
        fields[part.get_param('name', header='content-disposition')] = data if part.get_filename() else data.decode()
    return fields


class Peer:
    def __init__(self):
        self.calls = []
        self.first_bodies = []
        self.mai_gate = None
        self.mai_seen = threading.Event()
        self.mai_status = 200
        self.mai_text = 'higher quality words'
        self.mai_count = 0
        self.redirect = False
        self.first_ids = {}
        peer = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                peer.calls.append((self.path, dict(self.headers), body))
                if peer.redirect:
                    self.send_response(302)
                    self.send_header('Location', peer.url + '/redirected')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                if self.path == '/mai/v1/audio/transcriptions':
                    peer.mai_count += 1
                    peer.mai_seen.set()
                    if peer.mai_gate:
                        peer.mai_gate.wait(5)
                    status = peer.mai_status
                    data = {'text': peer.mai_text, 'language': 'en', 'duration': 0.1}
                else:
                    status = 200
                    peer.first_bodies.append(body)
                    value = json.loads(body)
                    key = (value['transcription'], value['recordedAt'], value['client'])
                    peer.first_ids.setdefault(key, f'{len(peer.first_ids) + 1:032x}')
                    data = {'ok': True, 'exit_code': 0, 'handler': 'bash', 'stdout_tail': json.dumps({'id': peer.first_ids[key], 'state': 'queued'})}
                encoded = json.dumps(data).encode()
                with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                    self.send_response(status)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(encoded)))
                    self.end_headers()
                    self.wfile.write(encoded)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def close(self):
        if self.mai_gate: self.mai_gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class ReceiverTest(unittest.TestCase):
    def setUp(self):
        # The fleet sets Entra speech auth (AZURE_STT_AUTH etc.); inherited, it
        # switches the bridge from the fixture key to a real token broker.
        environment = patch.dict(os.environ)
        environment.start()
        self.addCleanup(environment.stop)
        for name in [k for k in os.environ if k.startswith(('AZURE_', 'AZ_TOKEN_', 'PI_STT_'))]:
            del os.environ[name]
        self.tmp = tempfile.TemporaryDirectory(prefix='pebble-audio-test-')
        self.root = Path(self.tmp.name)
        self.config = receiver.load_config(ROOT / 'tests/fixtures/config-v1.json')
        self.config.update(state_dir=str(self.root / 'state'), token_file=str(self.root / 'webhook-token'), min_free_bytes=1,
                           retry_delays_secs=[1, 1, 1])
        self.config['listen'].update(port=0, request_timeout_secs=2)
        self.config['chime']['enabled'] = False
        self.token = 'fixture-webhook-secret'
        self.azure_key = 'fixture-azure-secret'
        for name, value in [('webhook-token', self.token), ('azure-key', self.azure_key), ('endpoint', 'https://example.services.ai.azure.com')]:
            file = self.root / name
            file.write_text(value)
            file.chmod(0o600)
        self.config['transcription'].update(key_file=str(self.root / 'azure-key'), endpoint_file=str(self.root / 'endpoint'), timeout_secs=2)
        self.config['forward']['timeout_secs'] = 2
        # Dependency-free adapter stand-in; credentialed/local validation can
        # explicitly select the actual installed Agent Utils module instead.
        module = self.root / 'agent-utils/extensions/lib/realtime-stt-batch.js'
        module.parent.mkdir(parents=True)
        (self.root / 'agent-utils/package.json').write_text('{"type":"module"}')
        module.write_text('''export async function transcribeMaiAudioDirect(o) {
          if(o.model !== 'mai-transcribe-2' || o.wav.toString('ascii',0,4) !== 'RIFF') throw Error('bad adapter input');
          const form=new FormData();
          form.set('file', new Blob([o.wav], {type:'audio/wav'}), 'audio.wav');
          form.set('model', o.deployment);
          if (o.language) form.set('language', o.language);
          const r=await o.fetchImpl(o.endpoint+'/mai/v1/audio/transcriptions', {method:'POST',
            headers:{'api-key':o.apiKey}, body:form});
          if(!r.ok) throw Error('HTTP failure');
          return (await r.json()).text;
        }''')
        self.config['transcription']['agent_utils_dir'] = str(self.root / 'agent-utils')
        self.archive = receiver.Archive(self.config)
        self.peer = None

    def tearDown(self):
        if self.peer: self.peer.close()
        self.tmp.cleanup()

    def use_peer(self):
        self.peer = Peer()
        (self.root / 'endpoint').write_text(self.peer.url)
        self.config['forward']['url'] = self.peer.url + '/hooks/global/pebble'
        return self.peer

    def admit(self, **kwargs):
        return receiver.admit(self.archive, *multipart(**kwargs), sound=lambda _: 'played')

    @contextlib.contextmanager
    def http_server(self, worker=None, sound=lambda _: 'played'):
        worker = worker or receiver.Worker(self.archive)
        server = receiver.Server(self.archive, worker, ('127.0.0.1', 0), sound=sound)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            yield server, f'http://127.0.0.1:{server.server_port}'
        finally:
            server.shutdown()
            server.server_close()
            worker.stop.set()
            worker.wake.set()
            if worker.thread.is_alive(): worker.thread.join(10)
            thread.join(5)
            self.assertFalse(thread.is_alive())
            with self.assertRaises(OSError): socket.create_connection(server.server_address, timeout=.2)

    def post(self, url, body, content_type, token=None):
        request = urllib.request.Request(url, data=body, headers={'Authorization': 'Bearer ' + (token or self.token), 'Content-Type': content_type})
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response)

    def test_valid_config_ignores_ownership_permissions_and_follows_symlinks(self):
        config = self.root / 'readable-config.json'
        config.write_text(json.dumps(self.config))
        config.chmod(0o666)
        link = self.root / 'config-link.json'
        link.symlink_to(config)
        with patch.object(receiver.os, 'fstat', side_effect=AssertionError('config must not inspect ownership')):
            self.assertEqual(receiver.load_config(link), self.config)
        config.write_text('{"version":1,"unexpected":true}')
        with self.assertRaisesRegex(receiver.Failure, 'invalid_config_fields'):
            receiver.load_config(link)

    def test_raw_binary_audio_and_each_retry_preserved_without_filename_use(self):
        audio = b'\x00\xff\x80\r\n' * 100
        body, content_type = multipart(audio=audio, mime='audio/mp4')
        first = receiver.admit(self.archive, body, content_type, sound=lambda _: 'played')
        second = self.admit(audio=audio, mime='audio/mp4', boundary='different-boundary', text='revised device words')
        self.assertEqual(first['id'], second['id'])
        self.assertTrue(second['duplicate'])
        capture = self.archive.get(first['id'])
        self.assertEqual(capture['audio'], audio)
        self.assertEqual(capture['device_transcript'], 'device rough transcript')
        self.archive.export(first['id'], self.root / 'export')
        self.assertEqual((self.root / 'export/audio.m4a').read_bytes(), audio)
        self.assertEqual((self.root / 'export/request-1.body').read_bytes(), body)
        self.assertFalse((self.root / 'never-use-this.wav').exists())
        self.assertEqual(self.archive.status()['requests'], 2)

    def test_unauthenticated_requests_do_not_enter_archive_and_rotation_follows_symlink(self):
        link = self.root / 'token-link'
        link.symlink_to('webhook-token')
        self.config['token_file'] = str(link)
        with self.http_server() as (_, url):
            with self.assertRaises(urllib.error.HTTPError) as err:
                self.post(url, *multipart(), token='wrong')
            self.assertEqual(err.exception.code, 401)
            err.exception.close()
            self.assertEqual(self.archive.status()['requests'], 0)
            self.assertEqual(self.post(url, *multipart())[0], 202)
            (self.root / 'webhook-token').write_text('rotated-token')
            with self.assertRaises(urllib.error.HTTPError) as denied: self.post(url, *multipart())
            denied.exception.close()
            self.assertEqual(self.post(url, *multipart(), token='rotated-token')[0], 202)
        (self.root / 'webhook-token').chmod(0o644)
        with self.assertRaises(receiver.Failure): receiver.read_secret(link)

    def test_immediate_chime_once_even_when_worker_is_blocked(self):
        calls = []
        first = self.admit()
        self.archive.update(first['id'], phase='blocked')
        for boundary in ['one', 'two']:
            receiver.admit(self.archive, *multipart(boundary=boundary, recorded='1770000000001'), sound=lambda c: calls.append(c['pitch']) or 'played')
        self.assertEqual(calls, [523.25])
        self.assertNotEqual(self.config['chime']['pitch'], 880)

    def test_malformed_request_archived_without_chime_or_audio_url_fetch(self):
        calls = []
        receipt = receiver.admit(self.archive, b'{"audio_url":"http://169.254.169.254/"}', 'application/json', sound=lambda _: calls.append(True))
        self.assertEqual(receipt['phase'], 'rejected')
        self.assertEqual(calls, [])
        self.assertEqual(self.archive.status()['requests'], 1)
        self.assertIsNone(self.archive.head())

    def test_limits_and_incomplete_http_body(self):
        self.config['max_pending'] = 1
        self.admit()
        with self.assertRaisesRegex(receiver.Failure, 'queue_full'): self.admit(recorded='2')
        self.assertTrue(self.admit()['duplicate'])
        self.config['max_archive_bytes'] = 1
        with self.assertRaisesRegex(receiver.Failure, 'archive_quota_reached'): self.admit()
        self.config['max_body_bytes'] = 1
        with self.http_server() as (_, url):
            with self.assertRaises(urllib.error.HTTPError) as err: self.post(url, *multipart())
            self.assertEqual(err.exception.code, 413)
            err.exception.close()

    def test_recovery_retains_audio_and_frozen_forward_payload(self):
        receipt = self.archive.accept(*multipart())
        self.archive.recover()
        capture = self.archive.get(receipt['id'])
        self.assertEqual((capture['phase'], capture['chime']), ('queued', 'interrupted'))
        self.archive.update(receipt['id'], phase='forwarding', forward_body=b'{"unchanged":true}', mai_text='saved words')
        self.archive.recover()
        capture = self.archive.get(receipt['id'])
        self.assertEqual(capture['phase'], 'ready')
        self.assertEqual(capture['forward_body'], b'{"unchanged":true}')

    def test_fifo_failures_no_silent_device_fallback_and_explicit_retry(self):
        first = self.admit()
        self.admit(recorded='2')
        def fail(_): raise receiver.Failure('mai_http_403', response={'error': 'auth failed'})
        worker = receiver.Worker(self.archive, transcriber=fail, forwarder=lambda _: self.fail('must not forward rough transcript'))
        worker.step()
        self.assertEqual(self.archive.head()['phase'], 'blocked')
        self.assertFalse(worker.step())
        self.archive.resolve(first['id'], 'retry')
        self.assertEqual(self.archive.head()['phase'], 'queued')
        worker.step()
        self.archive.resolve(first['id'], 'skip')
        self.assertNotEqual(self.archive.head()['id'], first['id'])

    def test_real_http_ffmpeg_node_pipeline_acknowledges_before_transcription(self):
        peer = self.use_peer()
        peer.mai_gate = threading.Event()
        sound = []
        worker = receiver.Worker(self.archive)
        worker.thread.start()
        with self.http_server(worker, sound=lambda _: sound.append('upload') or 'played') as (_, url):
            body, ct = multipart()
            status, receipt = self.post(url + '/hooks/global/pebble-audio', body, ct)
            self.assertEqual(status, 202)
            self.assertTrue(peer.mai_seen.wait(5))
            self.assertEqual(peer.first_bodies, [])
            self.assertEqual(sound, ['upload'])
            self.assertIsNone(self.archive.get(receipt['id'])['mai_text'])
            peer.mai_gate.set()
            deadline = time.monotonic() + 10
            while self.archive.get(receipt['id'])['phase'] != 'forwarded' and time.monotonic() < deadline:
                time.sleep(.02)
            capture = self.archive.get(receipt['id'])
            self.assertEqual(capture['phase'], 'forwarded', capture['error'])
            self.assertEqual(capture['mai_text'], 'higher quality words')
            self.assertEqual(json.loads(capture['mai_response'])['language'], 'en')
            payload = json.loads(peer.first_bodies[0])
            self.assertEqual(payload['transcription'], 'higher quality words')
            self.assertEqual(payload['audio_capture_id'], receipt['id'])
            self.assertNotIn('audio', payload)
            self.assertNotIn('device rough', peer.first_bodies[0].decode())
            mai_headers = next(headers for path, headers, _ in peer.calls if path.startswith('/mai/'))
            first_headers = next(headers for path, headers, _ in peer.calls if path.startswith('/hooks/'))
            self.assertEqual(mai_headers.get('api-key'), self.azure_key)
            self.assertNotIn('Authorization', mai_headers)
            self.assertEqual(first_headers['Authorization'], 'Bearer ' + self.token)
            self.post(url, *multipart(boundary='retry'))
            self.assertEqual(sound, ['upload'])
            self.assertEqual(peer.mai_count, 1)
            self.archive.export(receipt['id'], self.root / 'export')
            for file in (self.root / 'export').iterdir():
                self.assertNotIn(self.azure_key.encode(), file.read_bytes())
                self.assertNotIn(self.token.encode(), file.read_bytes())

    def test_actual_agent_utils_adapter_against_local_http_peer(self):
        source = os.environ.get('PEBBLE_AGENT_UTILS')
        if not source: self.skipTest('set PEBBLE_AGENT_UTILS for the installed adapter contract')
        self.use_peer()
        self.config['transcription']['agent_utils_dir'] = source
        capture = self.archive.get(self.admit()['id'])
        result = receiver.transcribe(capture)
        self.assertEqual(result['text'], 'higher quality words')
        sent = mai_form(self.peer.calls[0][1], self.peer.calls[0][2])
        self.assertEqual(sent['model'], 'example-mai-transcribe-2')
        self.assertEqual(sent['file'][:4], b'RIFF')

    def test_forward_retry_reuses_saved_text_and_first_hook_deduplicates(self):
        peer = self.use_peer()
        receipt = self.admit()
        calls = 0
        def forward_then_lose_response(capture):
            nonlocal calls
            calls += 1
            result = receiver.forward(capture)
            if calls == 1: raise receiver.Failure('lost_http_receipt', True)
            return result
        worker = receiver.Worker(self.archive, forwarder=forward_then_lose_response)
        worker.step()
        payload = self.archive.get(receipt['id'])['forward_body']
        worker.step()
        self.assertEqual(self.archive.get(receipt['id'])['phase'], 'ready')
        time.sleep(1.05)
        worker.step()
        self.assertEqual(self.archive.get(receipt['id'])['phase'], 'forwarded')
        self.assertEqual(peer.mai_count, 1)
        self.assertEqual(peer.first_bodies, [payload, payload])
        self.assertEqual(len(peer.first_ids), 1)

    def test_provider_timeout_and_secret_redaction_are_persistent_not_forwarded(self):
        peer = self.use_peer()
        peer.mai_status = 403
        peer.mai_text = 'provider accidentally echoed ' + self.azure_key
        receipt = self.admit()
        receiver.Worker(self.archive).step()
        capture = self.archive.get(receipt['id'])
        self.assertEqual(capture['phase'], 'blocked')
        self.assertEqual(capture['error'], 'mai_http_403')
        self.assertNotIn(self.azure_key, capture['mai_response'])
        self.assertIn('[redacted]', capture['mai_response'])
        self.assertEqual(peer.first_bodies, [])

    def test_process_timeout_and_output_bound(self):
        start = time.monotonic()
        with self.assertRaisesRegex(receiver.Failure, 'process_timeout'):
            receiver.run_process([sys.executable, '-c', 'import time; time.sleep(30)'], .15)
        self.assertLess(time.monotonic() - start, 3)
        with self.assertRaisesRegex(receiver.Failure, 'process_output_limit'):
            receiver.run_process([sys.executable, '-c', 'print("a"*100000)'], 2, 128)

    def test_real_m4a_decode_then_mai_adapter(self):
        self.use_peer()
        source, output = self.root / 'fixture.wav', self.root / 'fixture.m4a'
        source.write_bytes(wav_bytes())
        receiver.run_process([self.config['ffmpeg'], '-nostdin', '-v', 'error', '-i', str(source), '-c:a', 'aac', str(output)], 10)
        receipt = self.admit(audio=output.read_bytes(), mime='audio/mp4')
        result = receiver.transcribe(self.archive.get(receipt['id']))
        self.assertEqual(result['text'], 'higher quality words')
        self.assertEqual(self.archive.get(receipt['id'])['audio'], output.read_bytes())

    def test_mai_timeout_and_redirect_do_not_leak_credentials(self):
        peer = self.use_peer()
        self.config['transcription']['timeout_secs'] = 1
        peer.mai_gate = threading.Event()
        capture = self.archive.get(self.admit()['id'])
        with self.assertRaises(receiver.Failure) as err:
            receiver.transcribe(capture)
        self.assertEqual(err.exception.code, 'request_timeout')
        self.assertTrue(err.exception.retryable)
        peer.mai_gate.set()
        peer.mai_gate = None
        peer.redirect = True
        before = len(peer.calls)
        with self.assertRaises(receiver.Failure): receiver.transcribe(capture)
        self.assertEqual(len(peer.calls), before + 1)
        self.assertNotIn('/redirected', [item[0] for item in peer.calls])

    def test_incomplete_request_deadline_and_fifo_credentials_rejected(self):
        self.config['listen']['request_timeout_secs'] = 1
        with self.http_server() as (server, _):
            with socket.create_connection(server.server_address, timeout=3) as client:
                client.settimeout(3)
                client.sendall(('POST / HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ' + self.token +
                                '\r\nContent-Type: multipart/form-data; boundary=test\r\nContent-Length: 1000\r\n\r\nabc').encode())
                started = time.monotonic()
                while client.recv(4096): pass
                self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(self.archive.status()['requests'], 0)
        fifo = self.root / 'not-a-token'
        os.mkfifo(fifo, 0o600)
        with self.assertRaises(receiver.Failure): receiver.read_secret(fifo)

    def test_cli_serve_lifecycle_and_single_worker_ownership(self):
        config_file = self.root / 'service-config.json'
        config_file.write_text(json.dumps(self.config))
        command = [sys.executable, str(ROOT / 'receiver.py'), '--config', str(config_file), 'serve']
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout, selectors.EVENT_READ)
                self.assertTrue(selector.select(5), 'service did not report readiness')
            started = json.loads(proc.stdout.readline())
            self.assertEqual(started['event'], 'pebble_audio_listening')
            request = urllib.request.Request(f"http://127.0.0.1:{started['port']}/health", headers={'Authorization': 'Bearer ' + self.token})
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertTrue(json.load(response)['worker_alive'])
            duplicate = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertNotEqual(duplicate.returncode, 0)
            self.assertIsNone(proc.poll())
        finally:
            proc.terminate()
            proc.communicate(timeout=5)
        self.assertEqual(proc.returncode, 0)
        with self.assertRaises(OSError): socket.create_connection(('127.0.0.1', started['port']), timeout=.2)

    def test_retry_uses_current_runtime_but_preserves_recorded_route(self):
        original_url = self.config['forward']['url']
        receipt = self.admit()
        self.config['node'] = '/new-runtime/node'
        self.config['ffmpeg'] = '/new-runtime/ffmpeg'
        self.config['transcription']['agent_utils_dir'] = '/new-runtime/agent-utils'
        self.config['forward']['url'] = 'http://127.0.0.1:9999/different'
        seen = []
        def transcriber(capture):
            seen.append(json.loads(capture['settings']))
            return {'text': 'saved transcript', 'response': {'text': 'saved transcript'}}
        receiver.Worker(self.archive, transcriber=transcriber).step()
        self.assertEqual(seen[0]['node'], '/new-runtime/node')
        self.assertEqual(seen[0]['forward']['url'], original_url)
        self.assertEqual(seen[0]['transcription']['agent_utils_dir'], '/new-runtime/agent-utils')
        self.assertEqual(json.loads(self.archive.get(receipt['id'])['settings'])['node'], 'node')

    def test_audio_duration_limit_prevents_truncation_and_forwarding(self):
        self.config['transcription']['max_audio_secs'] = 1
        receipt = self.admit(audio=wav_bytes(1.5))
        with self.assertRaisesRegex(receiver.Failure, 'audio_too_long'):
            receiver.transcribe(self.archive.get(receipt['id']))


class FunnelTest(unittest.TestCase):
    def state(self):
        return {'TCP': {'443': {'HTTPS': True}}, 'AllowFunnel': {'pebble.example.invalid:443': True},
                'Web': {'pebble.example.invalid:443': {'Handlers': {'/': {'Proxy': 'http://127.0.0.1:11300'}}}}}

    def test_only_audio_path_added_and_repeat_is_noop(self):
        state = self.state()
        original = copy.deepcopy(state)
        calls = []
        def run(args, **_):
            calls.append(args)
            state['Web']['pebble.example.invalid:443']['Handlers'][funnel.PATH] = {'Proxy': args[-1]}
        result = funnel.install('tailscale', 'pebble.example.invalid', 'http://127.0.0.1:11301', run=run, inspect=lambda _: copy.deepcopy(state))
        self.assertTrue(result['changed'])
        self.assertEqual(funnel.without_owned_path(state, 'pebble.example.invalid:443'), original)
        self.assertFalse(funnel.install('tailscale', 'pebble.example.invalid', 'http://127.0.0.1:11301', run=run, inspect=lambda _: state)['changed'])
        self.assertEqual(len(calls), 1)
        self.assertNotIn('reset', calls[0])

    def test_foreign_path_or_missing_funnel_refuses_before_mutation(self):
        state = self.state()
        state['Web']['pebble.example.invalid:443']['Handlers'][funnel.PATH] = {'Proxy': 'http://other:1234'}
        def fail(*_, **__): self.fail('must not mutate')
        with self.assertRaisesRegex(RuntimeError, 'already_owned'):
            funnel.install('tailscale', 'pebble.example.invalid', 'http://127.0.0.1:11301', run=fail, inspect=lambda _: state)
        with self.assertRaisesRegex(RuntimeError, 'existing_https_funnel'):
            funnel.install('tailscale', 'pebble.example.invalid', 'http://127.0.0.1:11301', run=fail, inspect=lambda _: {})


if __name__ == '__main__':
    unittest.main()
