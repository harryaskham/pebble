"""Shared Index payload parsing and command ledger in the existing archive.

Legacy audio tables remain untouched. The command worker never drains them or
imports historical text jobs as new work. Matching historical identities are
retained with a link, not replayed.
"""
from __future__ import annotations

import contextlib
import action_registry
import settings as settings_schema
from email.parser import BytesParser
import email.policy
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import time
import uuid

MIMES = {'audio/mp4': 'mov', 'audio/m4a': 'mov', 'audio/x-m4a': 'mov', 'audio/wav': 'wav', 'audio/x-wav': 'wav'}
ACTIONS = {'route', 'pebble', 'bd-file', 'bd-expand', 'agent-msg', 'broadcast'}
TERMINAL_SQL = "('stored','created','completed','skipped','retained','dry_run','clarification_required','answered')"


class IntakeError(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def parse_input(body, content_type):
    if not isinstance(content_type, str) or len(content_type) > 512 or any(c in content_type for c in '\r\n'):
        raise IntakeError('invalid_content_type')
    audio, mime = None, None
    if content_type.lower().startswith('multipart/form-data'):
        message = BytesParser(policy=email.policy.default).parsebytes(
            b'Content-Type: ' + content_type.encode('ascii') + b'\r\nMIME-Version: 1.0\r\n\r\n' + body)
        if not message.is_multipart() or message.defects:
            raise IntakeError('malformed_multipart')
        parts, fields = list(message.iter_parts()), {}
        if len(parts) > 16:
            raise IntakeError('too_many_parts')
        seen = set()
        for part in parts:
            name = part.get_param('name', header='content-disposition')
            if not name or name in seen or part.is_multipart() or part.defects:
                raise IntakeError('invalid_multipart_part')
            seen.add(name)
            data = part.get_payload(decode=True)
            if name == 'audio':
                audio, mime = data, part.get_content_type()
            else:
                if part.get_filename() is not None or len(data) > 32768:
                    raise IntakeError('invalid_text_field')
                fields[name] = data.decode('utf-8', errors='strict')
    elif content_type.lower().split(';')[0].strip() == 'application/json':
        fields = json.loads(body)
        if not isinstance(fields, dict):
            raise IntakeError('invalid_json_object')
        if 'audio' in fields:
            raise IntakeError('binary_audio_multipart_required')
    else:
        raise IntakeError('unsupported_content_type')
    if any(key in fields for key in ('audio_url', 'audio_base64')):
        raise IntakeError('remote_audio_not_supported')
    test = fields.get('test', False)
    if type(test) is str and test in ('true', 'false'):
        test = test == 'true'
    if type(test) is not bool:
        raise IntakeError('invalid_test_flag')
    if test:
        # Authenticated connection probes do not archive, speak, transcribe,
        # invoke Jev, or create even a queued mutation.
        return {'test': True}, None
    if audio is not None and (not audio or mime not in MIMES):
        raise IntakeError('supported_audio_required')
    texts = [fields[key] for key in ('transcription', 'transcript', 'text') if key in fields]
    if len(texts) > 1 and any(text != texts[0] for text in texts):
        raise IntakeError('conflicting_transcriptions')
    text = texts[0] if texts else ''
    if not isinstance(text, str) or '\0' in text or len(text.encode()) > 16384 or (audio is None and not text.strip()):
        raise IntakeError('invalid_transcription')
    recorded = fields.get('recordedAt')
    if recorded is not None and (type(recorded) not in (str, int) or not str(recorded).isdigit() or not 1 <= len(str(recorded)) <= 17):
        raise IntakeError('invalid_recorded_at')
    client = fields.get('client', 'ring' if audio is not None else 'unknown')
    if not isinstance(client, str) or not client or len(client) > 128 or '\0' in client:
        raise IntakeError('invalid_client')
    return {'test': False, 'recordedAt': str(recorded) if recorded is not None else None,
            'client': client, 'transcription': text, 'audio_mime': mime}, audio


def extraction_capability(config):
    settings = config['commands'].get('extractor', {})
    return {'enabled': settings.get('enabled', False), 'model': settings.get('model')}


def failed_before_effect(state, attempts):
    """Receipt-only classification; failed inputs never reserve scheduling lanes.

    More than one historical attempt is deliberately not inferred from the last
    not_started result: earlier attempts may still have unconfirmed effects.
    """
    return (state is None or (state == 'planned' and attempts == 0)
            or (state == 'not_started' and attempts is not None and attempts <= 1))


def lanes_overlap(first, second):
    """Broadcast audiences overlap individual conversations, never bead lanes."""
    if first[0] == 'unknown' or second[0] == 'unknown' or first == second:
        return True
    if first[0] == 'broadcast' and second[0] in ('message', 'broadcast'):
        project = second[1].split(':')[1] if second[0] == 'message' else second[1]
        return first[1] is None or project is None or first[1] == project
    if second[0] == 'broadcast' and first[0] == 'message':
        return lanes_overlap(second, first)
    return False


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def legacy_audio_identity(metadata, audio):
    return hashlib.sha256(json.dumps([metadata['recordedAt'], metadata['client'], hashlib.sha256(audio).hexdigest()]).encode()).hexdigest()


class CommandArchive:
    def __init__(self, legacy):
        self.legacy, self.root, self.config, self.db = legacy, legacy.root, legacy.config, legacy.db
        settings_schema.normalize_runtime(self.config)
        with self.db() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS command_inputs (
                seq INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL, identity TEXT UNIQUE NOT NULL,
                fingerprint TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
                endpoint_action TEXT NOT NULL, metadata TEXT NOT NULL, audio BLOB,
                settings TEXT NOT NULL, phase TEXT NOT NULL, text TEXT,
                transcription_response TEXT, context TEXT, plan TEXT, error TEXT, error_stage TEXT,
                transcribe_attempts INTEGER NOT NULL DEFAULT 0, route_attempts INTEGER NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0, legacy_reference TEXT);
              CREATE TABLE IF NOT EXISTS command_requests (
                seq INTEGER PRIMARY KEY, received REAL NOT NULL, input_id TEXT,
                endpoint_action TEXT NOT NULL, content_type TEXT NOT NULL, body BLOB NOT NULL, error TEXT);
              CREATE TABLE IF NOT EXISTS command_actions (
                input_id TEXT PRIMARY KEY, recipe TEXT NOT NULL, recipe_hash TEXT NOT NULL,
                state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                receipt TEXT, error TEXT, updated REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS command_speech (
                input_id TEXT NOT NULL, kind TEXT NOT NULL, state TEXT NOT NULL,
                updated REAL NOT NULL, error TEXT, PRIMARY KEY(input_id,kind));
              CREATE TABLE IF NOT EXISTS command_audit (
                seq INTEGER PRIMARY KEY, at REAL NOT NULL, input_id TEXT, action TEXT NOT NULL);
              CREATE INDEX IF NOT EXISTS command_phase ON command_inputs(phase,seq);
            ''')
            # Additive workflow migration; existing rows remain uncancelled.
            db.execute('BEGIN IMMEDIATE')
            if 'cancelled_at' not in {r[1] for r in db.execute('PRAGMA table_info(command_inputs)')}:
                db.execute('ALTER TABLE command_inputs ADD COLUMN cancelled_at REAL')
            if 'conversation' not in {r[1] for r in db.execute('PRAGMA table_info(command_inputs)')}:
                db.execute('ALTER TABLE command_inputs ADD COLUMN conversation TEXT')

    def legacy_match(self, metadata, audio, body):
        if audio is not None and metadata['recordedAt']:
            with self.db() as db:
                old = db.execute('SELECT id,phase FROM captures WHERE identity=?', (legacy_audio_identity(metadata, audio),)).fetchone()
            if old:
                return {'archive': str(self.legacy.filename), 'table': 'captures', 'id': old['id'], 'observed_phase': old['phase']}
        filename = self.config['commands']['legacy_text_archive']
        if audio is not None or not filename:
            return None
        source = Path(filename).expanduser()
        if not source.exists():
            return None
        payload = {key: metadata[key] for key in ('transcription', 'recordedAt', 'client')}
        identity = hashlib.sha256(json.dumps(payload, sort_keys=True).encode() if metadata['recordedAt'] else body).hexdigest()
        # Read-only URI: never creates a legacy database or drains its jobs.
        with contextlib.closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            row = db.execute('SELECT id,stage FROM jobs WHERE dedupe=?', (identity,)).fetchone()
        return {'archive': str(source), 'table': 'jobs', 'id': row[0], 'observed_phase': row[1]} if row else None

    def accept(self, body, content_type, action='route', idempotency_key=None, *, dry_run=False, actions=None,
               speech_owner='server', connection=None, config_snapshot=None, profile='default'):
        started = time.monotonic()
        if type(dry_run) is not bool:
            raise IntakeError('invalid_dry_run_mode')
        if speech_owner not in ('server', 'client'):
            raise IntakeError('invalid_speech_owner')
        if not 0 < len(body) <= self.config['max_body_bytes']:
            raise IntakeError('body_size_invalid')
        actions = action_registry.catalog(self.config['commands']) if actions is None else actions
        snapshot = self.config if config_snapshot is None else config_snapshot
        settings_schema.normalize_runtime(snapshot)
        if profile != 'default' and profile not in snapshot['commands'].get('jev_profiles', {}):
            raise IntakeError('unknown_jev_profile')
        selected = settings_schema.apply_profile({**snapshot['commands'], 'actions': actions}, profile)
        builtins = ACTIONS if selected['caco']['enabled'] else {'route'}
        if action not in builtins and action not in action_registry.describe(selected['actions']):
            raise IntakeError('invalid_endpoint_action')
        frozen_config = {**snapshot, 'commands': {**snapshot['commands'], 'actions': actions, 'actions_file': ''}}
        metadata, audio, error = None, None, None
        try:
            metadata, audio = parse_input(body, content_type)
            metadata['dry_run'] = dry_run
            metadata['speech_owner'] = speech_owner
            metadata['route_profile'] = profile
            if metadata['test']:
                return {'id': None, 'phase': 'probe', 'test': True, 'archived': False, 'error': None, 'duplicate': False,
                        'dry_run_supported': True, 'status_feed_supported': True,
                        'supported_actions': sorted((builtins - {'route'}) | set(action_registry.describe(selected['actions']))),
                        'route_profile': profile, 'action_manifest_supported': True,
                        'custom_actions': action_registry.describe(selected['actions']),
                        'payload_extractor': extraction_capability(self.config)}
            if idempotency_key is not None and (not isinstance(idempotency_key, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', idempotency_key)):
                raise IntakeError('invalid_idempotency_key')
        except (IntakeError, ValueError, UnicodeError, TypeError, RecursionError) as exc:
            metadata, audio = None, None
            error = exc.code if isinstance(exc, IntakeError) else 'malformed_payload'
        content = hashlib.sha256(audio).hexdigest() if audio is not None else metadata['transcription'] if metadata else None
        fingerprint = digest([metadata['recordedAt'], metadata['client'], 'audio' if audio is not None else 'text', content]) if metadata else None
        identity = digest(['key', metadata['client'], idempotency_key]) if metadata and idempotency_key else fingerprint
        legacy, legacy_error = None, None
        if metadata:
            try:
                legacy = self.legacy_match(metadata, audio, body)
            except (OSError, sqlite3.Error):
                # A lookup outage must not lose the upload or allow a possible
                # historical duplicate through. Archive it, with no effect.
                legacy_error = 'legacy_identity_lookup_unavailable'
        now = time.time()
        with (self.db() if connection is None else contextlib.nullcontext(connection)) as db:
            if connection is None:
                db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM command_inputs WHERE identity=?', (identity,)).fetchone() if identity else None
            pending = db.execute(f'SELECT count(*) FROM command_inputs WHERE phase NOT IN {TERMINAL_SQL}').fetchone()[0]
            used = sum(p.stat().st_size for p in self.root.glob('archive.sqlite3*'))
            reserve = 2 * len(body) + (pending + (0 if old else 1)) * 3 * 1024 * 1024
            if used + reserve > self.config['max_archive_bytes']:
                raise IntakeError('archive_quota_reached')
            if shutil.disk_usage(self.root).free - reserve < self.config['min_free_bytes']:
                raise IntakeError('archive_disk_low')
            if metadata and not old and not legacy and pending >= self.config['max_pending']:
                raise IntakeError('queue_full')
            input_id = old['id'] if old else uuid.uuid4().hex if metadata else None
            if old and (old['fingerprint'] != fingerprint or old['endpoint_action'] != action or json.loads(old['metadata']).get('dry_run', False) != dry_run
                        or json.loads(old['metadata']).get('route_profile', 'default') != profile
                        or json.loads(old['metadata']).get('speech_owner', 'server') != speech_owner):
                error = 'input_identity_conflict'
            phase = old['phase'] if old else 'rejected'
            if metadata and not old:
                phase = 'retained' if legacy else 'blocked' if legacy_error else 'queued' if audio is not None else 'ready'
                if dry_run and phase in ('queued', 'ready', 'blocked'):
                    phase = 'dry_' + phase
                db.execute('''INSERT INTO command_inputs(id,identity,fingerprint,created,updated,endpoint_action,
                              metadata,audio,settings,phase,text,legacy_reference,error,error_stage) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                           (input_id, identity, fingerprint, now, now, action, json.dumps(metadata), audio,
                            json.dumps(frozen_config), phase, None if audio is not None else metadata['transcription'],
                            json.dumps(legacy) if legacy else None, legacy_error, 'legacy_check' if legacy_error else None))
            request = db.execute('INSERT INTO command_requests(received,input_id,endpoint_action,content_type,body,error) VALUES(?,?,?,?,?,?)',
                                 (now, input_id, action, content_type, body, error)).lastrowid
        return {'id': input_id, 'request_id': request, 'phase': phase, 'duplicate': bool(old), 'error': error,
                'dry_run': json.loads(old['metadata']).get('dry_run', False) if old else dry_run,
                'archived': True, 'legacy_retained': bool(legacy or (old and old['legacy_reference'])), 'archive_ms': round((time.monotonic() - started) * 1000, 2)}

    def get(self, input_id):
        with self.db() as db:
            row = db.execute('SELECT * FROM command_inputs WHERE id=?', (input_id,)).fetchone()
        if not row:
            raise IntakeError('input_not_found')
        return dict(row)

    def head(self):
        with self.db() as db:
            row = db.execute(f"SELECT * FROM command_inputs WHERE phase NOT IN {TERMINAL_SQL} AND phase NOT IN ('blocked','dry_blocked') ORDER BY seq LIMIT 1").fetchone()
        return dict(row) if row else None

    def next_work(self, active=(), *, server_speech_busy=False):
        # Called only by the owner/coordinator (or synchronous tests). Active
        # IDs are skipped as candidates, NEVER removed from ordering evidence.
        # Read small scheduling fields, not every queued audio/context blob.
        with self.db() as db:
            rows = db.execute(f'''SELECT i.id,i.phase,i.retry_at,i.endpoint_action,
                json_extract(i.settings,'$.commands.pebble_target') AS assistant,
                json_extract(i.metadata,'$.dry_run') AS dry_run,
                COALESCE(json_extract(i.metadata,'$.speech_owner'),'server') AS speech_owner,
                COALESCE(json_extract(a.recipe,'$.action'),json_extract(i.plan,'$.action')) AS action,
                COALESCE(json_extract(a.recipe,'$.target'),json_extract(i.plan,'$.target')) AS target,
                COALESCE(json_extract(a.recipe,'$.project'),json_extract(i.plan,'$.project')) AS project,
                COALESCE(json_extract(a.recipe,'$.custom_spec.lane'),
                    json_extract(i.settings,'$.commands.actions."' || substr(COALESCE(json_extract(i.plan,'$.action'),i.endpoint_action),8) || '".lane')) AS custom_lane,
                EXISTS(SELECT 1 FROM command_speech s WHERE s.input_id=i.id AND s.kind='waiting') AS notified
                FROM command_inputs i LEFT JOIN command_actions a ON a.input_id=i.id
                WHERE i.cancelled_at IS NULL AND i.phase NOT IN {TERMINAL_SQL}
                  AND i.phase NOT IN ('blocked','dry_blocked') ORDER BY i.seq''').fetchall()
        blocked = set()
        for row in rows:
            phase = row['phase'].removeprefix('dry_') if row['dry_run'] else row['phase']
            if action_registry.custom(row['action']) or action_registry.custom(row['endpoint_action']):
                action = row['action'] or row['endpoint_action']
                lane = ('custom', row['custom_lane'] or action)
            elif row['action'] == 'broadcast':
                lane = ('broadcast', row['project'])  # None is the explicit global scope.
            elif row['action'] in ('pebble', 'agent-msg'):
                lane = ('message', row['target'])
            elif row['action'] in ('bd-file', 'bd-expand'):
                lane = ('beads', row['project'])
            elif row['endpoint_action'] == 'pebble':
                lane = ('message', row['assistant'])
            else:
                # An unfinished automatic route can become ANY lane, including
                # beads/custom/global broadcast. Never guess its future target.
                lane = ('unknown', None)
            waiting = (phase == 'planned' and not row['dry_run']
                       and any(lanes_overlap(lane, earlier) for earlier in blocked))
            # Only unfinished live work contributes a lane. Retained failures
            # (including uncertain outcomes) are excluded above, not retried or
            # treated as ordering evidence. Preparation can pass active work.
            if not row['dry_run']:
                blocked.add(lane)
            if (row['id'] in active or row['retry_at'] > time.time()
                    or (server_speech_busy and row['speech_owner'] != 'client')):
                continue
            if waiting:
                if not row['notified']:
                    return {**self.get(row['id']), 'work_hint': 'wait_for_destination'}
                continue
            return self.get(row['id'])
        return None

    def require_active(self, db, input_id):
        """Must run inside the same write transaction as the subsequent change."""
        # Never reload audio/transcription-response blobs merely to check a fence.
        row = db.execute('''SELECT id,metadata,cancelled_at,phase,text,plan,context,settings,error_stage,conversation
                            FROM command_inputs WHERE id=?''', (input_id,)).fetchone()
        if not row:
            raise IntakeError('input_not_found')
        if row['cancelled_at'] is not None:
            raise IntakeError('input_cancelled')
        return row

    def cancel(self, input_id, *, connection):
        """CAS against start_action's BEGIN IMMEDIATE, never against phase alone.

        A prior attempt, even one subsequently resolved to planned/not_started,
        is beyond this API's boundary. Cancellation is not dispatch rollback.
        The caller owns the transaction so turn lookup and this fence are atomic.
        """
        db = connection
        source = db.execute('SELECT * FROM command_inputs WHERE id=?', (input_id,)).fetchone()
        if not source:
            raise IntakeError('input_not_found')
        if source['cancelled_at'] is not None:
            return
        if source['phase'] == 'clarification_required' and source['conversation'] and db.execute('''SELECT 1 FROM triage_links l JOIN triage_turns t
                ON t.id=l.source_turn WHERE t.input_id=?''', (input_id,)).fetchone():
            raise IntakeError('draft_consumed_cancel_action_owner')
        question = source['conversation'] and json.loads(source['conversation']).get('state') == 'disambiguation'
        if question and db.execute('''SELECT 1 FROM triage_selections s JOIN triage_turns t
                ON t.id=s.question_turn WHERE t.input_id=?''', (input_id,)).fetchone():
            raise IntakeError('question_consumed_cancel_action_owner')
        action = db.execute('SELECT state,attempts FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
        if action and (action['attempts'] != 0 or action['state'] not in ('planned', 'dry_run')):
            raise IntakeError('cancel_dispatch_boundary_crossed')
        if action and action['state'] == 'dry_run':
            raise IntakeError('turn_not_cancellable')  # Finished preview, not a dispatch attempt.
        phase = source['phase'].removeprefix('dry_')
        if (phase not in ('queued', 'ready', 'acknowledged', 'routing', 'transcribing', 'payload_pending', 'extracting', 'planned', 'blocked', 'clarification_required')
                and not (phase == 'answered' and question)) or source['error_stage'] == 'dispatch':
            raise IntakeError('turn_not_cancellable')
        now = time.time()
        db.execute("UPDATE command_actions SET state='cancelled',updated=? WHERE input_id=?", (now, input_id))
        db.execute("UPDATE command_inputs SET cancelled_at=?,phase='skipped',error='cancelled_before_dispatch',error_stage='cancel',retry_at=0,updated=? WHERE id=?",
                   (now, now, input_id))
        db.execute('INSERT INTO command_audit(at,input_id,action) VALUES(?,?,?)', (now, input_id, 'cancel_before_dispatch'))

    def update(self, input_id, **values):
        allowed = {'phase', 'text', 'transcription_response', 'context', 'plan', 'error', 'error_stage', 'transcribe_attempts', 'route_attempts', 'retry_at'}
        if not set(values) <= allowed:
            raise IntakeError('invalid_archive_update')
        values['updated'] = time.time()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            source = self.require_active(db, input_id)
            if values.get('phase') in ('queued', 'ready', 'transcribing', 'acknowledged', 'routing', 'payload_pending', 'extracting', 'planned', 'confirming', 'blocked'):
                if json.loads(source['metadata']).get('dry_run', False):
                    values['phase'] = 'dry_' + values['phase']
            db.execute('UPDATE command_inputs SET ' + ','.join(key + '=?' for key in values) + ' WHERE id=?', (*values.values(), input_id))

    def save_route(self, input_id, plan, context, *, connection=None):
        with (self.db() if connection is None else contextlib.nullcontext(connection)) as db:
            if connection is None:
                db.execute('BEGIN IMMEDIATE')
            row = self.require_active(db, input_id)
            phase = 'dry_payload_pending' if json.loads(row['metadata']).get('dry_run', False) else 'payload_pending'
            db.execute('UPDATE command_inputs SET phase=?,plan=?,context=?,updated=? WHERE id=?',
                       (phase, json.dumps(plan, ensure_ascii=False), json.dumps(context, ensure_ascii=False), time.time(), input_id))

    def plan(self, input_id, plan, recipe, context, *, connection=None):
        with (self.db() if connection is None else contextlib.nullcontext(connection)) as db:
            if connection is None:
                db.execute('BEGIN IMMEDIATE')
            source = self.require_active(db, input_id)
            if source['conversation']:
                import triage_conversation
                triage_conversation.fence(db, source)
            if json.loads(source['metadata']).get('dry_run', False) != recipe.get('dry_run', False):
                raise IntakeError('frozen_mode_mismatch')
            db.execute('INSERT INTO command_actions(input_id,recipe,recipe_hash,state,updated) VALUES(?,?,?,?,?)',
                       (input_id, json.dumps(recipe, ensure_ascii=False), digest(recipe), 'planned', time.time()))
            db.execute("UPDATE command_inputs SET phase=?,plan=?,context=?,error=NULL,error_stage=NULL,updated=? WHERE id=?",
                       ('dry_planned' if recipe.get('dry_run') else 'planned', json.dumps(plan, ensure_ascii=False), json.dumps(context, ensure_ascii=False), time.time(), input_id))

    def action(self, input_id):
        with self.db() as db:
            row = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
        return dict(row) if row else None

    def start_action(self, input_id):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            source = self.require_active(db, input_id)
            row = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
            if not row or row['state'] != 'planned':
                raise IntakeError('action_not_planned')
            recipe = json.loads(row['recipe'])
            if digest(recipe) != row['recipe_hash']:
                raise IntakeError('frozen_action_changed')
            if recipe.get('dry_run', False) is not False or json.loads(source['metadata']).get('dry_run', False):
                raise IntakeError('dry_run_execution_forbidden')
            approved = json.loads(source['metadata']).get('approval')
            if source['conversation'] or approved:
                import triage_conversation
                triage_conversation.fence(db, source)
            resolved = source['conversation'] and json.loads(source['conversation']).get('state') == 'resolved'
            if approved or resolved:
                import triage_proposals
                try:
                    current = triage_proposals.current_settings(self.config)
                    triage_proposals.validate_authority(source, current)
                except (triage_proposals.ProposalError, action_registry.RegistryError) as exc:
                    raise IntakeError('stale_proposal_authority') from exc
            db.execute("UPDATE command_actions SET state='attempting',attempts=attempts+1,updated=? WHERE input_id=?", (time.time(), input_id))
            db.execute("UPDATE command_inputs SET phase='dispatching',updated=? WHERE id=?", (time.time(), input_id))
        return recipe

    def finish_preview(self, input_id, *, connection=None):
        # No external attempt: a crash before this atomic commit can safely
        # recompute the preview. Speech claims remain independently durable.
        with (self.db() if connection is None else contextlib.nullcontext(connection)) as db:
            if connection is None:
                db.execute('BEGIN IMMEDIATE')
            source = self.require_active(db, input_id)
            if source['conversation']:
                import triage_conversation
                triage_conversation.fence(db, source)
            row = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
            if not row or row['state'] != 'planned':
                raise IntakeError('action_not_planned')
            recipe = json.loads(row['recipe'])
            if digest(recipe) != row['recipe_hash'] or recipe.get('dry_run') is not True:
                raise IntakeError('invalid_preview_recipe')
            result = {'state': 'dry_run', 'dry_run': True, 'executed': False,
                      **{key: recipe[key] for key in ('action', 'scope', 'project', 'target', 'title', 'priority') if key in recipe}}
            if action_registry.custom(recipe['action']) or recipe.get('original_text', recipe['text']) != recipe['text']:
                result['payload_preview'] = recipe['text'][:240] + ('…' if len(recipe['text']) > 240 else '')
            db.execute("UPDATE command_actions SET state='dry_run',receipt=?,updated=? WHERE input_id=?",
                       (json.dumps(result), time.time(), input_id))
            db.execute("UPDATE command_inputs SET phase='dry_confirming',updated=? WHERE id=?", (time.time(), input_id))

    def finish_action(self, input_id, state, receipt=None, error=None):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.require_active(db, input_id)
            db.execute('UPDATE command_actions SET state=?,receipt=?,error=?,updated=? WHERE input_id=?',
                       (state, json.dumps(receipt, ensure_ascii=False) if receipt is not None else None, error, time.time(), input_id))
            db.execute("UPDATE command_inputs SET phase='confirming',error=?,error_stage=?,updated=? WHERE id=?",
                       (error, 'dispatch' if error else None, time.time(), input_id))

    def claim_speech(self, input_id, kind):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            self.require_active(db, input_id)
            cursor = db.execute("INSERT OR IGNORE INTO command_speech(input_id,kind,state,updated) VALUES(?,?,'attempting',?)", (input_id, kind, time.time()))
        return cursor.rowcount == 1

    def finish_speech(self, input_id, kind, state, error=None):
        with self.db() as db:
            db.execute('UPDATE command_speech SET state=?,error=?,updated=? WHERE input_id=? AND kind=?', (state, error, time.time(), input_id, kind))

    def recover(self):
        # Called only while holding the same worker.lock as the legacy worker.
        # Read-only stages can resume. No uncertain mutation or speech replays.
        with self.db() as db:
            db.execute("UPDATE command_inputs SET phase='payload_pending' WHERE phase='extracting' AND cancelled_at IS NULL")
            db.execute("UPDATE command_inputs SET phase='dry_payload_pending' WHERE phase='dry_extracting' AND cancelled_at IS NULL")
            db.execute("UPDATE command_inputs SET phase='dry_queued' WHERE phase='dry_transcribing' AND cancelled_at IS NULL")
            db.execute("UPDATE command_inputs SET phase='dry_ready' WHERE phase='dry_routing' AND cancelled_at IS NULL")
            db.execute("UPDATE command_inputs SET phase='queued' WHERE phase='transcribing' AND cancelled_at IS NULL")
            db.execute("UPDATE command_inputs SET phase='ready' WHERE phase='routing' AND cancelled_at IS NULL")
            db.execute("UPDATE command_actions SET state='indeterminate',error='action_interrupted',updated=? WHERE state='attempting'", (time.time(),))
            db.execute("UPDATE command_inputs SET phase='confirming',error='action_interrupted',error_stage='dispatch' WHERE phase='dispatching' AND cancelled_at IS NULL")
            db.execute("UPDATE command_speech SET state='interrupted',error='playback_interrupted',updated=? WHERE state='attempting'", (time.time(),))

    def resolve(self, input_id, action):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self.require_active(db, input_id)
            if row['phase'] not in ('blocked', 'dry_blocked'):
                raise IntakeError('only_blocked_input_can_be_resolved')
            operation = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
            if action == 'skip':
                phase = 'skipped'
            elif action == 'retry' and operation:
                recipe = json.loads(operation['recipe'])
                if operation['state'] == 'not_started' or (operation['state'] == 'indeterminate' and recipe['idempotent']):
                    db.execute("UPDATE command_actions SET state='planned',error=NULL,updated=? WHERE input_id=?", (time.time(), input_id))
                    phase = 'planned'
                else:
                    raise IntakeError('action_requires_authoritative_reconciliation_no_replay')
            elif action == 'retry':
                legacy = None
                if row['error_stage'] == 'legacy_check':
                    body = db.execute('SELECT body FROM command_requests WHERE input_id=? ORDER BY seq LIMIT 1', (input_id,)).fetchone()[0]
                    audio = db.execute('SELECT audio FROM command_inputs WHERE id=?', (input_id,)).fetchone()[0]
                    legacy = self.legacy_match(json.loads(row['metadata']), audio, body)
                route = json.loads(row['plan'] or '{}')
                phase = ('retained' if legacy else 'payload_pending' if route.get('payload_policy') == 'extract-v1'
                         else 'ready' if row['text'] is not None else 'queued')
                db.execute('UPDATE command_inputs SET transcribe_attempts=0,retry_at=0,legacy_reference=? WHERE id=?',
                           (json.dumps(legacy) if legacy else None, input_id))
            else:
                raise IntakeError('invalid_resolution')
            if json.loads(row['metadata']).get('dry_run', False) and phase in ('queued', 'ready', 'payload_pending', 'planned'):
                phase = 'dry_' + phase
            db.execute('UPDATE command_inputs SET phase=?,error=NULL,error_stage=NULL,updated=? WHERE id=?', (phase, time.time(), input_id))
            db.execute('INSERT INTO command_audit(at,input_id,action) VALUES(?,?,?)', (time.time(), input_id, action))

    def status(self):
        with self.db() as db:
            counts = dict(db.execute('SELECT phase,count(*) FROM command_inputs GROUP BY phase'))
            row = db.execute(f"SELECT id,phase,error,error_stage FROM command_inputs WHERE phase NOT IN {TERMINAL_SQL} AND phase NOT IN ('blocked','dry_blocked') ORDER BY seq LIMIT 1").fetchone()
            requests = db.execute('SELECT count(*) FROM command_requests').fetchone()[0]
        try:
            actions = action_registry.catalog(self.config['commands'])
            registry_error = None
        except action_registry.RegistryError as exc:
            actions, registry_error = {}, str(exc)
        return {'pipeline': 'commands', 'dispatch_order': 'per_destination',
                'scheduling': 'bounded_stage_workers', 'stage_workers': self.config['commands'].get('stage_workers', 4),
                'unknown_route_barrier': 'unfinished_only', 'failed_before_effect_barrier': False, 'failed_input_barrier': False, 'server_speech_concurrency': 1,
                'dry_run_supported': True, 'status_feed_supported': True,
                'action_manifest_supported': True, 'custom_actions': action_registry.describe(actions), 'action_manifest_error': registry_error,
                'supported_actions': sorted((ACTIONS - {'route'}) | set(action_registry.describe(actions))), 'payload_extractor': extraction_capability(self.config), 'captures': counts, 'requests': requests,
                'head': dict(row) if row else None, 'legacy_audio': self.legacy.status()}

    def receipt(self, input_id, *, connection=None):
        with (self.db() if connection is None else contextlib.nullcontext(connection)) as db:
            if connection is None:
                db.execute('BEGIN')
            record = db.execute('''SELECT id,phase,created,updated,error,error_stage,conversation,
                metadata,cancelled_at,plan,legacy_reference FROM command_inputs WHERE id=?''', (input_id,)).fetchone()
            if record is None:
                raise IntakeError('input_not_found')
            action = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
            speech = [dict(row) for row in db.execute('SELECT kind,state,error FROM command_speech WHERE input_id=? ORDER BY kind', (input_id,))]
        result = {key: record[key] for key in ('id', 'phase', 'created', 'updated', 'error', 'error_stage')}
        result['speech'] = speech
        if record['phase'] in ('blocked', 'dry_blocked'):
            before_effect = failed_before_effect(action['state'] if action else None, action['attempts'] if action else None)
            result['queue_guard'] = {'blocks_following': False,
                                     'reason': 'pre_dispatch_failure' if before_effect else 'unconfirmed_prior_effect'}
        if record['conversation']:
            conversation = json.loads(record['conversation'])
            result['conversation'] = {key: conversation[key] for key in
                ('state', 'slot', 'response', 'reference', 'revision', 'decision', 'provenance', 'proposal', 'selection', 'reply_observation') if key in conversation}
        result['dry_run'] = json.loads(record['metadata']).get('dry_run', False)
        if record['cancelled_at'] is not None:
            result['cancellation'] = {'state': 'cancelled', 'at': record['cancelled_at'],
                                      'no_action_command_started': True, 'read_only_work_may_continue': True}
        if result['dry_run'] and record['plan']:
            plan = json.loads(record['plan'])
            decision = plan.get('decision', {})
            result['triage'] = {
                'source': decision.get('source'), 'model': decision.get('model'),
                'context_revision': plan.get('context_revision'), 'fallback_reason': plan.get('fallback_reason'),
                'choices': {name: {key: answer.get(key) for key in ('choice', 'confidence')}
                            for name, answer in decision.get('answers', {}).items()},
            }
        if record['plan']:
            extraction_plan = json.loads(record['plan'])
            if extraction_plan.get('payload_policy') == 'extract-v1':
                evidence = extraction_plan.get('extraction', {})
                result['extraction'] = {key: evidence.get(key) for key in ('requested_model', 'model', 'elapsed_ms')}
                result['extraction']['status'] = 'complete' if 'payload' in extraction_plan else 'failed' if record['error_stage'] == 'extract' else 'pending'
                if 'payload' in extraction_plan:
                    result['extraction']['payload_preview'] = extraction_plan['payload'][:240]
                    result['extraction']['payload_bytes'] = len(extraction_plan['payload'].encode())
        if record['legacy_reference']:
            result['legacy_reference'] = json.loads(record['legacy_reference'])
        if action:
            recipe = json.loads(action['recipe'])
            outcome = json.loads(action['receipt']) if action['receipt'] else {}
            result['action'] = {'state': action['state'], 'attempts': action['attempts'], 'error': action['error'],
                                **{key: recipe[key] for key in ('action', 'scope', 'project', 'target', 'request_id') if key in recipe},
                                **{key: outcome[key] for key in ('message_id', 'admission', 'delivery', 'beads', 'dry_run', 'executed', 'title', 'priority', 'payload_preview', 'exit_code', 'elapsed_ms') if key in outcome}}
        return result

    def export(self, input_id, destination):
        try:
            record = self.get(input_id)
        except IntakeError:
            return self.legacy.export(input_id, destination)
        destination.mkdir(mode=0o700, parents=True, exist_ok=False)
        audio = record.pop('audio')
        if audio is not None:
            mime = json.loads(record['metadata'])['audio_mime']
            (destination / ('audio.' + ('wav' if MIMES[mime] == 'wav' else 'm4a'))).write_bytes(audio)
        (destination / 'input.json').write_text(json.dumps(record, ensure_ascii=False, indent=2))
        (destination / 'receipt.json').write_text(json.dumps(self.receipt(input_id), ensure_ascii=False, indent=2))
        (destination / 'action.json').write_text(json.dumps(self.action(input_id), ensure_ascii=False, indent=2))
        with self.db() as db:
            for request in db.execute('SELECT * FROM command_requests WHERE input_id=? ORDER BY seq', (input_id,)):
                (destination / f"request-{request['seq']}.body").write_bytes(request['body'])
                (destination / f"request-{request['seq']}.json").write_text(json.dumps({key: request[key] for key in request.keys() if key != 'body'}))
