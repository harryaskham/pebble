"""Durable Triage turns linked atomically to the existing command owner.

One authenticated operator principal (the receiver's existing credential). No new
worker, Caco transport, transcript replay, or model-generated receipt authority.
"""
from __future__ import annotations
import contextlib
import hashlib
import json
import re
import shutil
import time
import action_registry
import dispatch
import triage_proposals
import triage_conversation
import uuid
from intake import IntakeError

MAX_EVENTS = 256
EVENT_PAGE_BYTES = 65536
DONE = {'created', 'stored', 'completed', 'skipped', 'retained', 'dry_run', 'blocked', 'dry_blocked', 'clarification_required', 'answered'}


class Error(Exception):
    def __init__(self, code, status=400, details=None):
        self.code, self.status, self.details = code, status, details
        super().__init__(code)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,160}', value):
        raise Error('invalid_identity')
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def event_text(data):
    phase = data.get('phase', '')
    normal = phase.removeprefix('dry_') if phase != 'dry_run' else phase
    action = data.get('action') or 'route'
    destination = data.get('project') or data.get('target') or ''
    if phase == 'accepted': return 'Received.'
    if phase in ('clarification_required', 'answered'): return data.get('response') or 'Please clarify. No action executed.'
    if phase == 'reference_consumed': return 'Continued in turn ' + data['consumed_by'] + '.'
    if phase == 'proposal_derived': return 'Server-derived preview from turn ' + data['derived_from'] + '. No action executed.'
    if normal == 'routing': return 'Routing.'
    if normal in ('payload_pending', 'planned'):
        return f"Intent: {action}" + (f" → {destination}" if destination else '') + '.'
    if normal == 'extracting': return 'Preparing payload.'
    if normal == 'dispatching': return f'Dispatching {action}.'
    if phase == 'dry_run':
        return f"Would execute {action}" + (f" in {destination}" if destination else '') + (': ' + data['title'] if data.get('title') else '') + '. No action executed.'
    if phase == 'created':
        beads = data.get('beads') or []
        return 'Created ' + ', '.join(str(x.get('id', x)) if isinstance(x, dict) else str(x) for x in beads) + '.' if beads else 'Expansion finished; no beads created.'
    if phase == 'stored': return 'Message stored. Recipient execution is not confirmed.'
    if phase == 'completed': return 'Custom command exited successfully. Downstream completion is not confirmed.'
    if phase in ('blocked', 'dry_blocked'):
        return 'Retained: ' + (data.get('error') or 'outcome requires inspection') + '. Not resent.'
    if phase == 'skipped' and data.get('cancelled_at') is not None:
        return 'Cancelled before dispatch. No action command started; read-only model work may have continued.'
    if phase in ('retained', 'skipped'): return 'Retained without dispatch.'
    return ''


def approval_source(archive, db, session, reference, revision):
    """Shared typed/natural approval guard; caller owns the admission transaction."""
    target = db.execute("SELECT input_id FROM triage_turns WHERE id=? AND session_id=? AND operation IN ('submit','proposal') AND mode='dry_run'",
                        (reference, session)).fetchone()
    if not target: raise Error('proposal_not_approvable', 409)
    if db.execute('SELECT 1 FROM triage_links WHERE source_turn=?', (reference,)).fetchone():
        raise Error('proposal_already_consumed', 409)
    source = db.execute('SELECT * FROM command_inputs WHERE id=?', (target['input_id'],)).fetchone()
    action = db.execute('SELECT * FROM command_actions WHERE input_id=?', (target['input_id'],)).fetchone()
    actual_revision = triage_proposals.proposal_revision(source, action)
    if actual_revision is None: raise Error('proposal_not_approvable', 409)
    if actual_revision != revision: raise Error('stale_proposal_revision', 409)
    try:
        current = triage_proposals.current_settings(archive.config)
        triage_proposals.validate_authority(source, current)
    except action_registry.RegistryError as exc:
        raise Error('stale_proposal_authority', 409) from exc
    except triage_proposals.ProposalError as exc:
        raise Error(str(exc), 409) from exc
    return source, current


class Turns:
    def __init__(self, archive, *, initialize=True):
        self.archive = archive
        if not initialize: return
        with archive.db() as db:
            db.executescript('''
              BEGIN IMMEDIATE;
              CREATE TABLE IF NOT EXISTS triage_sessions (id TEXT PRIMARY KEY, created REAL NOT NULL);
              CREATE TABLE IF NOT EXISTS triage_turns (
                id TEXT PRIMARY KEY, session_id TEXT NOT NULL, client_turn_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, input_id TEXT, reference_turn TEXT, operation TEXT NOT NULL,
                text TEXT NOT NULL, mode TEXT NOT NULL, created REAL NOT NULL,
                event_floor INTEGER NOT NULL DEFAULT 0,
                UNIQUE(session_id,client_turn_id));
              CREATE TABLE IF NOT EXISTS triage_links (
                source_turn TEXT PRIMARY KEY, owner_turn TEXT UNIQUE NOT NULL,
                revision TEXT NOT NULL, kind TEXT NOT NULL);
              CREATE TABLE IF NOT EXISTS triage_selections (
                question_turn TEXT PRIMARY KEY, owner_turn TEXT UNIQUE NOT NULL,
                revision TEXT NOT NULL, target_turn TEXT NOT NULL, target_revision TEXT);
              CREATE INDEX IF NOT EXISTS triage_input ON triage_turns(input_id);
              CREATE INDEX IF NOT EXISTS triage_session_floor ON triage_turns(session_id,event_floor);
              CREATE TABLE IF NOT EXISTS triage_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, turn_id TEXT NOT NULL, at REAL NOT NULL,
                kind TEXT NOT NULL, data TEXT NOT NULL);
              CREATE INDEX IF NOT EXISTS triage_event_turn ON triage_events(turn_id,seq);
              DROP TRIGGER IF EXISTS triage_link_event;
              DROP TRIGGER IF EXISTS triage_selection_event;
              DROP TRIGGER IF EXISTS triage_transition;
              CREATE TRIGGER triage_transition AFTER UPDATE OF phase ON command_inputs
              WHEN NEW.phase != OLD.phase AND EXISTS(SELECT 1 FROM triage_turns WHERE input_id=NEW.id AND operation IN ('submit','approve','proposal'))
              BEGIN
                INSERT INTO triage_events(turn_id,at,kind,data)
                SELECT t.id,NEW.updated,NEW.phase,json_object(
                  'phase',NEW.phase,'action',json_extract(NEW.plan,'$.action'),
                  'project',json_extract(NEW.plan,'$.project'),'target',json_extract(NEW.plan,'$.target'),
                  'error',NEW.error,'cancelled_at',NEW.cancelled_at,'beads',json_extract(a.receipt,'$.beads'),
                  'title',json_extract(a.recipe,'$.title'),'response',json_extract(NEW.conversation,'$.response'))
                FROM triage_turns t LEFT JOIN command_actions a ON a.input_id=NEW.id
                WHERE t.input_id=NEW.id AND t.operation IN ('submit','approve','proposal');
                UPDATE triage_turns SET event_floor=max(event_floor,coalesce((
                  SELECT max(seq) FROM triage_events WHERE turn_id=triage_turns.id
                    AND seq NOT IN (SELECT seq FROM triage_events WHERE turn_id=triage_turns.id ORDER BY seq DESC LIMIT 256)
                ),0)) WHERE input_id=NEW.id AND operation IN ('submit','approve','proposal');
                DELETE FROM triage_events WHERE turn_id IN
                  (SELECT id FROM triage_turns WHERE input_id=NEW.id AND operation IN ('submit','approve','proposal'))
                  AND seq <= (SELECT event_floor FROM triage_turns WHERE id=triage_events.turn_id);
              END;
            ''')
            if 'expected_revision' not in {r[1] for r in db.execute('PRAGMA table_info(triage_turns)')}:
                db.execute('ALTER TABLE triage_turns ADD COLUMN expected_revision TEXT')
            if 'derived_from' not in {r[1] for r in db.execute('PRAGMA table_info(triage_turns)')}:
                db.execute('ALTER TABLE triage_turns ADD COLUMN derived_from TEXT')
            db.execute('CREATE UNIQUE INDEX IF NOT EXISTS triage_derived ON triage_turns(derived_from) WHERE derived_from IS NOT NULL')
            # Existing explicit approvals consumed their preview too. Backfill
            # only ownership tombstones, never commands, phases or old events.
            db.execute('''INSERT OR IGNORE INTO triage_links(source_turn,owner_turn,revision,kind)
                SELECT reference_turn,id,expected_revision,'approve' FROM triage_turns
                WHERE operation='approve' AND reference_turn IS NOT NULL AND expected_revision IS NOT NULL
                ORDER BY created,id''')
            # New relationships are observable without rewriting any old input,
            # preview or event. Install after the silent historical backfill.
            db.execute('''CREATE TRIGGER triage_link_event AFTER INSERT ON triage_links
              BEGIN
                INSERT INTO triage_events(turn_id,at,kind,data)
                VALUES(NEW.source_turn,(julianday('now')-2440587.5)*86400.0,'reference_consumed',
                  json_object('phase','reference_consumed','consumed_by',NEW.owner_turn,'relation',NEW.kind));
                UPDATE triage_turns SET event_floor=max(event_floor,coalesce((
                  SELECT max(seq) FROM triage_events WHERE turn_id=NEW.source_turn
                    AND seq NOT IN (SELECT seq FROM triage_events WHERE turn_id=NEW.source_turn ORDER BY seq DESC LIMIT 256)
                ),0)) WHERE id=NEW.source_turn;
                DELETE FROM triage_events WHERE turn_id=NEW.source_turn
                  AND seq <= (SELECT event_floor FROM triage_turns WHERE id=NEW.source_turn);
              END''')
            db.execute('''CREATE TRIGGER triage_selection_event AFTER INSERT ON triage_selections
              BEGIN
                INSERT INTO triage_events(turn_id,at,kind,data)
                VALUES(NEW.question_turn,(julianday('now')-2440587.5)*86400.0,'reference_consumed',
                  json_object('phase','reference_consumed','consumed_by',NEW.owner_turn,'relation','selection'));
                UPDATE triage_turns SET event_floor=max(event_floor,coalesce((
                  SELECT max(seq) FROM triage_events WHERE turn_id=NEW.question_turn
                    AND seq NOT IN (SELECT seq FROM triage_events WHERE turn_id=NEW.question_turn ORDER BY seq DESC LIMIT 256)
                ),0)) WHERE id=NEW.question_turn;
                DELETE FROM triage_events WHERE turn_id=NEW.question_turn
                  AND seq <= (SELECT event_floor FROM triage_turns WHERE id=NEW.question_turn);
              END''')

    def capacity(self, db, payload_bytes=0):
        from intake import TERMINAL_SQL
        pending = db.execute(f'SELECT count(*) FROM command_inputs WHERE phase NOT IN {TERMINAL_SQL}').fetchone()[0]
        reserve = 16384 + 2 * payload_bytes + pending * 3 * 1024 * 1024
        used = sum(p.stat().st_size for p in self.archive.root.glob('archive.sqlite3*'))
        if used + reserve > self.archive.config['max_archive_bytes']: raise Error('archive_quota_reached', 503)
        if shutil.disk_usage(self.archive.root).free - reserve < self.archive.config['min_free_bytes']: raise Error('archive_disk_low', 503)

    def session(self, session_id):
        identifier(session_id)
        with self.archive.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM triage_sessions WHERE id=?', (session_id,)).fetchone():
                self.capacity(db)
                if db.execute('SELECT count(*) FROM triage_sessions').fetchone()[0] >= 10000:
                    raise Error('session_quota', 429)
                db.execute('INSERT INTO triage_sessions VALUES(?,?)', (session_id, time.time()))
            return dict(db.execute('SELECT * FROM triage_sessions WHERE id=?', (session_id,)).fetchone())

    def sessions(self):
        with self.archive.db() as db:
            return {'sessions': [dict(r) for r in db.execute('SELECT * FROM triage_sessions ORDER BY created DESC LIMIT 100')]}

    def session_turns(self, session_id):
        identifier(session_id)
        with self.archive.db() as db:
            if not db.execute('SELECT 1 FROM triage_sessions WHERE id=?', (session_id,)).fetchone(): raise Error('session_not_found', 404)
            return {'id': session_id, 'turns': [dict(r) for r in db.execute(
                'SELECT id,client_turn_id,text,mode,created FROM triage_turns WHERE session_id=? ORDER BY created DESC LIMIT 100', (session_id,))]}

    def submit(self, session_id, request):
        identifier(session_id)
        if not isinstance(request, dict) or set(request) - {'client_turn_id', 'text', 'mode', 'route', 'operation', 'reference_turn', 'expected_revision'}:
            raise Error('invalid_turn')
        key = identifier(request.get('client_turn_id'))
        if key.startswith('server-proposal:'): raise Error('reserved_turn_identity')
        text, mode = request.get('text'), request.get('mode', 'dry_run')
        operation, reference = request.get('operation', 'submit'), request.get('reference_turn')
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 16384 or '\0' in text: raise Error('invalid_text')
        if mode not in ('live', 'dry_run') or operation not in ('submit', 'inspect', 'approve'): raise Error('invalid_turn_mode')
        route = request.get('route', 'route')
        if not isinstance(route, str): raise Error('invalid_route')
        if reference is not None: identifier(reference)
        revision = request.get('expected_revision')
        if operation != 'approve' and 'expected_revision' in request: raise Error('unexpected_proposal_revision')
        # Preserve the base API's existing submit/inspect identity digests.
        fingerprint = digest([text, mode, route, operation, reference] + ([revision] if operation == 'approve' else []))
        with self.archive.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if not db.execute('SELECT 1 FROM triage_sessions WHERE id=?', (session_id,)).fetchone(): raise Error('session_not_found', 404)
            old = db.execute('SELECT id,fingerprint FROM triage_turns WHERE session_id=? AND client_turn_id=?', (session_id, key)).fetchone()
            if old:
                if old['fingerprint'] != fingerprint: raise Error('turn_identity_conflict', 409)
                turn_id = old['id']
            else:
                if db.execute('SELECT count(*) FROM triage_turns').fetchone()[0] >= 100000: raise Error('turn_quota', 429)
                turn_id, now = uuid.uuid4().hex, time.time()
                approval = None
                if operation == 'approve':
                    if mode != 'live': raise Error('approval_requires_live_mode')
                    if route != 'route': raise Error('approval_route_override_forbidden')
                    if not reference or not isinstance(revision, str) or not re.fullmatch(r'[a-f0-9]{64}', revision):
                        raise Error('approval_reference_and_revision_required')
                    source, current = approval_source(self.archive, db, session_id, reference, revision)
                    approval = {'source': source, 'settings': current}
                if operation == 'inspect':
                    self.capacity(db, len(text.encode()))
                    target = db.execute('SELECT input_id FROM triage_turns WHERE id=? AND session_id=?', (reference, session_id)).fetchone()
                    if not target: raise Error('reference_not_found', 404)
                    input_id = target['input_id']
                else:
                    # Stable admission body is written only on the first unique turn.
                    original = json.loads(approval['source']['plan'])['text'] if approval else text
                    body = json.dumps({'transcription': original, 'client': 'triage', 'recordedAt': str(int(now * 1000))}).encode()
                    try:
                        result = self.archive.accept(body, 'application/json', route,
                            'triage:' + digest([session_id, key]), dry_run=mode == 'dry_run',
                            speech_owner='client', connection=db,
                            **({'actions': approval['settings']['commands']['actions'], 'config_snapshot': approval['settings']} if approval else {}))
                    except IntakeError as exc: raise Error(exc.code, 400 if exc.code.startswith('invalid') else 503) from exc
                    except action_registry.RegistryError as exc: raise Error(str(exc), 503) from exc
                    if result['error']: raise Error(result['error'], 409)
                    input_id = result['id']
                    if approval:
                        if result['phase'] != 'ready' or result['duplicate']:
                            raise Error('proposal_admission_conflict', 409)
                        db.execute("UPDATE command_inputs SET metadata=json_set(metadata,'$.approval',json(?)) WHERE id=?",
                                   (json.dumps({'reference_turn': reference, 'expected_revision': revision}), input_id))
                if operation == 'submit':
                    try:
                        conversation = triage_conversation.snapshot(db, session_id, reference, text=text)
                    except IntakeError as exc: raise Error(exc.code, 404) from exc
                    db.execute('UPDATE command_inputs SET conversation=? WHERE id=?', (json.dumps(conversation), input_id))
                db.execute('INSERT INTO triage_turns(id,session_id,client_turn_id,fingerprint,input_id,reference_turn,operation,text,mode,created,expected_revision) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (turn_id, session_id, key, fingerprint, input_id, reference, operation, text, mode, now, revision))
                db.execute('INSERT INTO triage_events(turn_id,at,kind,data) VALUES(?,?,?,?)',
                    (turn_id, now, 'accepted', json.dumps({'phase': 'accepted'})))
                if approval:
                    db.execute('INSERT INTO triage_links(source_turn,owner_turn,revision,kind) VALUES(?,?,?,?)',
                               (reference, turn_id, revision, 'approve'))
                    source = approval['source']
                    plan = json.loads(source['plan'])
                    recipe = dispatch.prepare(plan, input_id, approval['settings']['commands'], dry_run=False)
                    self.archive.plan(input_id, plan, recipe, json.loads(source['context']), connection=db)
        return self.get(turn_id)

    def cancel(self, turn_id):
        identifier(turn_id)
        try:
            with self.archive.db() as db:
                db.execute('BEGIN IMMEDIATE')
                turn = db.execute('SELECT input_id,operation FROM triage_turns WHERE id=?', (turn_id,)).fetchone()
                if not turn: raise Error('turn_not_found', 404)
                if turn['operation'] not in ('submit', 'approve', 'proposal'): raise IntakeError('inspection_not_cancellable')
                self.archive.cancel(turn['input_id'], connection=db)
        except IntakeError as exc:
            # Read only after rollback/release. Never label a rejected race as a
            # cancellation, nor overwrite an attempting/indeterminate receipt.
            raise Error(exc.code, 409, {'turn': self.get(turn_id)}) from exc
        return self.get(turn_id)

    def get(self, turn_id):
        identifier(turn_id)
        with self.archive.db() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT * FROM triage_turns WHERE id=?', (turn_id,)).fetchone()
            if not row: raise Error('turn_not_found', 404)
            result = {k: row[k] for k in ('id', 'session_id', 'client_turn_id', 'text', 'mode', 'created', 'operation', 'reference_turn', 'expected_revision')}
            input_id = row['input_id']
            source = db.execute('SELECT * FROM command_inputs WHERE id=?', (input_id,)).fetchone()
            action = db.execute('SELECT * FROM command_actions WHERE input_id=?', (input_id,)).fetchone()
            result['proposal_revision'] = (triage_proposals.proposal_revision(source, action)
                if row['operation'] in ('submit', 'proposal') and row['mode'] == 'dry_run' else None)
            link = db.execute('''SELECT owner_turn FROM triage_links WHERE source_turn=?
                UNION ALL SELECT owner_turn FROM triage_selections WHERE question_turn=?''', (turn_id, turn_id)).fetchone()
            result['consumed_by'] = link['owner_turn'] if link else None
            result['requires_input'] = bool(row['operation'] == 'submit' and (triage_conversation.draft_revision(source)
                or triage_conversation.question_revision(source)) and not link)
            if row['derived_from']:
                result['derived_from'] = row['derived_from']
            receipt = self.archive.receipt(input_id, connection=db)
            result.update(receipt=receipt, phase=receipt['phase'], done=receipt['phase'] in DONE)
            result['clarification_revision'] = triage_conversation.draft_revision(source)
            if receipt.get('conversation', {}).get('state') in ('resolved', 'approved', 'refined'):
                result['resolved_reference'] = receipt['conversation']['reference']
            if receipt.get('conversation', {}).get('proposal'):
                result['derived_proposal'] = receipt['conversation']['proposal']
            events = self.event_page('turn', turn_id, connection=db)
            result.update(events=events['events'], cursor=events['cursor'], retention_floor=events['retention_floor'])
            if result['operation'] == 'inspect': result['done'] = True
            return result

    def replies(self, turn_id):
        # Resolve authority from our own receipt, not a client/model message ID.
        # Release all SQLite snapshots before network observation.
        import triage_replies
        turn = self.get(turn_id)
        source = self.archive.get(turn['receipt']['id'])
        frozen = json.loads(source['settings'])
        return {'turn_id': turn_id, 'observation': triage_replies.observe(
            turn['receipt'], frozen, current_config=self.archive.config)}

    def events(self, turn_id, after=0, *, initial=True):
        return self.event_page('turn', turn_id, after, initial=initial)

    def session_events(self, session_id, after=0, *, initial=True):
        return self.event_page('session', session_id, after, initial=initial)

    def event_page(self, scope, identity, after=0, *, initial=True, connection=None):
        identifier(identity)
        if type(after) is not int or not 0 <= after <= 9223372036854775807:
            raise Error('invalid_cursor')
        reset = after == 0 and initial
        field = 't.id' if scope == 'turn' else 't.session_id'
        with (self.archive.db() if connection is None else contextlib.nullcontext(connection)) as db:
            # Floor, high-water, rows and reset snapshot share one read snapshot.
            # Never retain this transaction while writing to an HTTP consumer.
            if connection is None:
                db.execute('BEGIN')
            if scope == 'turn':
                turn = db.execute('SELECT session_id FROM triage_turns WHERE id=?', (identity,)).fetchone()
                if not turn: raise Error('turn_not_found', 404)
                session_id = turn['session_id']
            else:
                if not db.execute('SELECT 1 FROM triage_sessions WHERE id=?', (identity,)).fetchone():
                    raise Error('session_not_found', 404)
                session_id = identity
            floor = db.execute(f'SELECT coalesce(max(t.event_floor),0) FROM triage_turns t WHERE {field}=?', (identity,)).fetchone()[0]
            high = db.execute(f'''SELECT coalesce(max(e.seq),0) FROM triage_events e
                JOIN triage_turns t ON t.id=e.turn_id WHERE {field}=?''', (identity,)).fetchone()[0]
            if not reset and after < floor:
                raise Error('cursor_expired_resync_' + scope, 409,
                            {'retention_floor': floor, 'resync': f'/route/{scope}s/{identity}/events?after=0'})
            if after > high: raise Error('cursor_ahead_of_scope', 409)
            order = 'DESC' if reset else 'ASC'
            rows = db.execute(f'''SELECT e.seq,e.turn_id,e.at,e.kind,
                CASE WHEN length(CAST(e.data AS BLOB))<=8192 THEN e.data END AS data
                FROM triage_events e JOIN triage_turns t ON t.id=e.turn_id
                WHERE {field}=? AND e.seq>? ORDER BY e.seq {order} LIMIT 257''', (identity, after))
            events, size, omitted = [], 0, False
            for row in rows:
                text = event_text(json.loads(row['data'])) if row['data'] is not None else 'Event details require turn inspection.'
                clipped = len(text.encode()) > 2048 or row['data'] is None
                text = text.encode()[:2048].decode('utf-8', errors='ignore')
                event = {'seq': row['seq'], 'session_id': session_id, 'turn_id': row['turn_id'],
                         'kind': row['kind'], 'at': row['at'], 'text': text}
                if clipped: event['details_truncated'] = True
                width = len(json.dumps(event).encode())
                if len(events) == MAX_EVENTS or size + width > EVENT_PAGE_BYTES:
                    omitted = True
                    break
                events.append(event)
                size += width
            if reset: events.reverse()
            # Small, secret-free phase snapshots, not raw command settings,
            # transcripts, recipes, subprocess output or credential references.
            summaries = db.execute(f'''SELECT t.id,t.client_turn_id,t.operation,t.mode,t.reference_turn,
                i.phase,i.updated FROM triage_turns t JOIN command_inputs i ON i.id=t.input_id
                WHERE {field}=? ORDER BY t.created DESC,t.id DESC LIMIT 101''', (identity,)).fetchall()
            done = scope == 'turn' and (summaries[0]['phase'] in DONE or summaries[0]['operation'] == 'inspect')
            result = {'session_id': session_id, 'events': events, 'cursor': high if reset else events[-1]['seq'] if events else after,
                      'retention_floor': floor, 'done': done, 'reset': reset, 'has_more': omitted and not reset}
            if scope == 'turn': result['turn_id'] = identity
            if reset:
                result.update(history_complete=not floor and not omitted,
                              snapshot={'turns': [dict(r) for r in summaries[:100]], 'truncated': len(summaries) > 100})
        return result

    def response_text(self, turn):
        if turn.get('consumed_by'):
            return f"Turn {turn['id']} was consumed by turn {turn['consumed_by']}. Inspect or cancel that action owner; do not resubmit the proposal."
        if turn['operation'] == 'inspect':
            a = turn['receipt'].get('action', {})
            return f"Turn {turn['reference_turn']}: {turn['phase']}. " + event_text({
                'phase': turn['phase'], 'action': a.get('action'), 'project': a.get('project'),
                'beads': a.get('beads'), 'title': a.get('title'), 'error': turn['receipt'].get('error'),
                'cancelled_at': turn['receipt'].get('cancellation', {}).get('at')})
        lines = [r['text'] for r in turn['events'] if r['text']]
        # Same selected intent may appear at both routed and planned stages.
        return '\n'.join(dict.fromkeys(lines)) + f"\nTurn {turn['id']}: " + (turn['phase'] if turn['done'] else 'pending; not complete') + '.'
