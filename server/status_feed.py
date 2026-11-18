"""Bounded read-only projections for widgets/SSE; never consume inboxes or run tools."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time

STATUS_PATHS = {'/pebble/status', '/hooks/global/pebble-status'}
EVENT_PATHS = {'/pebble/events', '/hooks/global/pebble-events'}
TERMINAL = {'stored', 'created', 'completed', 'dry_run', 'skipped', 'retained'}


def clipped(value, limit=160):
    return str(value or '')[:limit]


def code(value):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_:-]{1,100}', value) else None


def number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def read_json(filename, limit):
    fd = os.open(Path(filename).expanduser(), os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode): raise ValueError('not_regular')
        raw = stream.read(limit + 1)
    if len(raw) > limit: raise ValueError('too_large')
    return json.loads(raw), info.st_mtime


def attachment(filename):
    # This is the assistant's durable selected attachment, NOT a claim that its
    # runtime is alive or the selected recipient can currently accept delivery.
    result = {'state': 'unknown', 'evidence': 'saved_state', 'runtime_verified': False}
    try:
        data, modified = read_json(filename, 2 * 1024 * 1024)
        if not isinstance(data, dict) or data.get('version') != 1 or 'attachment' not in data:
            raise ValueError('invalid_state')
        target = data['attachment']
        if target is None:
            result.update(state='detached', updated_at=modified)
            return result
        if not isinstance(target, dict) or target.get('kind') not in ('agent', 'project'):
            raise ValueError('invalid_attachment')
        if not isinstance(target.get('project'), str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,199}', target['project']):
            raise ValueError('invalid_project')
        result.update(state='attached', kind=target['kind'], project=target['project'], updated_at=modified)
        if target['kind'] == 'agent':
            value = target.get('target')
            if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,512}', value):
                raise ValueError('invalid_target')
            result['target'] = value
        return result
    except FileNotFoundError:
        result['error'] = 'attachment_state_missing'
    except Exception:
        result = {'state': 'unknown', 'evidence': 'saved_state', 'runtime_verified': False, 'error': 'attachment_state_unreadable'}
    return result


def snapshot(archive, worker, *, limit=5, include_text=False, now=None):
    if type(limit) is not int or not 1 <= limit <= 10: raise ValueError('invalid_limit')
    now = time.time() if now is None else now
    config = archive.config['commands']
    # Only selected small fields; never SELECT * on audio/settings/provider bodies.
    text_field = ",substr(COALESCE(json_extract(i.plan,'$.payload'),i.text),1,160) AS preview" if include_text else ''
    with archive.db() as db:
        db.execute('BEGIN')
        counts = dict(db.execute('SELECT phase,count(*) FROM command_inputs GROUP BY phase'))
        requests = db.execute('SELECT count(*),max(received) FROM command_requests').fetchone()
        rows = db.execute('''SELECT i.id,i.created,i.updated,i.phase,i.error,i.error_stage,i.endpoint_action,
            json_extract(i.metadata,'$.dry_run') AS dry_run,
            json_extract(i.plan,'$.action') AS selected,
            json_extract(i.plan,'$.decision.model') AS model,
            json_extract(i.plan,'$.extraction.model') AS extraction_model,
            json_extract(a.recipe,'$.scope') AS scope,
            json_extract(a.recipe,'$.project') AS project,
            json_extract(a.recipe,'$.target') AS target,
            a.state AS action_state,a.attempts AS attempts,
            json_extract(a.receipt,'$.message_id') AS message_id,
            json_extract(a.receipt,'$.admission.runtime_delivery_state') AS runtime_delivery,
            json_extract(a.receipt,'$.admission.pi_admission_state') AS pi_admission''' + text_field + '''
            FROM command_inputs i LEFT JOIN command_actions a ON i.id=a.input_id ORDER BY i.seq DESC LIMIT ?''', (limit,)).fetchall()
        recent = []
        for row in rows:
            speech = [dict(s) for s in db.execute('SELECT kind,state,error FROM command_speech WHERE input_id=? ORDER BY updated DESC LIMIT 20', (row['id'],))]
            item = {k: row[k] for k in ('id', 'created', 'updated')}
            item.update(phase=code(row['phase']) or 'unknown', action=clipped(row['selected'] or row['endpoint_action'], 64),
                        dry_run=bool(row['dry_run']), error=code(row['error']), error_stage=code(row['error_stage']),
                        action_state=code(row['action_state']), attempts=row['attempts'] or 0,
                        project=clipped(row['project'], 100), target=clipped(row['target'], 160), scope=code(row['scope']),
                        model=clipped(row['model'], 100), extraction_model=clipped(row['extraction_model'], 100),
                        message_id=clipped(row['message_id'], 200), runtime_delivery=code(row['runtime_delivery']), pi_admission=code(row['pi_admission']),
                        speech=[{'kind':code(s['kind']), 'state':code(s['state']), 'error':code(s['error'])} for s in speech])
            if include_text: item['preview'] = clipped(row['preview'])
            recent.append(item)
    pending = sum(count for phase, count in counts.items() if phase not in TERMINAL and phase not in ('blocked', 'dry_blocked'))
    blocked = sum(counts.get(phase, 0) for phase in ('blocked', 'dry_blocked'))
    alive = worker.thread.is_alive()
    fatal = code(worker.fatal)
    state = 'offline' if not alive or fatal else 'attention' if blocked else 'working' if pending else 'ready'
    extraction = config.get('extractor', {})
    actions = ['pebble', 'bd-file', 'bd-expand', 'agent-msg', 'broadcast']
    registry_error = None
    # Forward-compatible with the separate custom-action registry feature.
    if config.get('actions') or config.get('actions_file'):
        try:
            import action_registry
            actions += list(action_registry.describe(action_registry.catalog(config)))
        except Exception:
            registry_error = 'action_manifest_unavailable'
            state = 'attention' if alive and not fatal else state
    context = {'state': 'unknown', 'created_at': None}
    try:
        data, _ = read_json(config['context_file'], 262144)
        created = number(data.get('created_at'))
        if created is None: raise ValueError()
        context = {'state': 'stale' if now-created > config['jev']['max_context_age_secs'] or created-now > 60 else 'fresh', 'created_at': created}
    except Exception:
        context['error'] = 'context_unreadable'
    filename = config.get('status', {}).get('attachment_file') or str(Path(os.environ.get('PEBBLE_ROUTER_STATE_DIR', '~/.local/state/pebble/router')).expanduser()/'pebble-state.json')
    value = {
        'version': 1,
        'service': {'state':state, 'worker_alive':alive, 'error':fatal, 'pending':pending, 'blocked':blocked, 'requests':requests[0], 'last_received_at':requests[1], 'caco_binary':clipped(config['caco']['binary'], 4096)},
        'actions': sorted(actions), 'action_manifest_error':registry_error,
        'extractor': {'enabled':extraction.get('enabled', False), 'model':extraction.get('model')},
        'context': context, 'attachment': attachment(filename), 'recent': recent,
    }
    value['revision'] = hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',', ':')).encode()).hexdigest()[:32]
    value['generated_at'] = now
    return value


def event(value):
    return ('id: '+value['revision']+'\nevent: status\ndata: '+json.dumps(value,ensure_ascii=False,separators=(',', ':'))+'\n\n').encode()
