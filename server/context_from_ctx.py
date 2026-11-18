#!/usr/bin/env python3
"""Project the ctx Cacophony mirror into Pebble's unchanged snapshot v1.

A long-lived service watches the mirror's sync marker, not individual records.
Only reads ctx; never publishes Pebble state, invokes Caco, or changes requests.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import math
import os
from pathlib import Path
import signal
import sys
import time

from context_snapshot import ContextError, context_text, save
from jev_router import ACTIVE_STATES, MAX_SNAPSHOT_BYTES, RouterError, clipped, validate_snapshot
from process_io import ProcessError, run

SECTIONS = ('projects', 'agents', 'nodes', 'messages')
MAX_READ_BYTES = 4 * 1024 * 1024


def timestamp(value):
    try:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError()
            value = parsed.timestamp()
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError()
        return float(value)
    except (TypeError, ValueError, OverflowError):
        raise ContextError('invalid_ctx_timestamp') from None


def project(data, *, now=None, max_age=900, messages_limit=40, beads_limit=3):
    """Pure mapping. Freshness comes from observations, never projection time."""
    now = time.time() if now is None else now
    if not 1 <= messages_limit <= 100 or not 1 <= beads_limit <= 10:
        raise ContextError('invalid_context_limit')
    if not isinstance(data, dict) or not data.get('found') or data.get('omitted') != 0:
        raise ContextError('ctx_inventory_missing_or_omitted')
    entries = data.get('entries')
    if not isinstance(entries, list) or data.get('count') != len(entries):
        raise ContextError('invalid_ctx_entries')
    inventory = {name: [] for name in SECTIONS}
    sync = None
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get('key'), str):
            raise ContextError('invalid_ctx_entry')
        key, value = entry['key'], entry.get('value')
        if key in seen:
            raise ContextError('duplicate_ctx_key')
        seen.add(key)
        if key == 'cacophony.sync':
            sync = value
        else:
            parts = key.split('.')
            if len(parts) >= 3 and parts[0] == 'cacophony' and parts[1] in inventory:
                expected = 4 if parts[1] == 'agents' else 3
                if len(parts) == expected:
                    if not isinstance(value, dict):
                        raise ContextError('invalid_ctx_record')
                    inventory[parts[1]].append(value)
    if not isinstance(sync, dict) or sync.get('status') != 'ok' or sync.get('errors'):
        raise ContextError('ctx_sync_unavailable')
    sync_age = now - timestamp(sync.get('observed_at'))
    if sync_age < -60 or sync_age > max_age:
        raise ContextError('ctx_sync_stale')
    sections = sync.get('sections')
    if not isinstance(sections, dict):
        raise ContextError('ctx_sections_missing')
    observed = {}
    for name in SECTIONS:
        section = sections.get(name)
        if not isinstance(section, dict) or section.get('count') != len(inventory[name]):
            raise ContextError('ctx_section_incomplete_' + name)
        observed[name] = timestamp(section.get('observed_at'))
        age = now - observed[name]
        if age < -60 or age > max_age:
            raise ContextError('ctx_section_stale_' + name)

    projects = []
    project_ids = set()
    for row in inventory['projects']:
        # Escaped ctx key segments are storage identities, not project names.
        name = row.get('name')
        if not isinstance(name, str) or not name or len(name) > 200 or name in project_ids:
            raise ContextError('invalid_project_name')
        project_ids.add(name)
        beads = row.get('recent_beads', [])
        if not isinstance(beads, list) or any(not isinstance(bead, dict) for bead in beads):
            raise ContextError('invalid_ctx_beads')
        projects.append({
            'id': name, 'name': name, 'description': context_text(row.get('description'), 600),
            'description_at': timestamp(row['description_checked_at']) if row.get('description_checked_at') else observed['projects'],
            'beads_at': observed['projects'], 'errors': [],
            'beads': [{key: bead.get(key) for key in ('id', 'status', 'priority', 'assignee', 'updated_at')}
                      | {'title': context_text(bead.get('title'), 200), 'description': context_text(bead.get('description'), 240)}
                      for bead in beads[:beads_limit]],
        })
    agents, targets = [], set()
    for row in inventory['agents']:
        if row.get('state') not in ACTIVE_STATES:
            continue
        node, name, ident = row.get('node'), row.get('project'), row.get('id')
        if any(not isinstance(part, str) or not part or ':' in part for part in (node, name, ident)):
            raise ContextError('invalid_agent_identity')
        target = f'{node}:{name}:{ident}'
        if row.get('target') != target or target in targets:
            raise ContextError('invalid_ctx_agent_target')
        targets.add(target)
        if not isinstance(row.get('beads', []), list):
            raise ContextError('invalid_ctx_agent_beads')
        agents.append({'target': target, 'node': node, 'project': name,
                       'name': clipped(row.get('name') or ident, 160), 'state': row['state'],
                       'goal': context_text(row.get('goal'), 500), 'beads': row.get('beads') or [],
                       'inventory_source': clipped(row.get('inventory_source'), 120)})
    messages = sorted(inventory['messages'], key=lambda row: (timestamp(row.get('ts')), str(row.get('id', ''))), reverse=True)
    snapshot = {
        'version': 1, 'created_at': min(observed.values()),
        'projects': sorted(projects, key=lambda row: row['id']),
        'agents': sorted(agents, key=lambda row: row['target']),
        'nodes': sorted([{key: row.get(key) for key in ('name', 'liveness', 'last_seen')}
                         for row in inventory['nodes']], key=lambda row: str(row['name'])),
        'messages': [{key: row.get(key) for key in ('id', 'project', 'sender', 'target', 'kind', 'ts', 'sender_kind')}
                     | {'body': context_text(row.get('body'), 500),
                        'body_truncated': bool(row.get('body_truncated')) or len(str(row.get('body') or '')) > 500}
                     for row in messages[:messages_limit]],
    }
    validate_snapshot(snapshot, now=now, max_age=max_age)
    return snapshot


class CtxReads:
    def __init__(self, binary='ctx', config=None, *, runner=run, env=None, timeout=30):
        self.command = [binary, *(['--config', str(Path(config).expanduser())] if config else []), '--json']
        self.runner, self.env, self.timeout = runner, dict(os.environ if env is None else env), timeout

    def get(self):
        result = self.runner([*self.command, 'get', 'cacophony', '--format', 'ndjson'],
                             timeout=self.timeout, max_output=MAX_READ_BYTES, env=self.env)
        try:
            envelope = json.loads(result.stdout)
            data = envelope['data']
            if envelope['status'] != 'success' or type(data['head_rev']) is not int or data['head_rev'] < 0:
                raise ValueError()
            return data
        except (ValueError, TypeError, KeyError):
            raise ContextError('invalid_ctx_response') from None

    def wait(self, since, watch_timeout=120):
        # Native ctx owns transport/auth/reconnect. --once lets the next full
        # read coalesce a burst, then subscribe after that read's global revision.
        # Periodic stream renewal also re-reads credentials and handles DB resets;
        # it is not a second upstream inventory poller or a systemd cron.
        self.runner([*self.command, 'watch', 'cacophony.sync', '--since', str(since),
                     '--once', '--timeout', f'{watch_timeout}s'],
                    timeout=watch_timeout + 5, max_output=65536, env=self.env)


def refresh(data, output, **options):
    snapshot = project(data, **options)
    payload = json.dumps(snapshot, ensure_ascii=False, separators=(',', ':')).encode()
    path = Path(output).expanduser()
    try:
        with path.open('rb') as previous:
            unchanged = previous.read(MAX_SNAPSHOT_BYTES + 1) == payload
    except FileNotFoundError:
        unchanged = False
    if not unchanged:
        save(snapshot, output)
    return {'ok': True, 'source': 'ctx', 'head_rev': data['head_rev'], 'created_at': snapshot['created_at'],
            'projects': len(snapshot['projects']), 'agents': len(snapshot['agents']),
            'messages': len(snapshot['messages']), 'written': not unchanged}


def report_error(error):
    code = str(error) if isinstance(error, (ContextError, RouterError)) else error.code if isinstance(error, ProcessError) else type(error).__name__
    result = {'ok': False, 'source': 'ctx', 'error': code}
    if isinstance(error, (ContextError, RouterError, ProcessError)):
        result['previous_snapshot_preserved'] = True
    else:
        # A filesystem error can occur after atomic replacement (e.g. fsync).
        # Do not claim the old file survived an unconfirmed write outcome.
        result['cache_write_unconfirmed'] = True
    print(json.dumps(result), file=sys.stderr, flush=True)


def follow(client, output, *, watch_timeout=120, retry_secs=5, sleep=time.sleep, **options):
    while True:
        try:
            data = client.get()
        except Exception as error:
            report_error(error)
            sleep(retry_secs)
            continue
        try:
            print(json.dumps(refresh(data, output, **options)), flush=True)
        except Exception as error:
            report_error(error)
        try:
            client.wait(data['head_rev'], watch_timeout)
        except ProcessError as error:
            if error.exit_code == 7:  # ctx watch's normal lease expiry
                continue
            report_error(error)
            sleep(retry_secs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path.home() / '.local/state/pebble/context.json')
    parser.add_argument('--ctx', default='ctx')
    parser.add_argument('--ctx-config')
    parser.add_argument('--messages', type=int, default=40)
    parser.add_argument('--beads', type=int, default=3)
    parser.add_argument('--max-age', type=int, default=900)
    parser.add_argument('--watch', action='store_true')
    parser.add_argument('--watch-timeout', type=int, default=120)
    args = parser.parse_args()
    if not 1 <= args.watch_timeout <= 590 or args.max_age < 1:
        parser.error('watch timeout must be 1..590 seconds and max age must be positive')
    client = CtxReads(args.ctx, args.ctx_config)
    options = {'max_age': args.max_age, 'messages_limit': args.messages, 'beads_limit': args.beads}
    def stopped(*_): raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, stopped)
    try:
        if args.watch:
            follow(client, args.output, watch_timeout=args.watch_timeout, **options)
        else:
            print(json.dumps(refresh(client.get(), args.output, **options)))
    except KeyboardInterrupt:
        # process_io.run's finally block terminates/joins the active ctx child.
        return 0
    except Exception as error:
        report_error(error)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
