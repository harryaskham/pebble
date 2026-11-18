#!/usr/bin/env python3
"""Refresh Pebble's shared routing context through bounded first-party reads.

Run this outside the webhook path (normally every five minutes). Core read
failure preserves the previous file and its age; this never consumes an inbox.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

from jev_router import MAX_SNAPSHOT_BYTES, ACTIVE_STATES, clipped, validate_snapshot
from process_io import ProcessError, run


class ContextError(Exception):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class CacoReads:
    def __init__(self, base, token_file=None, *, timeout=8, env=None):
        self.env = os.environ if env is None else env
        parsed = urllib.parse.urlsplit(base)
        if parsed.scheme not in ('http', 'https') or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ContextError('invalid_caco_url')
        self.base, self.timeout = base.rstrip('/'), timeout
        self.token_file = token_file
        self.opener = urllib.request.build_opener(NoRedirect())

    def get(self, endpoint):
        # Re-read a managed credential target on each refresh request. No token
        # is sent to a redirect, passed on argv, included in context or logged.
        token = None if self.token_file else self.env.get('CACO_AGENT_TOKEN')
        if not token:
            filename = self.token_file or self.env.get('CACO_AGENT_TOKEN_FILE')
            if not filename:
                raise ContextError('caco_read_credentials_required')
            with Path(filename).expanduser().open('rb') as stream:
                raw = stream.read(16385)
            if not 0 < len(raw) <= 16384:
                raise ContextError('invalid_caco_credential')
            token = raw.decode().strip()
        if not endpoint.startswith('/api/v1/') or '\n' in endpoint:
            raise ContextError('invalid_caco_read_path')
        request = urllib.request.Request(self.base + endpoint, headers={'Authorization': 'Bearer ' + token})
        try:
            deadline = time.monotonic() + self.timeout
            chunks, size = [], 0
            with self.opener.open(request, timeout=self.timeout) as response:
                while True:
                    if time.monotonic() >= deadline:
                        raise ContextError('caco_read_timeout')
                    chunk = response.read1(65536)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > 4 * 1024 * 1024:
                        raise ContextError('caco_read_too_large')
                    chunks.append(chunk)
            raw = b''.join(chunks)
        except urllib.error.HTTPError as exc:
            code = exc.code
            exc.close()
            raise ContextError(f'caco_read_http_{code}') from None
        except (OSError, TimeoutError):
            raise ContextError('caco_read_unavailable') from None
        if len(raw) > 4 * 1024 * 1024:
            raise ContextError('caco_read_too_large')
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeError):
            raise ContextError('invalid_caco_response') from None
        if isinstance(value, dict) and value.get('ok') is False:
            raise ContextError('caco_read_failed')
        return value.get('data', value) if isinstance(value, dict) else value


def context_text(value, limit):
    text = str(value or '')
    text = re.sub(r'(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{12,}', 'Bearer [redacted]', text)
    text = re.sub(r'\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|sk-[A-Za-z0-9_-]{20,})\b', '[redacted]', text)
    text = re.sub(r'''(?i)(\b(?:api[_-]?key|access[_-]?token|authorization|password|client[_-]?secret)\b["']?\s*[:=]\s*["']?)([^\s"',;}]{8,})''', r'\1[redacted]', text)
    return text[:limit]


def rows(value, key):
    result = value.get(key) if isinstance(value, dict) else value
    if not isinstance(result, list) or any(not isinstance(row, dict) for row in result):
        raise ContextError('invalid_' + key + '_inventory')
    return result


def github_hosts():
    hosts = os.environ.get('PEBBLE_GITHUB_HOSTS', 'github.com').split(',')
    return {host.strip().lower() for host in hosts if re.fullmatch(r'[A-Za-z0-9.-]{1,253}', host.strip())}


def repository(remote):
    if not isinstance(remote, str):
        return None
    match = re.fullmatch(r'(?:ssh://git@|https://|git@)([A-Za-z0-9.-]+)[:/]([^/?#]+/[^/?#]+?)(?:\.git)?/?', remote)
    return match.groups() if match and match.group(1).lower() in github_hosts() else None


def repo_description(remote, *, runner=run):
    parsed = repository(remote)
    if not parsed:
        return ''
    host, repo = parsed
    # Use gh's configured account for each host. Deployments with several
    # accounts can provide GH_TOKEN or GH_CONFIG_DIR in the service environment.
    result = runner(['gh', 'api', '--hostname', host, 'repos/' + repo, '--jq', '.description // ""'],
                    timeout=8, max_output=8192, env={**os.environ, 'GH_HOST': host})
    return result.stdout.decode().strip()[:600]


def refresh(api, *, previous=None, messages_limit=40, beads_limit=3, describe=repo_description, now=None):
    if not 1 <= messages_limit <= 100 or not 1 <= beads_limit <= 10:
        raise ContextError('invalid_context_limit')
    now = time.time() if now is None else now
    previous = previous if isinstance(previous, dict) else {}
    endpoints = {
        'projects': '/api/v1/projects', 'agents': '/api/v1/agents', 'nodes': '/api/v1/nodes',
        'local_agents': '/api/v1/agents?local=true',
        'messages': f'/api/v1/messages/chat?limit={messages_limit}&body_preview=500',
    }
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {key: pool.submit(api.get, path) for key, path in endpoints.items()}
        core = {key: rows(future.result(), 'agents' if key == 'local_agents' else key)
                for key, future in futures.items()}
    # Fleet snapshots can lag a visitor registered by a separate CLI process.
    # Live local rows own their exact identity, including terminal states; merge
    # before active-state filtering so stale fleet rows cannot resurrect them.
    merged_agents = {}
    for agent in core['agents'] + core['local_agents']:
        identity = tuple(agent.get(key) for key in ('node', 'project', 'id'))
        if any(not isinstance(part, str) or not part or ':' in part for part in identity):
            raise ContextError('invalid_agent_identity')
        merged_agents[identity] = agent
    core['agents'] = list(merged_agents.values())
    if len(core['projects']) > 128 or len(core['nodes']) > 64:
        raise ContextError('context_inventory_limit')
    old_projects = {p['id']: p for p in previous.get('projects', []) if isinstance(p, dict) and 'id' in p}

    def project_context(project):
        name = project.get('name')
        if not isinstance(name, str) or not name or len(name) > 200:
            raise ContextError('invalid_project_name')
        old = old_projects.get(name, {})
        value = {'id': name, 'name': name, 'description': context_text(project.get('description'), 600),
                 'description_at': now, 'beads': [], 'beads_at': now, 'errors': []}
        if not value['description']:
            if old.get('description') and now - old.get('description_at', 0) < 86400:
                value.update(description=old['description'], description_at=old['description_at'])
            else:
                try:
                    value['description'] = context_text(describe(project.get('remote')), 600)
                except (ProcessError, OSError, ValueError):
                    value.update(description=clipped(old.get('description'), 600), description_at=old.get('description_at', 0))
                    value['errors'].append('description_unavailable')
        try:
            path = f'/api/v1/projects/{urllib.parse.quote(name, safe="")}/beads?sort=updated&limit={beads_limit}&description_preview=240'
            beads = rows(api.get(path), 'beads')[:beads_limit]
            value['beads'] = [{key: bead.get(key) for key in ('id', 'status', 'priority', 'assignee', 'updated_at')}
                              | {'title': context_text(bead.get('title'), 200),
                                 'description': context_text(bead.get('description'), 240)} for bead in beads]
        except (ContextError, OSError, ValueError):
            value.update(beads=old.get('beads', []), beads_at=old.get('beads_at', 0))
            value['errors'].append('recent_beads_unavailable')
        return value

    with ThreadPoolExecutor(max_workers=4) as pool:
        projects = list(pool.map(project_context, core['projects']))
    agents = []
    for agent in core['agents']:
        if agent.get('state') not in ACTIVE_STATES:
            continue
        node, project, identifier = agent.get('node'), agent.get('project'), agent.get('id')
        if any(not isinstance(p, str) or not p or ':' in p for p in (node, project, identifier)):
            raise ContextError('invalid_agent_identity')
        agents.append({'target': f'{node}:{project}:{identifier}', 'node': node, 'project': project,
                       'name': clipped(agent.get('agent_name') or agent.get('short_name') or identifier, 160),
                       'state': agent['state'], 'goal': context_text(agent.get('goal'), 500),
                       'beads': agent.get('assigned_bead_ids') or ([agent['bead_id']] if agent.get('bead_id') else []),
                       'inventory_source': clipped(agent.get('inventory_source'), 120)})
    snapshot = {
        'version': 1, 'created_at': now,
        'projects': projects, 'agents': agents,
        'nodes': [{key: node.get(key) for key in ('name', 'liveness', 'last_seen')} for node in core['nodes']],
        'messages': [{key: message.get(key) for key in ('id', 'project', 'sender', 'target', 'kind', 'ts', 'sender_kind')}
                     | {'body': context_text(message.get('body'), 500), 'body_truncated': message.get('body_truncated', False) or len(str(message.get('body') or '')) > 500}
                     for message in core['messages'][:messages_limit]],
    }
    validate_snapshot(snapshot, now=now)
    return snapshot


def save(snapshot, filename):
    # Preserve operator-managed symlink indirection instead of replacing it.
    path = Path(filename).expanduser().resolve()
    payload = json.dumps(snapshot, ensure_ascii=False, separators=(',', ':')).encode()
    if len(payload) > MAX_SNAPSHOT_BYTES:
        raise ContextError('context_too_large')
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.pebble-context-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path.home() / '.local/state/pebble/context.json')
    parser.add_argument('--api', default=os.environ.get('CACO_DAEMON_URL', 'http://127.0.0.1:11100'))
    parser.add_argument('--token-file', default=os.environ.get('PEBBLE_CACO_TOKEN_FILE'))
    parser.add_argument('--messages', type=int, default=40)
    parser.add_argument('--beads', type=int, default=3)
    parser.add_argument('--no-repo-descriptions', action='store_true')
    args = parser.parse_args()
    try:
        previous = None
        if args.output.exists():
            with args.output.open('rb') as stream:
                previous = json.loads(stream.read(MAX_SNAPSHOT_BYTES + 1))
        api = CacoReads(args.api, args.token_file)
        snapshot = refresh(api, previous=previous, messages_limit=args.messages, beads_limit=args.beads,
                           describe=(lambda remote: '') if args.no_repo_descriptions else repo_description)
        save(snapshot, args.output)
        print(json.dumps({'ok': True, 'created_at': snapshot['created_at'],
                          'projects': len(snapshot['projects']), 'agents': len(snapshot['agents']),
                          'messages': len(snapshot['messages']), 'file': str(args.output)}))
    except Exception as exc:
        # Preserve old snapshot and freshness; never log API bodies/credentials.
        print(json.dumps({'ok': False, 'error': str(exc) if isinstance(exc, ContextError) else type(exc).__name__,
                          'previous_snapshot_preserved': True}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
