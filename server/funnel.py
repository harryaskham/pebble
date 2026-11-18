#!/usr/bin/env python3
"""Add only configured Pebble paths to an existing Tailscale HTTPS Funnel; never reset it."""
import argparse
import copy
import json
import subprocess

PATH = '/hooks/global/pebble-audio'
COMMAND_PATHS = [PATH, '/hooks/global/pebble', '/hooks/global/pebble-route',
                 '/hooks/global/pebble-bd-file', '/hooks/global/pebble-bd-expand', '/hooks/global/pebble-agent-msg',
                 '/hooks/global/pebble-broadcast', '/pebble/action', '/hooks/global/pebble/action',
                 '/pebble/status', '/pebble/events', '/hooks/global/pebble-status', '/hooks/global/pebble-events', '/route']


def snapshot(command):
    result = subprocess.run([command, 'funnel', 'status', '--json'], capture_output=True, text=True, timeout=15, check=True)
    if len(result.stdout) > 65536:
        raise RuntimeError('funnel_status_too_large')
    return json.loads(result.stdout)


def without_owned_path(value, host, paths=None):
    value = copy.deepcopy(value)
    handlers = value.get('Web', {}).get(host, {}).get('Handlers', {})
    for path in paths or [PATH]:
        for key in (path, path + '/'):
            handlers.pop(key, None)
    return value


def proxy_target(backend, path):
    # Funnel strips the handler prefix. Restore it explicitly in the upstream
    # URL; otherwise fixed actions become '/', and status/custom suffixes 404.
    return backend.rstrip('/') + path.rstrip('/')


def owned(value, backend, path):
    # Migrate only our exact old root mapping or the exact corrected mapping.
    return value in ({'Proxy': backend}, {'Proxy': proxy_target(backend, path)})


def install(command, hostname, backend, run=subprocess.run, inspect=snapshot, *, commands=False, retire_commands=False, command_paths=None):
    command_paths = list(command_paths or COMMAND_PATHS)
    if not command_paths or len(set(command_paths)) != len(command_paths):
        raise RuntimeError('invalid_funnel_paths')
    host = hostname + ':443'
    before = inspect(command)
    if before.get('TCP', {}).get('443', {}).get('HTTPS') is not True or before.get('AllowFunnel', {}).get(host) is not True:
        raise RuntimeError('existing_https_funnel_required')
    handlers = before.get('Web', {}).get(host, {}).get('Handlers', {})
    if '/' not in handlers:
        raise RuntimeError('existing_root_handler_required')
    if commands and retire_commands:
        raise RuntimeError('conflicting_route_modes')
    retired = []
    if retire_commands:
        # Explicit rollback: remove only our exact proxy handlers. Foreign
        # handlers remain untouched; the existing root resumes the text route.
        previous = before
        for path in command_paths[1:]:
            current = inspect(command)
            if current != previous:
                raise RuntimeError('funnel_changed_during_reconciliation')
            entries = current.get('Web', {}).get(host, {}).get('Handlers', {})
            keys = [key for key in (path, path + '/') if key in entries]
            if not keys or not all(owned(entries[key], backend, path) for key in keys):
                continue
            for key in keys:
                run([command, 'funnel', '--https=443', '--set-path=' + key, 'off'],
                    capture_output=True, text=True, timeout=30, check=True)
                after = inspect(command)
                if without_owned_path(after, host, [path]) != without_owned_path(previous, host, [path]):
                    raise RuntimeError('funnel_changed_during_reconciliation')
                if key in after.get('Web', {}).get(host, {}).get('Handlers', {}):
                    raise RuntimeError('command_path_not_removed')
                previous = after
            retired.append(path)
        before = previous
        handlers = before.get('Web', {}).get(host, {}).get('Handlers', {})
    paths = command_paths if commands else [PATH]
    missing, urls = [], []
    # Preflight the entire set before changing even the first handler. An
    # unrelated/foreign handler is never overwritten, even on a Pebble path.
    for path in paths:
        keys = [key for key in (path, path + '/') if key in handlers]
        if keys and not all(owned(handlers[key], backend, path) for key in keys):
            raise RuntimeError('command_path_already_owned_by_another_backend' if commands else 'audio_path_already_owned_by_another_backend')
        for key in keys or [path]:
            if handlers.get(key) != {'Proxy': proxy_target(backend, path)}:
                missing.append((path, key))
    previous = before
    for path, key in missing:
        if inspect(command) != previous:
            raise RuntimeError('funnel_changed_during_reconciliation')
        run([command, 'funnel', '--bg', '--https=443', '--set-path=' + key, '--yes', proxy_target(backend, path)],
            capture_output=True, text=True, timeout=30, check=True)
        after = inspect(command)
        if without_owned_path(after, host, [path]) != without_owned_path(previous, host, [path]):
            raise RuntimeError('funnel_changed_outside_audio_path_inspect_before_retry')
        handlers = after.get('Web', {}).get(host, {}).get('Handlers', {})
        if handlers.get(key) != {'Proxy': proxy_target(backend, path)}:
            raise RuntimeError('audio_path_not_installed')
        previous = after
    handlers = previous.get('Web', {}).get(host, {}).get('Handlers', {})
    for path in paths:
        key = next(key for key in (path, path + '/') if handlers.get(key) == {'Proxy': proxy_target(backend, path)})
        urls.append('https://' + hostname + key)
    result = {'configured': True, 'changed': bool(missing or retired), 'url': urls[0]}
    if commands:
        result['urls'] = urls
    if retire_commands:
        result['retired_paths'] = retired
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tailscale', default='tailscale')
    parser.add_argument('--hostname', required=True)
    parser.add_argument('--backend', default='http://127.0.0.1:11301')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--path', action='append', dest='paths', help='Command-mode path; repeat. The first must be the audio path.')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--commands', action='store_true', help='Include all shared command paths (requires the shared receiver; recipient delivery is independently reported)')
    modes.add_argument('--retire-commands', action='store_true', help='Roll back only our extra command paths; preserve audio, root and foreign handlers')
    args = parser.parse_args()
    if not args.apply:
        paths = args.paths or COMMAND_PATHS
        print(json.dumps({'planned_paths': paths if args.commands else [PATH], 'backend': args.backend,
                          'retire_owned_paths': paths[1:] if args.retire_commands else [],
                          'requires': 'existing HTTPS Funnel on 443; use --apply'}))
        return
    print(json.dumps(install(args.tailscale, args.hostname, args.backend, commands=args.commands,
                             retire_commands=args.retire_commands, command_paths=args.paths)))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Provider/process output may include unrelated configuration: do not log it.
        print(json.dumps({'error': str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__}))
        raise SystemExit(1)
