#!/usr/bin/env python3
"""Load scoped provider credentials at service startup, never into its archive."""
import argparse
import json
import os
from pathlib import Path
import stat
import sys

import re

NAME = re.compile(r'[A-Z_][A-Z0-9_]{0,127}')
RESERVED = {'HOME', 'PATH', 'PWD', 'SHELL', 'USER', 'LOGNAME'}
SECRET_NAME = re.compile(r'(?:^|_)(?:API_?KEY|KEY|TOKEN|SECRET|PASSWORD|PASS|CREDENTIALS?)(?:_|$)')


class EnvironmentError(Exception):
    pass


def load(filename, inherited=None):
    try:
        with Path(filename).expanduser().open('rb') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError()
        config = json.loads(raw)
        if set(config) != {'environment', 'secret_files'}:
            raise ValueError()
        if not isinstance(config['environment'], dict) or not isinstance(config['secret_files'], dict):
            raise ValueError()
        names = set(config['environment']) | set(config['secret_files'])
        if (len(names) > 128 or set(config['environment']) & set(config['secret_files'])
                or any(not NAME.fullmatch(name) or name in RESERVED for name in names)
                or any(SECRET_NAME.search(name) for name in config['environment'])):
            raise ValueError()
        if any(not isinstance(v, str) or not v or '\0' in v or len(v) > 4096
               for section in config.values() for v in section.values()):
            raise ValueError()
    except (OSError, ValueError, TypeError, UnicodeError):
        raise EnvironmentError('invalid_provider_environment_manifest') from None
    result = dict(os.environ if inherited is None else inherited)
    result.update(config['environment'])
    if result.get('AZURE_SPEECH_AUTH') == 'entra':
        # Provider keys are disabled. Do not carry stale shell/service keys into
        # the shared helpers; az-token obtains a fresh Entra credential on use.
        result.pop('AZURE_SPEECH_API_KEY', None)
        result.pop('AZURE_SPEECH_KEY', None)
    for name, filename in config['secret_files'].items():
        try:
            fd = os.open(Path(filename).expanduser(), os.O_RDONLY | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise ValueError()
                raw = stream.read(16385).strip()
            if not raw or len(raw) > 16384 or b'\0' in raw or b'\n' in raw or b'\r' in raw:
                raise ValueError()
            result[name] = raw.decode('utf-8')
        except (OSError, ValueError, UnicodeError):
            # Name only: never include secret contents or provider responses.
            raise EnvironmentError('provider_secret_unavailable:' + name) from None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--check', action='store_true', help='report presence only, without values or executing a child')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        env = load(args.config)
        if args.check:
            with Path(args.config).expanduser().open('rb') as stream:
                configured = json.load(stream)
            names = set(configured['environment']) | set(configured['secret_files'])
            present = {name: bool(env.get(name)) for name in sorted(names)}
            ready = all(present.values())
            print(json.dumps({'ready': ready, 'present': present}))
            return 0 if ready else 78
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        if not command:
            raise EnvironmentError('provider_command_required')
        os.execvpe(command[0], command, env)
    except EnvironmentError as exc:
        print('pebble: ' + str(exc), file=sys.stderr)
        return 78
    except OSError:
        print('pebble: provider_command_unavailable', file=sys.stderr)
        return 78


if __name__ == '__main__':
    sys.exit(main())
