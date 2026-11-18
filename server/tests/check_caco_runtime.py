#!/usr/bin/env python3
"""Execute the evaluated Nix selector against disposable binaries, never live Caco."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

BASH = shutil.which('bash')
cases = json.loads(Path(sys.argv[1]).read_text())
with tempfile.TemporaryDirectory(prefix='pebble-caco-selector-') as directory:
    root = Path(directory)

    def binary(relative):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('#!/bin/sh\nexit 0\n')
        path.chmod(0o700)
        return path

    system = binary('system/bin/caco')
    cargo = binary('home/.cargo/bin/caco')
    override = binary('override/tool')
    managed = binary('managed path/custom-caco')
    named = binary('system/bin/custom-caco')
    home_override = binary('home/bin/custom-caco')

    def run(name='system', **overrides):
        script = cases[name].replace('/fixture', directory) + '\nprintf "%s" "$PEBBLE_CACO_BIN"\n'
        env = dict(os.environ, HOME=str(root / 'home'), PATH=str(system.parent))
        env.pop('CACO_BIN', None)
        env.update(overrides)
        return subprocess.run([BASH, '-c', script], capture_output=True, text=True,
                              timeout=3, env=env)

    assert run().stdout == str(system), 'trust system PATH, not the Cargo install'
    assert run(CACO_BIN='').stdout == str(system)
    assert run(CACO_BIN=str(managed)).stdout == str(managed), 'spaces stay one executable'
    assert run(CACO_BIN='custom-caco').stdout == str(named)
    assert run(CACO_BIN=str(root / 'missing')).returncode == 78, 'no fallback from explicit path'
    assert run(CACO_BIN='missing-name').returncode == 78, 'no fallback from explicit command'
    assert run(CACO_BIN=str(root)).returncode == 78, 'directory is not an executable file'
    assert run(PATH=str(cargo.parent)).stdout == str(cargo), 'Cargo also works when PATH selects it'
    assert run(PATH=str(root / 'empty')).returncode == 78, 'do not search home directories'
    assert run('operatorOverride', CACO_BIN=str(managed)).stdout == str(override)
    assert run('namedOverride', CACO_BIN=str(managed)).stdout == str(named)
    assert run('homeOverride').stdout == str(home_override)
    override.unlink()
    assert run('operatorOverride', CACO_BIN=str(managed)).returncode == 78

    # Custom handlers invoking bare caco must still reach the already-resolved
    # executable, including a differently named path, without shell evaluation.
    managed.write_text('#!' + sys.executable + '\nimport json,sys\n'
                       'print(json.dumps({"args":sys.argv[1:],"stdin":sys.stdin.read()}))\n')
    relay = Path(__file__).resolve().parent.parent / 'caco-relay.sh'
    env = dict(os.environ, PEBBLE_CACO_BIN=str(managed))
    args = ['literal;$(not-a-command)', 'two words']
    result = subprocess.run([BASH, str(relay), *args], input='payload $(not-a-command)\n',
                            text=True, capture_output=True, env=env, check=True, timeout=3)
    assert json.loads(result.stdout) == {'args': args, 'stdin': 'payload $(not-a-command)\n'}
    env.pop('PEBBLE_CACO_BIN')
    assert subprocess.run([BASH, str(relay)], capture_output=True, env=env, timeout=3).returncode == 78
print('15 system Caco selector/relay assertions passed; no live Caco commands invoked.')
