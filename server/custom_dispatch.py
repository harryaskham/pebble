"""Execute only the operator-authored recipe; payload never becomes shell code."""
import os
from pathlib import Path

import action_registry
from dispatch import NotStarted
from process_io import run


def execute(recipe, *, runner=run, env=None):
    if recipe.get('dry_run', False) is not False:
        raise NotStarted('dry_run_execution_forbidden')
    action = recipe.get('action')
    if not action_registry.custom(action):
        raise NotStarted('not_a_custom_action')
    name = action.removeprefix('custom:')
    spec = action_registry.validate({name: recipe['custom_spec']})[name]
    if not spec['enabled']:
        raise NotStarted('custom_action_disabled')
    cmd = spec['command']
    # Strings are fixed trusted shell programs, not templates. Shell expansion
    # occurs only in operator-supplied code; untrusted payload is stdin only.
    argv = ['bash', '-euo', 'pipefail', '-c', cmd] if isinstance(cmd, str) else [str(Path(cmd[0]).expanduser()), *cmd[1:]]
    environment = dict(os.environ if env is None else env)
    environment.update(PEBBLE_ACTION=name, PEBBLE_INPUT_ID=recipe['input_id'], PEBBLE_REQUEST_ID=recipe['request_id'])
    output = runner(argv, recipe['text'].encode(), timeout=spec['timeout_secs'], max_output=65536, env=environment)
    # Do not echo/store arbitrary handler output (may include credentials).
    # Exit zero means the command completed, not that downstream work finished.
    return {'state': 'completed', 'action': action, 'exit_code': 0, 'elapsed_ms': output.elapsed_ms}
