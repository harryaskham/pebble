"""Operator-owned custom actions. Only descriptions enter Jev, never commands."""
import copy
import json
import re
import os
import stat
from pathlib import Path

BUILTINS = {'route', 'pebble', 'bd-file', 'bd-expand', 'agent-msg', 'broadcast'}
PREFIXES = ('/pebble/action/', '/hooks/global/pebble/action/')
MAX_BYTES = 65536


class RegistryError(Exception):
    pass


def custom(name):
    return isinstance(name, str) and name.startswith('custom:')


def validate(actions):
    if not isinstance(actions, dict) or len(actions) > 32:
        raise RegistryError('invalid_custom_actions')
    normalized = {}
    for name, spec in actions.items():
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,47}', name) or name in BUILTINS:
            raise RegistryError('invalid_custom_action_name')
        if not isinstance(spec, dict) or not {'description', 'command'} <= set(spec) or set(spec) - {'description', 'command', 'extract', 'timeout_secs', 'lane', 'enabled'}:
            raise RegistryError('invalid_custom_action_fields')
        enabled = spec.get('enabled', True)
        if type(enabled) is not bool:
            raise RegistryError('invalid_custom_action_enabled')
        for field, maximum in [('description', 600), ('extract', 1500), ('lane', 120)]:
            value = spec.get(field, '' if field != 'description' else None)
            if not isinstance(value, str) or '\0' in value or len(value.encode()) > maximum or (field == 'description' and not value.strip()):
                raise RegistryError('invalid_custom_action_' + field)
        cmd = spec['command']
        # A string is explicitly operator-authored shell code, never model text.
        if isinstance(cmd, str):
            if not cmd.strip() or '\0' in cmd or len(cmd.encode()) > 8192:
                raise RegistryError('invalid_custom_command')
        elif not isinstance(cmd, list) or not 1 <= len(cmd) <= 64 or any(not isinstance(v, str) or '\0' in v or len(v.encode()) > 8192 for v in cmd) or not cmd[0]:
            raise RegistryError('invalid_custom_command')
        timeout = spec.get('timeout_secs', 60)
        if type(timeout) is not int or not 1 <= timeout <= 300:
            raise RegistryError('invalid_custom_timeout')
        normalized[name] = {**copy.deepcopy(spec), 'enabled': enabled, 'timeout_secs': timeout,
                            'extract': spec.get('extract', ''), 'lane': spec.get('lane') or name}
    if len(json.dumps(normalized).encode()) > MAX_BYTES:
        raise RegistryError('custom_actions_too_large')
    return normalized


def parse(raw):
    # YAML is parsed only when an external manifest is configured. JSON (a YAML
    # subset) also works without PyYAML for simple source-only tooling.
    if len(raw) > MAX_BYTES:
        raise RegistryError('custom_actions_too_large')
    try:
        import yaml
        class Loader(yaml.SafeLoader):
            pass
        def mapping(loader, node, deep=False):
            result = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node, deep=deep)
                if not isinstance(key, str) or key in result:
                    raise RegistryError('duplicate_or_invalid_yaml_key')
                result[key] = loader.construct_object(value_node, deep=deep)
            return result
        Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
        # Reject aliases, including cyclic/expanding alias graphs, and multiple docs.
        if any(isinstance(event, yaml.events.AliasEvent) for event in yaml.parse(raw)):
            raise RegistryError('yaml_aliases_not_supported')
        value = yaml.load(raw, Loader=Loader)
    except ImportError:
        def unique(pairs):
            d = {}
            for k, v in pairs:
                if k in d: raise RegistryError('duplicate_or_invalid_yaml_key')
                d[k] = v
            return d
        try: value = json.loads(raw, object_pairs_hook=unique)
        except ValueError: raise RegistryError('yaml_dependency_unavailable') from None
    except RegistryError:
        raise
    except Exception:
        raise RegistryError('invalid_action_manifest') from None
    if not isinstance(value, dict) or set(value) != {'actions'}:
        raise RegistryError('invalid_action_manifest')
    return validate(value['actions'])


def catalog(settings):
    actions = validate(settings.get('actions', {}))
    filename = settings.get('actions_file', '')
    if filename:
        try:
            fd = os.open(Path(filename).expanduser(), os.O_RDONLY | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise RegistryError('action_manifest_not_regular')
                external = parse(stream.read(MAX_BYTES + 1))
        except OSError:
            raise RegistryError('action_manifest_unavailable') from None
        if set(actions) & set(external):
            raise RegistryError('duplicate_custom_action')
        actions.update(external)
    return {k: v for k, v in validate(actions).items() if v['enabled']}


def routes(actions):
    return {prefix + name: 'custom:' + name for name in actions for prefix in PREFIXES}


def describe(actions):
    return {'custom:' + k: v['description'] for k, v in actions.items()}


def spec_for(action, actions):
    name = action.removeprefix('custom:')
    if not custom(action) or name not in actions or not actions[name].get('enabled', True):
        raise RegistryError('unknown_custom_action')
    return actions[name]


def main(argv=None):
    """pebble-actions-check FILE: validate a manifest with the receiver's parser; exit 1 (and say why) when it would
    make new requests fail closed."""
    import sys
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1 or not argv[0]:
        print('usage: pebble-actions-check FILE', file=sys.stderr)
        return 2
    try:
        actions = catalog({'actions': {}, 'actions_file': argv[0]})
    except RegistryError as exc:
        print(f'invalid: {exc}', file=sys.stderr)
        return 1
    for name in sorted(actions):
        print(f"{name}: lane {actions[name]['lane']}, timeout {actions[name]['timeout_secs']}s -> /pebble/action/{name}")
    print(f'ok: {len(actions)} enabled action(s)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
