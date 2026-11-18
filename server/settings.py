"""Versioned Pebble configuration normalization.

Version 1 is the original single-node shape. Version 2 is the public, provider-neutral
shape. Old stored request snapshots are upgraded on read, so a deployment can move
without rewriting its archive.
"""
from __future__ import annotations

import copy
import re

ROUTE_PATH = re.compile(r'/(?:[A-Za-z0-9._~-]+(?:/[A-Za-z0-9._~-]+)*)?')
ACTION = re.compile(r'(?:route|pebble|bd-file|bd-expand|agent-msg|broadcast|custom:[a-z][a-z0-9-]{0,47})')
PROFILE = re.compile(r'[a-z][a-z0-9-]{0,47}')
ENV_NAME = re.compile(r'[A-Z_][A-Z0-9_]{0,127}')
LEGACY_ROUTES = {
    '/': 'route',
    '/hooks/global/pebble': 'pebble',
    '/hooks/global/pebble-audio': 'route',
    '/hooks/global/pebble-route': 'route',
    '/hooks/global/pebble-bd-file': 'bd-file',
    '/hooks/global/pebble-bd-expand': 'bd-expand',
    '/hooks/global/pebble-agent-msg': 'agent-msg',
    '/hooks/global/pebble-broadcast': 'broadcast',
}
CACO_ACTIONS = {'pebble', 'bd-file', 'bd-expand', 'agent-msg', 'broadcast'}


class ConfigError(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def _string(value, *, empty=False, limit=4096):
    return isinstance(value, str) and (empty or bool(value)) and len(value) <= limit and '\0' not in value


def _argv(value, *, minimum=1, maximum=64):
    return (isinstance(value, list) and minimum <= len(value) <= maximum
            and all(_string(v, limit=8192) for v in value))


def normalize(config):
    """Return a version-2 copy. Validation of shared limits stays with the receiver."""
    if not isinstance(config, dict):
        raise ConfigError('invalid_config_fields')
    value = copy.deepcopy(config)
    if value.get('version') == 1:
        if 'mai' not in value or not isinstance(value['mai'], dict) or not isinstance(value.get('forward'), dict):
            raise ConfigError('invalid_config_fields')
        mai = value.pop('mai')
        value['transcription'] = {'provider': 'agent-utils-mai', 'model': 'mai-transcribe-2', **mai}
        value['forward'] = {**value['forward'], 'auth': 'bearer-ingress', 'token_file': '', 'receipt': 'caco-hook'}
        value['version'] = 2
    if value.get('version') != 2:
        raise ConfigError('invalid_config_fields')
    validate_transcription(value.get('transcription'))
    validate_forward(value.get('forward'))
    if 'commands' in value:
        normalize_runtime(value)
    return value


def normalize_runtime(config):
    """Normalize commands attached to an already-normalized receiver config."""
    normalize_commands(config['commands'])
    speech = config['commands'].get('speech')
    if isinstance(speech, dict) and not speech.get('agent_utils_dir'):
        speech['agent_utils_dir'] = config['transcription'].get('agent_utils_dir', '')
    return config


def validate_transcription(value):
    common = {'provider', 'model', 'timeout_secs', 'max_audio_secs', 'language'}
    if not isinstance(value, dict) or value.get('provider') not in ('agent-utils-mai', 'command'):
        raise ConfigError('invalid_transcription_settings')
    fields = common | ({'agent_utils_dir', 'endpoint_file', 'key_file', 'deployment'}
                       if value['provider'] == 'agent-utils-mai' else {'command', 'output'})
    if set(value) != fields:
        raise ConfigError('invalid_transcription_settings')
    for key, low, high in [('timeout_secs', 1, 120), ('max_audio_secs', 1, 300)]:
        if type(value[key]) is not int or not low <= value[key] <= high:
            raise ConfigError('invalid_config_limit')
    if not _string(value['model'], limit=200) or not _string(value['language'], empty=True, limit=32):
        raise ConfigError('invalid_transcription_settings')
    if value['provider'] == 'agent-utils-mai':
        if not all(_string(value[k]) for k in ('agent_utils_dir', 'endpoint_file', 'key_file', 'deployment')):
            raise ConfigError('invalid_transcription_settings')
    elif not _argv(value['command']) or value['output'] not in ('text', 'json'):
        raise ConfigError('invalid_transcription_settings')


def validate_forward(value):
    if (not isinstance(value, dict) or set(value) != {'url', 'timeout_secs', 'auth', 'token_file', 'receipt'}
            or not _string(value['url'], empty=True) or not _string(value['token_file'], empty=True)
            or value['auth'] not in ('none', 'bearer-ingress', 'bearer-file')
            or (value['auth'] == 'bearer-file') != bool(value['token_file'])
            or value['receipt'] not in ('http-2xx', 'caco-hook')
            or type(value['timeout_secs']) is not int or not 1 <= value['timeout_secs'] <= 120):
        raise ConfigError('invalid_forward_settings')


def normalize_commands(commands):
    if not isinstance(commands, dict):
        raise ConfigError('invalid_command_settings')
    caco = commands.get('caco')
    if isinstance(caco, dict) and 'enabled' not in caco:
        caco['enabled'] = True
    speech = commands.get('speech')
    if isinstance(speech, dict):
        speech.setdefault('provider', 'auto')
        speech.setdefault('command', [])
        speech.setdefault('agent_utils_dir', '')
    if 'routes' not in commands:
        commands['routes'] = {path: {'action': action, 'profile': 'default'} for path, action in LEGACY_ROUTES.items()}
    commands.setdefault('jev_profiles', {})
    validate_routes(commands)
    return commands


def validate_routes(commands):
    routes, profiles = commands.get('routes'), commands.get('jev_profiles')
    if not isinstance(routes, dict) or not 1 <= len(routes) <= 64:
        raise ConfigError('invalid_command_routes')
    if not isinstance(profiles, dict) or len(profiles) > 16 or 'default' in profiles:
        raise ConfigError('invalid_jev_profiles')
    for name, profile in profiles.items():
        fields = {'provider', 'model', 'timeout_secs', 'max_context_age_secs', 'actions', 'builtins'}
        if not isinstance(name, str) or not PROFILE.fullmatch(name) or not isinstance(profile, dict) or set(profile) - fields:
            raise ConfigError('invalid_jev_profiles')
        if 'actions' in profile and (not isinstance(profile['actions'], list) or len(profile['actions']) > 32
                or any(not isinstance(v, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,47}', v) for v in profile['actions'])
                or len(set(profile['actions'])) != len(profile['actions'])):
            raise ConfigError('invalid_jev_profile_actions')
        if 'builtins' in profile and type(profile['builtins']) is not bool:
            raise ConfigError('invalid_jev_profile_builtins')
    for path, route in routes.items():
        if (not isinstance(path, str) or len(path) > 256 or not ROUTE_PATH.fullmatch(path)
                or path.startswith('/route') or path in ('/health', '/receipts') or path.startswith('/receipts/')):
            raise ConfigError('invalid_command_route_path')
        if (not isinstance(route, dict) or set(route) != {'action', 'profile'} or not isinstance(route['action'], str)
                or not ACTION.fullmatch(route['action']) or not isinstance(route['profile'], str)
                or route['profile'] != 'default' and route['profile'] not in profiles):
            raise ConfigError('invalid_command_route')
        if route['action'] in CACO_ACTIONS and not commands.get('caco', {}).get('enabled', True):
            raise ConfigError('caco_route_requires_adapter')


def route_table(commands):
    return {path: route['action'] for path, route in commands['routes'].items()}


def route_profiles(commands):
    return {path: route['profile'] for path, route in commands['routes'].items()}


def apply_profile(commands, profile):
    """Overlay one named Jev profile without changing other frozen settings."""
    if profile == 'default':
        return commands
    profiles = commands.get('jev_profiles', {})
    if profile not in profiles:
        raise ConfigError('unknown_jev_profile')
    selected = profiles[profile]
    result = copy.deepcopy(commands)
    for key in ('provider', 'model', 'timeout_secs', 'max_context_age_secs'):
        if key in selected:
            result['jev'][key] = selected[key]
    if 'actions' in selected:
        result['actions'] = {name: spec for name, spec in result.get('actions', {}).items() if name in selected['actions']}
    if selected.get('builtins') is False:
        result['caco'] = {**result['caco'], 'enabled': False}
    return result


def secret_names(value):
    return isinstance(value, dict) and all(isinstance(k, str) and ENV_NAME.fullmatch(k) for k in value)


def refresh_transcription_runtime(config, current):
    """Replace retired executable/module paths while keeping provider choices frozen."""
    frozen, live = config['transcription'], current['transcription']
    if frozen['provider'] == live['provider'] == 'agent-utils-mai':
        frozen['agent_utils_dir'] = live['agent_utils_dir']
    elif frozen['provider'] == live['provider'] == 'command':
        frozen['command'] = live['command']
