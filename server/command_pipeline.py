"""Single-owner command stages, composing the existing MAI, Jev and Caco tools."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from urllib.parse import urlsplit

import dispatch
import action_registry
import custom_dispatch
from feedback import Feedback, Voice
from intake import CommandArchive, IntakeError
import jev_router
import payload_extractor
import triage_conversation
from process_io import ProcessError
import settings as settings_schema

ROUTES = settings_schema.LEGACY_ROUTES


def validate_settings(settings):
    fields = {'enabled', 'context_file', 'legacy_text_archive', 'pebble_project', 'pebble_target', 'jev', 'caco', 'speech'}
    optional = {'extractor', 'status', 'actions', 'actions_file', 'stage_workers', 'project_aliases', 'routes', 'jev_profiles'}
    if not isinstance(settings, dict) or not fields <= set(settings) or set(settings) - (fields | optional):
        raise IntakeError('invalid_command_settings')
    try:
        settings_schema.normalize_commands(settings)
    except settings_schema.ConfigError as exc:
        raise IntakeError(exc.code) from exc
    try:
        jev_router.validate_aliases(settings.get('project_aliases', {}))
    except jev_router.RouterError as exc:
        raise IntakeError(str(exc)) from exc
    workers = settings.get('stage_workers', 4)
    if type(workers) is not int or not 1 <= workers <= 8:
        raise IntakeError('invalid_stage_workers')
    if 'status' in settings:
        status = settings['status']
        if (not isinstance(status, dict) or set(status) != {'attachment_file'}
                or not isinstance(status['attachment_file'], str) or len(status['attachment_file']) > 4096 or '\0' in status['attachment_file']):
            raise IntakeError('invalid_status_settings')
    action_registry.validate(settings.get('actions', {}))
    filename = settings.get('actions_file', '')
    if not isinstance(filename, str) or len(filename) > 4096 or '\0' in filename:
        raise IntakeError('invalid_actions_file')
    if 'extractor' in settings:
        extraction = settings['extractor']
        if (not isinstance(extraction, dict) or set(extraction) != {'enabled', 'model', 'base_url', 'timeout_secs', 'max_output_tokens'}
                or type(extraction['enabled']) is not bool
                or not isinstance(extraction['model'], str) or not 1 <= len(extraction['model']) <= 200
                or '\0' in extraction['model']
                or not isinstance(extraction['base_url'], str) or len(extraction['base_url']) > 4096
                or '\0' in extraction['base_url']
                or type(extraction['timeout_secs']) is not int or not 1 <= extraction['timeout_secs'] <= 120
                or type(extraction['max_output_tokens']) is not int or not 128 <= extraction['max_output_tokens'] <= 8192):
            raise IntakeError('invalid_extractor_settings')
        if extraction['base_url']:
            base = urlsplit(extraction['base_url'])
            if base.scheme not in ('http', 'https') or not base.netloc or base.username or base.password or base.query or base.fragment:
                raise IntakeError('invalid_extractor_base_url')
    for name, keys in {
        'jev': {'binary', 'provider', 'model', 'timeout_secs', 'max_context_age_secs'},
        'caco': {'enabled', 'binary', 'daemon_url', 'token_file', 'timeout_secs'},
        'speech': {'enabled', 'provider', 'command', 'agent_utils_dir', 'aztts', 'player', 'cache_dir', 'timeout_secs', 'pulse_server', 'voices'},
    }.items():
        if not isinstance(settings[name], dict) or set(settings[name]) != keys:
            raise IntakeError('invalid_command_' + name)
    caco_enabled = settings['caco']['enabled']
    if type(settings['enabled']) is not bool or type(settings['speech']['enabled']) is not bool or type(caco_enabled) is not bool:
        raise IntakeError('invalid_command_enable')
    if caco_enabled:
        dispatch.target_parts(settings['pebble_target'])
        if settings['pebble_project'] != settings['pebble_target'].split(':')[1]:
            raise IntakeError('invalid_pebble_target_project')
    elif settings['pebble_project'] or settings['pebble_target']:
        raise IntakeError('caco_target_requires_adapter')
    for value in [settings['context_file'], settings['legacy_text_archive'], settings['jev']['binary'], settings['caco']['binary'],
                  settings['caco']['token_file'], settings['speech']['aztts'], settings['speech']['player'],
                  settings['speech']['cache_dir'], settings['speech']['pulse_server'], settings['speech']['agent_utils_dir'],
                  settings['pebble_project'], settings['pebble_target']]:
        if not isinstance(value, str) or len(value) > 4096 or '\0' in value:
            raise IntakeError('invalid_command_string')
    required = [settings['jev']['binary'], settings['speech']['aztts'], settings['speech']['player'], settings['speech']['cache_dir']]
    if caco_enabled:
        required += [settings['context_file'], settings['caco']['binary']]
    if any(not value for value in required):
        raise IntakeError('empty_command_path')
    speech = settings['speech']
    if speech['provider'] not in ('auto', 'agent-utils-azure', 'aztts', 'command'):
        raise IntakeError('invalid_speech_provider')
    if (not isinstance(speech['command'], list) or len(speech['command']) > 64
            or any(not isinstance(v, str) or not v or len(v) > 8192 or '\0' in v for v in speech['command'])
            or speech['enabled'] and speech['provider'] == 'command' and not speech['command']):
        raise IntakeError('invalid_speech_command')
    if speech['enabled'] and speech['provider'] == 'agent-utils-azure' and not speech['agent_utils_dir']:
        raise IntakeError('speech_agent_utils_dir_required')
    if settings['jev']['provider'] not in ('openrouter', 'official', 'vercel', 'custom'):
        raise IntakeError('invalid_jev_provider')
    for profile in [settings['jev'], *settings.get('jev_profiles', {}).values()]:
        model = profile.get('model')
        if 'provider' in profile and profile['provider'] not in ('openrouter', 'official', 'vercel', 'custom'):
            raise IntakeError('invalid_jev_provider')
        if model is not None and (not isinstance(model, str) or not model or len(model) > 200 or '\0' in model):
            raise IntakeError('invalid_jev_model')
        for key, maximum in [('timeout_secs', 120), ('max_context_age_secs', 3600)]:
            if key in profile and (type(profile[key]) is not int or not 1 <= profile[key] <= maximum):
                raise IntakeError('invalid_command_timeout')
    for value, maximum in [(settings['caco']['timeout_secs'], 600), (settings['speech']['timeout_secs'], 180)]:
        if type(value) is not int or not 1 <= value <= maximum:
            raise IntakeError('invalid_command_timeout')
    if caco_enabled:
        base = urlsplit(settings['caco']['daemon_url'])
        if base.scheme not in ('http', 'https') or not base.netloc or base.username or base.password or base.query or base.fragment:
            raise IntakeError('invalid_caco_url')
    voices = settings['speech']['voices']
    expected = {f'PEBBLE_{role}_{field}' for role in ('UI', 'SELF') for field in ('VOICE', 'SPEED', 'EMBEDDING')}
    if not isinstance(voices, dict) or set(voices) != expected or any(not isinstance(value, str) for value in voices.values()):
        raise IntakeError('invalid_speech_settings')
    Voice.from_env('UI', voices)
    Voice.from_env('SELF', voices)


def safe_error(exc):
    code = getattr(exc, 'code', None)
    if code is None and isinstance(exc, (jev_router.RouterError, dispatch.DispatchError, payload_extractor.ExtractionError, action_registry.RegistryError)):
        code = str(exc)
    return code if isinstance(code, str) and re.fullmatch(r'[A-Za-z0-9_]{1,100}', code) else type(exc).__name__


class Processor:
    def __init__(self, archive, transcriber, *, planner=jev_router.plan, extractor=payload_extractor.extract, sender=None, custom_sender=custom_dispatch.execute, feedback_factory=None, resolver=triage_conversation.classify):
        self.archive, self.transcriber, self.planner = archive, transcriber, planner
        self.extractor = extractor
        self.resolver = resolver
        self.custom_sender = custom_sender
        self.sender, self.feedback_factory = sender, feedback_factory
        self._clients = threading.local()

    @staticmethod
    def caco_env(config):
        env = {**os.environ, 'CACO_DAEMON_URL': config['daemon_url']}
        if config['token_file']:
            env.pop('CACO_AGENT_TOKEN', None)
            env['CACO_AGENT_TOKEN_FILE'] = str(Path(config['token_file']).expanduser())
        return env

    @property
    def caco_clients(self):
        # Lightweight clients remain thread-local; construction does no I/O.
        if not hasattr(self._clients, 'caco'):
            self._clients.caco = {}
        return self._clients.caco

    def settings(self, record):
        frozen = settings_schema.normalize(json.loads(record['settings']))
        # Preserve intent/provider/authority choices; current binaries can replace
        # retired Nix store executables. Credentials stay external and may rotate.
        frozen['node'] = self.archive.config['node']
        frozen['ffmpeg'] = self.archive.config['ffmpeg']
        settings_schema.refresh_transcription_runtime(frozen, self.archive.config)
        profile = json.loads(record['metadata']).get('route_profile', 'default')
        frozen['commands'] = settings_schema.apply_profile(frozen['commands'], profile)
        current = self.archive.config['commands']
        for section, keys in {'jev': ['binary'], 'caco': ['binary'], 'speech': ['aztts', 'player', 'agent_utils_dir']}.items():
            for key in keys:
                frozen['commands'][section][key] = current[section][key]
        if frozen['commands']['speech']['provider'] == current['speech']['provider'] == 'command':
            frozen['commands']['speech']['command'] = current['speech']['command']
        return frozen

    def feedback(self, settings):
        if self.feedback_factory:
            return self.feedback_factory(settings)
        speech = settings['commands']['speech']
        env = {**os.environ, **speech['voices']}
        if speech['pulse_server']:
            env['PULSE_SERVER'] = speech['pulse_server']
        return Feedback(speech['cache_dir'], aztts=str(Path(speech['aztts']).expanduser()), player=str(Path(speech['player']).expanduser()),
                        node=settings['node'], agent_utils_dir=speech['agent_utils_dir'], provider=speech['provider'], command=speech['command'],
                        env=env, timeout=speech['timeout_secs'])

    def speak(self, record, settings, kind, callback):
        if not self.archive.claim_speech(record['id'], kind):
            return
        if json.loads(record['metadata']).get('speech_owner') == 'client':
            self.archive.finish_speech(record['id'], kind, 'delegated')
            return
        if not settings['commands']['speech']['enabled']:
            self.archive.finish_speech(record['id'], kind, 'disabled')
            return
        try:
            callback(self.feedback(settings))
        except Exception as exc:
            self.archive.finish_speech(record['id'], kind, 'failed', safe_error(exc))
        else:
            self.archive.finish_speech(record['id'], kind, 'played')

    def transcribe(self, record, settings):
        attempt = record['transcribe_attempts'] + 1
        if attempt > len(settings['retry_delays_secs']):
            raise IntakeError('transcription_retry_budget_exhausted')
        self.archive.update(record['id'], phase='transcribing', transcribe_attempts=attempt)
        # Acknowledge the already-archived recording before decoding/auth/STT.
        # The same durable claim is reused by acknowledge() and retries.
        self.speak(record, settings, 'ack-prefix', lambda voice: voice.prefix())
        metadata = json.loads(record['metadata'])
        result = self.transcriber({'audio': record['audio'], 'audio_mime': metadata['audio_mime'], 'settings': json.dumps(settings)})
        jev_router.validate_text(result.get('text'))
        response = json.dumps(result.get('response'), ensure_ascii=False)
        if len(response.encode()) > 2 * 1024 * 1024:
            raise IntakeError('transcription_receipt_too_large')
        self.archive.update(record['id'], phase='ready', text=result['text'], transcription_response=response,
                            error=None, error_stage=None, retry_at=0)

    def acknowledge(self, record, settings):
        self.speak(record, settings, 'ack-prefix', lambda voice: voice.prefix())
        self.speak(record, settings, 'readback', lambda voice: voice.speak(record['text'], voice.self_voice))
        self.archive.update(record['id'], phase='acknowledged')

    def route(self, record, settings):
        self.acknowledge(record, settings)
        self.archive.update(record['id'], phase='routing', route_attempts=record['route_attempts'] + 1)
        command = settings['commands']
        if triage_conversation.resolve(self, record, settings):
            return
        snapshot = None
        builtins = command['caco']['enabled']
        try:
            if record['endpoint_action'] != 'pebble' and not action_registry.custom(record['endpoint_action']):
                if command['context_file']:
                    with Path(command['context_file']).expanduser().open('rb') as stream:
                        raw = stream.read(jev_router.MAX_SNAPSHOT_BYTES + 1)
                    if len(raw) > jev_router.MAX_SNAPSHOT_BYTES:
                        raise jev_router.RouterError('context_too_large')
                    snapshot = json.loads(raw)
                elif builtins:
                    raise jev_router.RouterError('context_required')
                else:
                    snapshot = {'version': 1, 'created_at': time.time(), 'projects': [], 'agents': [], 'nodes': [], 'messages': []}
            plan = self.planner(record['text'], snapshot, record['endpoint_action'], binary=str(Path(command['jev']['binary']).expanduser()),
                                provider=command['jev']['provider'], model=command['jev']['model'],
                                timeout=command['jev']['timeout_secs'], max_age=command['jev']['max_context_age_secs'],
                                **({'actions': command['actions']} if command.get('actions') else {}),
                                **({'project_aliases': command['project_aliases']} if command.get('project_aliases') else {}),
                                **({'clarify': True} if record['conversation'] else {}),
                                **({} if builtins else {'builtins': False}))
        except (OSError, ValueError, jev_router.RouterError, ProcessError) as exc:
            if record['endpoint_action'] != 'route' or record['conversation'] or not builtins:
                raise
            # Failure is not a Jev vote. Hand the unchanged fresh request to the
            # assistant, without inventing a project or silently changing a
            # fixed-action endpoint. This is still a single internal action.
            plan = jev_router.plan(record['text'], None, 'pebble')
            plan['fallback_reason'] = safe_error(exc)
        if plan.get('text') != record['text']:
            raise IntakeError('routing_transcript_changed')
        if plan.get('clarification') and record['conversation']:
            triage_conversation.pause(self.archive, record, plan, snapshot)
            return
        if action_registry.custom(plan['action']) and not command.get('extractor', {}).get('enabled', False):
            raise IntakeError('custom_action_requires_payload_extractor')
        if plan['action'] != 'pebble' and command.get('extractor', {}).get('enabled', False):
            dispatch.prepare(plan, record['id'], command, dry_run=json.loads(record['metadata']).get('dry_run', False))
            # Commit the original routing decision before the text-generation
            # boundary. Retry/restart must not choose a different destination.
            plan = {**plan, 'payload_policy': 'extract-v1'}
            self.archive.save_route(record['id'], plan, snapshot)
            return
        recipe = dispatch.prepare(plan, record['id'], command, dry_run=json.loads(record['metadata']).get('dry_run', False))
        self.archive.plan(record['id'], plan, recipe, snapshot)

    def extract(self, record, settings):
        self.archive.update(record['id'], phase='extracting')
        plan = json.loads(record['plan'])
        result = self.extractor(plan, settings['commands']['extractor'])
        enriched = payload_extractor.apply(plan, result)
        recipe = dispatch.prepare(enriched, record['id'], settings['commands'], dry_run=json.loads(record['metadata']).get('dry_run', False))
        self.archive.plan(record['id'], enriched, recipe, json.loads(record['context']))

    def execute(self, record, settings):
        operation = self.archive.action(record['id'])
        recipe = json.loads(operation['recipe'])
        if json.loads(record['metadata']).get('dry_run', False) != recipe.get('dry_run', False):
            raise IntakeError('frozen_mode_mismatch')
        if recipe.get('dry_run') is True:
            self.speak(record, settings, 'routing', lambda voice: voice.speak('pebble: dry run; no action command will be executed'))
            self.archive.finish_preview(record['id'])
            return
        config = settings['commands']['caco']
        env = self.caco_env(config)
        if action_registry.custom(recipe['action']):
            self.speak(record, settings, 'routing', lambda voice: voice.speak('pebble: preparing action ' + recipe['action'].removeprefix('custom:')))
            recipe = self.archive.start_action(record['id'])
            try:
                result = self.custom_sender(recipe, env=env)
            except Exception as exc:
                not_started = isinstance(exc, dispatch.NotStarted) or (isinstance(exc, ProcessError) and exc.code == 'command_unavailable')
                self.archive.finish_action(record['id'], 'not_started' if not_started else 'indeterminate', error=safe_error(exc))
            else:
                self.archive.finish_action(record['id'], result['state'], result)
            return
        if not action_registry.custom(recipe['action']) and not settings['commands']['caco']['enabled']:
            self.archive.finish_action(record['id'], 'not_started', error='caco_adapter_disabled')
            return
        if recipe['action'] == 'broadcast':
            audience = 'all projects globally' if recipe['scope'] == 'global' else 'project ' + recipe['project']
            self.speak(record, settings, 'routing', lambda voice: voice.speak('pebble: preparing broadcast to ' + audience))
        else:
            self.speak(record, settings, 'routing', lambda voice: voice.routing(recipe['action']))
        recipe = self.archive.start_action(record['id'])
        try:
            if self.sender:
                result = self.sender(recipe)
            else:
                key = json.dumps(config, sort_keys=True)
                if key not in self.caco_clients:
                    if len(self.caco_clients) >= 8:
                        self.caco_clients.pop(next(iter(self.caco_clients)))
                    self.caco_clients[key] = dispatch.Caco(binary=str(Path(config['binary']).expanduser()), timeout=config['timeout_secs'], env=env)
                result = self.caco_clients[key].execute(recipe)
        except Exception as exc:
            # Failure to spawn proves no mutation began. Exit codes, timeouts
            # and lost receipts cannot; preserve their uncertain outcome.
            not_started = isinstance(exc, dispatch.NotStarted) or (isinstance(exc, ProcessError) and exc.code == 'command_unavailable')
            state = 'not_started' if not_started else 'indeterminate'
            self.archive.finish_action(record['id'], state, error=safe_error(exc))
        else:
            self.archive.finish_action(record['id'], result['state'], result)

    def confirm(self, record, settings):
        operation = self.archive.action(record['id'])
        outcome = json.loads(operation['receipt']) if operation['receipt'] else {}
        state, error = operation['state'], operation['error']
        blocked = state not in ('stored', 'created', 'dry_run', 'completed')
        if state == 'dry_run':
            action = outcome['action']
            description = ('run custom action ' + action.removeprefix('custom:') if action_registry.custom(action) else
                           'send to the dedicated Pebble assistant' if action == 'pebble' else
                           'broadcast globally to all projects' if action == 'broadcast' and outcome['scope'] == 'global' else
                           'broadcast to project ' + outcome['project'] if action == 'broadcast' else
                           'send a message to ' + outcome['target'] if action == 'agent-msg' else
                           'expand into beads in ' + outcome['project'] if action == 'bd-expand' else
                           f"file a P{outcome['priority']} bead in {outcome['project']}: {outcome['title']}")
            if outcome.get('payload_preview') and action != 'bd-file':
                description += ': ' + outcome['payload_preview']
            callback = lambda voice: voice.speak('pebble: would ' + description + '. No action command was executed.')
        elif state == 'completed':
            callback = lambda voice: voice.speak('pebble: custom command ' + outcome['action'].removeprefix('custom:') + ' completed')
        elif state == 'created':
            beads = outcome['beads']
            callback = (lambda voice: voice.beads_created(beads)) if beads else (lambda voice: voice.speak('pebble: expansion finished; no beads were created'))
        elif state == 'stored' and outcome['action'] == 'broadcast':
            delivery = outcome.get('delivery') or {}
            admission = outcome.get('admission') or {}
            blocked = (delivery.get('state') in ('failed', 'partially_delivered', 'backpressured')
                       or delivery.get('failed_recipients', 0) > 0
                       or any(admission.get(k) in ('rejected', 'indeterminate') for k in ('runtime_delivery_state', 'pi_admission_state')))
            error = 'broadcast_delivery_incomplete' if blocked else None
            audience = 'all projects globally' if outcome['scope'] == 'global' else 'project ' + outcome['project']
            callback = lambda voice: voice.speak('pebble: broadcast stored for ' + audience +
                ('; some deliveries need attention. Not resent.' if blocked else '; recipient execution is not confirmed.'))
        elif state == 'stored':
            admission = outcome['admission']
            rejected = any(admission.get(key) == 'rejected' for key in ('runtime_delivery_state', 'pi_admission_state'))
            uncertain = any(admission.get(key) == 'indeterminate' for key in ('runtime_delivery_state', 'pi_admission_state'))
            blocked = rejected or uncertain
            error = 'agent_delivery_rejected' if rejected else 'agent_delivery_indeterminate' if uncertain else None
            # Normal assistant response speech belongs to Pebble's native TTS.
            # Finish acknowledgement and routing speech BEFORE the send; do not
            # race a redundant success utterance against its response.
            callback = None if outcome['action'] == 'pebble' and not blocked else (lambda voice: voice.message_stored(outcome['target'], admission))
        elif state == 'pending':
            error = 'bead_creation_pending'
            callback = lambda voice: voice.speak('pebble: bead creation queued; no bead ID is confirmed yet')
        elif state == 'not_started':
            callback = lambda voice: voice.speak('pebble: request retained; the action could not start')
        else:
            callback = lambda voice: voice.speak('pebble: request retained; action outcome unconfirmed; not resent')
        if callback:
            self.speak(record, settings, 'confirmation-' + state, callback)
        self.archive.update(record['id'], phase='blocked' if blocked else state,
                            error=error, error_stage='dispatch' if blocked else None)

    def wait_for_destination(self, record, settings):
        self.speak(record, settings, 'waiting', lambda voice: voice.speak('pebble: saved; this destination is waiting on an earlier request'))

    def step(self, record=None):
        # Production passes a coordinator-claimed snapshot. The no-argument
        # path remains synchronous for operator tooling and existing fixtures.
        if record is None:
            record = self.archive.next_work()
        if not record:
            return False
        # next_work may have selected a row just before cancellation committed.
        if record['cancelled_at'] is not None:
            return True
        settings = self.settings(record)
        phase = record['phase'].removeprefix('dry_') if json.loads(record['metadata']).get('dry_run', False) else record['phase']
        stage = record.get('work_hint') or {'queued': 'transcribe', 'ready': 'route',
                 'acknowledged': 'route', 'payload_pending': 'extract', 'planned': 'execute', 'confirming': 'confirm'}.get(phase)
        if stage is None:
            raise IntakeError('unknown_command_phase')
        try:
            getattr(self, stage)(record, settings)
        except Exception as exc:
            # Persistence errors must stop the worker; don't continue after an
            # uncertain ledger commit. Domain/provider errors retain the input.
            if isinstance(exc, sqlite3.Error):
                raise
            current = self.archive.get(record['id'])
            if current['cancelled_at'] is not None:
                return True
            try:
                if stage == 'transcribe' and getattr(exc, 'retryable', False) and current['transcribe_attempts'] < len(settings['retry_delays_secs']):
                    delay = settings['retry_delays_secs'][max(0, current['transcribe_attempts'] - 1)]
                    self.archive.update(record['id'], phase='queued', error=safe_error(exc), error_stage=stage, retry_at=time.time() + delay)
                else:
                    self.archive.update(record['id'], phase='blocked', error=safe_error(exc), error_stage=stage)
                    self.speak(record, settings, 'retained-' + stage, lambda voice: voice.speak('pebble: request retained; ' +
                               ('transcription unavailable; device text was not used' if stage == 'transcribe' else
                                'payload extraction needs attention; nothing sent' if stage == 'extract' else 'routing or delivery needs attention')))
            except IntakeError as retained_error:
                # Cancellation can also win after the error handler's read.
                if retained_error.code != 'input_cancelled':
                    raise
        return True
