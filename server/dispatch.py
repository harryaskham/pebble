"""Frozen Pebble action recipes and receipt parsing around the real Caco CLI.

Planning never calls Caco. The owning ledger commits a recipe and an attempting
record before execute(). A lost/invalid result is indeterminate, not rejection.
"""
from __future__ import annotations

import json
import os
import re
import action_registry

from jev_router import validate_text
from payload_extractor import payload_for
from process_io import run


class DispatchError(Exception):
    pass


class NotStarted(DispatchError):
    """A local guard failed before any mutating command was invoked."""


def identifier(value):
    return isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,199}', value) is not None


def target_parts(value):
    if not isinstance(value, str):
        raise DispatchError('invalid_target')
    parts = value.split(':')
    if len(parts) != 3 or not all(identifier(part) for part in parts):
        raise DispatchError('invalid_target')
    return parts


def prepare(plan, input_id, settings, *, dry_run=False):
    """Freeze domain values, not executable paths or credentials."""
    if not isinstance(input_id, str) or not re.fullmatch(r'[a-f0-9]{32}', input_id):
        raise DispatchError('invalid_input_identity')
    if not isinstance(plan, dict) or plan.get('version') != 1 or plan.get('executed') is not False:
        raise DispatchError('invalid_plan')
    if type(dry_run) is not bool:
        raise DispatchError('invalid_dry_run_mode')
    original = plan.get('text')
    validate_text(original)
    text = payload_for(plan)
    if plan.get('payload_policy') == 'extract-v1' and 'extraction' not in plan:
        raise DispatchError('payload_extraction_required')
    action = plan.get('action')
    if action not in ('pebble', 'agent-msg', 'broadcast', 'bd-file', 'bd-expand') and not action_registry.custom(action):
        raise DispatchError('invalid_action')
    if not action_registry.custom(action) and not settings.get('caco', {}).get('enabled', True):
        raise DispatchError('caco_adapter_disabled')
    recipe = {'version': 1, 'input_id': input_id, 'action': action, 'text': text, 'original_text': original,
              'request_id': 'pebble-command-' + input_id, 'idempotent': action not in ('bd-expand', 'broadcast'), 'dry_run': dry_run}
    if action_registry.custom(action):
        spec = action_registry.spec_for(action, settings.get('actions', {}))
        recipe.update(custom_spec=spec, idempotent=False)
        return recipe
    if action == 'pebble':
        target = settings['pebble_target']
        project = settings['pebble_project']
        target_parts(target)
        if project != target.split(':')[1]:
            raise DispatchError('target_project_mismatch')
        recipe.update(project=project, target=target, raw=True,
                      body='Pebble voice input\n' + json.dumps({'version': 1, 'event_id': input_id, 'text': text}, ensure_ascii=False))
    elif action == 'agent-msg':
        target = plan.get('target')
        parts = target_parts(target)
        if plan.get('project') != parts[1] or plan.get('body') != text:
            raise DispatchError('invalid_message_plan')
        recipe.update(project=parts[1], target=target, body=text, raw=False)
    elif action == 'broadcast':
        scope = plan.get('scope')
        project = plan.get('project')
        if scope not in ('global', 'project') or plan.get('body') != text:
            raise DispatchError('invalid_broadcast_plan')
        if (scope == 'project' and not identifier(project)) or (scope == 'global' and project is not None):
            raise DispatchError('invalid_broadcast_scope')
        recipe.update(scope=scope, project=project, body=text)
    else:
        if not identifier(plan.get('project')):
            raise DispatchError('invalid_project')
        recipe['project'] = plan['project']
        if action == 'bd-file':
            if (plan.get('description') != text or plan.get('title') != ' '.join(text[:80].split())
                    or not plan['title'] or plan.get('labels') != ['from-pebble']
                    or type(plan.get('priority')) is not int or not 0 <= plan['priority'] <= 4):
                raise DispatchError('invalid_bead_plan')
            recipe.update(title=plan['title'], description=text, labels=['from-pebble'], priority=plan['priority'])
    return recipe


def command(recipe, binary):
    """Structured argv/stdin, with no shell or executable selected by the model."""
    action = recipe['action']
    if action in ('pebble', 'agent-msg'):
        argv = [binary, 'msg', 'send', '--project', recipe['project'], '--target', recipe['target'],
                '--body-stdin', '--wake', '--idempotency-key', recipe['request_id'], '--json']
        if recipe['raw']:
            argv.append('--raw')
        return argv, recipe['body'].encode()
    if action == 'broadcast':
        audience = ['--global'] if recipe['scope'] == 'global' else ['--project', recipe['project']]
        # Broadcast has no CLI idempotency key. Never invent one or replay an
        # uncertain fan-out. Caco, not the router, chooses actual recipients.
        return [binary, 'msg', 'broadcast', *audience, '--body-stdin', '--confirm-broadcast', '--direct-send=true', '--wake', '--json'], recipe['body'].encode()
    if action == 'bd-file':
        return [binary, 'bd', 'create', '--project', recipe['project'], '--title', recipe['title'],
                '--description-stdin', '--priority', str(recipe['priority']), '--labels', 'from-pebble',
                '--request-id', recipe['request_id'], '--json'], recipe['description'].encode()
    if action == 'bd-expand':
        # Caco's current expand CLI exposes --text but no text-stdin/request-id.
        # Use its actual supported interface. Never silently implement another
        # expansion API or invent an idempotency flag; uncertain calls stop.
        return [binary, 'bd', 'expand', '--project', recipe['project'], '--text', recipe['text'], '--json'], b''
    raise DispatchError('invalid_action')


def bead_receipt(value):
    if not isinstance(value, dict) or not isinstance(value.get('id'), str) or not re.fullmatch(r'bd-[A-Za-z0-9_-]+', value['id']):
        raise DispatchError('invalid_bead_receipt')
    if not isinstance(value.get('title'), str) or not value['title'].strip() or len(value['title']) > 500:
        raise DispatchError('invalid_bead_receipt')
    return {key: value[key] for key in ('id', 'title', 'priority') if key in value}


def receipt(recipe, value):
    if not isinstance(value, dict) or value.get('ok') is not True or not isinstance(value.get('data'), dict):
        raise DispatchError('caco_result_unconfirmed')
    data = value['data']
    if data.get('dry_run') is True:
        raise DispatchError('caco_result_was_only_a_preview')
    action = recipe['action']
    result = {'action': action, 'project': recipe['project'], 'request_id': recipe['request_id'], 'raw': value}
    if action == 'broadcast':
        message_id = data.get('id')
        if not isinstance(message_id, str) or not re.fullmatch(r'msg-[A-Za-z0-9_-]{1,196}', message_id):
            raise DispatchError('invalid_broadcast_receipt')
        if (data.get('kind') != 'broadcast' or data.get('scope') != recipe['scope'] or data.get('target') is not None
                or (recipe['scope'] == 'project' and data.get('project') != recipe['project'])
                or (recipe['scope'] == 'global' and data.get('project') not in (None, ''))):
            raise DispatchError('broadcast_scope_mismatch')
        admission = data.get('admission')
        if admission is not None and (not isinstance(admission, dict) or admission.get('storage_state') != 'stored'
                or admission.get('message_id') not in (None, message_id)):
            raise DispatchError('broadcast_storage_unconfirmed')
        delivery = data.get('delivery_receipt')
        summary = None
        if delivery is not None:
            fields = ('total_recipients', 'accepted_recipients', 'delivered_recipients', 'failed_recipients', 'pending_recipients', 'backpressured_recipients')
            if (not isinstance(delivery, dict) or delivery.get('origin_id') != message_id or delivery.get('logical_accepted') is not True
                    or delivery.get('state') not in ('accepted', 'fully_delivered', 'partially_delivered', 'failed', 'pending', 'backpressured')
                    or any(type(delivery.get(k)) is not int or delivery[k] < 0 for k in fields)):
                raise DispatchError('invalid_broadcast_delivery_receipt')
            if (any(delivery[k] > delivery['total_recipients'] for k in fields[1:])
                    or delivery['delivered_recipients'] > delivery['accepted_recipients']
                    or delivery['backpressured_recipients'] > delivery['pending_recipients']):
                raise DispatchError('invalid_broadcast_delivery_counts')
            summary = {k: delivery[k] for k in ('state', *fields)}
        if admission is not None and any(admission.get(k) is not None and
                (not isinstance(admission[k], str) or len(admission[k]) > 100)
                for k in ('runtime_delivery_state', 'pi_admission_state')):
            raise DispatchError('invalid_broadcast_admission')
        result.update(state='stored', scope=recipe['scope'], message_id=message_id, delivery=summary,
                      admission={k: admission.get(k) for k in ('storage_state', 'runtime_delivery_state', 'pi_admission_state')} if admission else None)
        return result
    if action in ('pebble', 'agent-msg'):
        message_id = data.get('id')
        if not isinstance(message_id, str) or not re.fullmatch(r'msg-[A-Za-z0-9_-]{1,196}', message_id):
            raise DispatchError('invalid_message_receipt')
        if data.get('target') != recipe['target'] or data.get('project', recipe['project']) != recipe['project']:
            raise DispatchError('message_recipient_mismatch')
        admission = data.get('admission')
        if admission is None:
            admission = {'storage_state': 'stored', 'runtime_delivery_state': 'unknown', 'pi_admission_state': 'unavailable'}
        if not isinstance(admission, dict) or admission.get('storage_state') != 'stored':
            raise DispatchError('message_storage_unconfirmed')
        if admission.get('message_id') not in (None, message_id):
            raise DispatchError('message_receipt_identity_mismatch')
        if any(admission.get(key) is not None and (not isinstance(admission[key], str) or len(admission[key]) > 100)
               for key in ('runtime_delivery_state', 'pi_admission_state')):
            raise DispatchError('invalid_message_admission')
        result.update(state='stored', message_id=message_id, target=recipe['target'],
                      admission={key: admission.get(key) for key in ('storage_state', 'runtime_delivery_state', 'pi_admission_state')})
        return result
    if data.get('status') == 'queued':
        result.update(state='pending', beads=[])
        return result
    if action == 'bd-file':
        beads = [bead_receipt(data)]
        if beads[0]['title'] != recipe['title']:
            raise DispatchError('bead_receipt_title_mismatch')
    else:
        created = data.get('created')
        if not isinstance(created, list) or len(created) > 100 or type(data.get('count')) is not int or data['count'] != len(created):
            raise DispatchError('invalid_expansion_receipt')
        beads = [bead_receipt(bead) for bead in created]
        if len({bead['id'] for bead in beads}) != len(beads):
            raise DispatchError('duplicate_expansion_receipt')
    result.update(state='created', beads=beads)
    return result


class Caco:
    def __init__(self, *, binary='caco', timeout=130, env=None, runner=run):
        self.binary, self.timeout, self.runner = binary, timeout, runner
        self.env = dict(os.environ if env is None else env)

    def execute(self, recipe):
        if action_registry.custom(recipe.get('action')):
            raise NotStarted('custom_action_requires_custom_executor')
        if recipe.get('dry_run', False) is not False:
            raise NotStarted('dry_run_execution_forbidden')
        # Invoke the configured CLI directly. No help/version probe, capability
        # cache or executable-change gate; command failures use the normal receipt path.
        argv, payload = command(recipe, self.binary)
        result = self.runner(argv, payload, timeout=self.timeout, max_output=262144, env=self.env)
        try:
            value = json.loads(result.stdout)
        except (ValueError, UnicodeError) as exc:
            raise DispatchError('invalid_caco_json') from exc
        parsed = receipt(recipe, value)
        parsed['elapsed_ms'] = result.elapsed_ms
        return parsed
