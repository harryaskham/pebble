"""Read-only validation of server-owned proposals; never routes or extracts text."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat

import action_registry
import dispatch
import jev_router
import payload_extractor


class ProposalError(Exception):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def authority(settings):
    """Only the runtime substitutions already allowed by Processor are exempt.

    Credential *contents* are external, not hashed. A different credential
    reference, provider policy, custom command or daemon is different authority.
    Speech is presentation only (Triage delegates it to the client).
    """
    command = copy.deepcopy(settings['commands'])
    command.pop('speech', None)
    command.pop('actions_file', None)  # Compare the resolved catalog, not its path.
    command['actions'] = action_registry.validate(command.get('actions', {}))
    command['project_aliases'] = jev_router.validate_aliases(command.get('project_aliases', {}))
    for section in ('jev', 'caco'):
        command[section].pop('binary', None)
    return command


def current_settings(config):
    settings = copy.deepcopy(config)
    settings['commands']['actions'] = action_registry.catalog(settings['commands'])
    settings['commands']['actions_file'] = ''
    return settings


def proposal_revision(source, action):
    """None is deliberately not an approvable revision (including fallback)."""
    try:
        metadata = json.loads(source['metadata'])
        if (source['phase'] != 'dry_run' or source['cancelled_at'] is not None or source['error']
                or source['legacy_reference'] or metadata.get('dry_run') is not True
                or not action or action['state'] != 'dry_run' or action['attempts'] != 0 or action['error']):
            return None
        plan, recipe = json.loads(source['plan']), json.loads(action['recipe'])
        settings, context = json.loads(source['settings']), json.loads(source['context'])
        conversation = json.loads(source['conversation']) if source['conversation'] else {}
        original = conversation.get('command_text', source['text'])
        if (plan.get('fallback_reason') or plan.get('clarification') or plan['text'] != original
                or source['text'] != metadata['transcription']):
            return None
        if digest(recipe) != action['recipe_hash'] or recipe != dispatch.prepare(plan, source['id'], settings['commands'], dry_run=True):
            return None
        proof = {'version': 1, 'input_id': source['id'], 'plan': plan, 'recipe_hash': action['recipe_hash'],
                 'settings': settings, 'context': context}
        if 'command_text' in conversation:
            proof['resolution_source'] = {'text': source['text'], 'metadata': metadata, 'conversation': conversation}
        return digest(proof)
    except (KeyError, TypeError, ValueError, dispatch.DispatchError, jev_router.RouterError,
            payload_extractor.ExtractionError, action_registry.RegistryError):
        return None


def read_context(command):
    fd = os.open(Path(command['context_file']).expanduser(), os.O_RDONLY | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ProposalError('stale_proposal_context')
        raw = stream.read(jev_router.MAX_SNAPSHOT_BYTES + 1)
    if len(raw) > jev_router.MAX_SNAPSHOT_BYTES:
        raise ProposalError('stale_proposal_context')
    result = json.loads(raw)
    jev_router.validate_snapshot(result, max_age=command['jev']['max_context_age_secs'])
    return result


def destination(plan, snapshot):
    """Compare configured identities, not rolling messages/beads/timestamps.

    An explicit global broadcast retains its dynamic global audience contract.
    A fixed Pebble target and custom action get authority from configuration.
    """
    if plan['action'] == 'pebble' or action_registry.custom(plan['action']):
        return None
    if plan['action'] == 'broadcast' and plan.get('scope') == 'global':
        return {'scope': 'global'}
    projects = [p for p in snapshot['projects'] if p.get('id') == plan.get('project')]
    if len(projects) != 1:
        raise ProposalError('stale_proposal_destination')
    result = {'project': {k: projects[0].get(k) for k in ('id', 'name', 'description')}}
    if plan['action'] == 'agent-msg':
        agents = [a for a in snapshot['agents'] if a.get('target') == plan['target'] and a.get('state') in jev_router.ACTIVE_STATES]
        if len(agents) != 1 or agents[0].get('project') not in (None, plan['project']):
            raise ProposalError('stale_proposal_destination')
        result['agent'] = {k: agents[0].get(k) for k in ('target', 'project', 'node', 'inventory_source')}
    return result


def validate_authority(source, current):
    """Called at approval admission and again at its dispatch fence.

    Returns only local snapshots. No model, Caco/help or custom command is run.
    Fail closed: callers must request a fresh preview, never repair the plan.
    """
    try:
        frozen = json.loads(source['settings'])
        if authority(frozen) != authority(current) or not current['commands']['enabled']:
            raise ProposalError('stale_proposal_authority')
        plan = json.loads(source['plan'])
        if plan['action'] == 'pebble' or action_registry.custom(plan['action']):
            return
        old_context = json.loads(source['context'])
        new_context = read_context(current['commands'])
        if destination(plan, old_context) != destination(plan, new_context):
            raise ProposalError('stale_proposal_destination')
    except ProposalError:
        raise
    except (OSError, KeyError, TypeError, ValueError, jev_router.RouterError, action_registry.RegistryError) as exc:
        raise ProposalError('stale_proposal_context') from exc
