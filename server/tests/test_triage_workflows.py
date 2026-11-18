"""Workflow contracts against disposable archives, real HTTP and barrier fakes.

No real provider, action executable, Caco daemon or audio sink is used here.
"""
import concurrent.futures
import copy
import json
from pathlib import Path
import sqlite3
import threading
import time
import unittest
import urllib.error
from unittest.mock import Mock, patch

import test_command_pipeline as fixtures
import test_triage
import command_pipeline
import intake
import triage_store


def assert_preserved_preview(case, original, current, owner):
    # A new relationship is observable; no old preview content/event is rewritten.
    for key, value in original.items():
        if key not in ('events', 'cursor', 'consumed_by'):
            case.assertEqual(current[key], value, key)
    case.assertEqual(current['consumed_by'], owner)
    case.assertEqual(current['events'][:len(original['events'])], original['events'])
    added = current['events'][len(original['events']):]
    case.assertEqual([e['kind'] for e in added], ['reference_consumed'])
    case.assertGreater(current['cursor'], original['cursor'])


class WorkflowsTest(unittest.TestCase):
    drain = fixtures.PipelineTest.drain
    http = fixtures.PipelineTest.http
    request = test_triage.TriageTest.request

    def setUp(self):
        fixtures.PipelineTest.setUp(self)
        self.store = triage_store.Turns(self.archive)
        self.store.session('workflow')
        self.planner = Mock(wraps=fixtures.fake_plan)
        self.processor.planner = self.planner
        self.extractor = Mock(return_value={'result': {'status': 'ok', 'segments': ['keep every requirement\nincluding “quoted” text']},
            'requested_model': 'fixture-extractor', 'model': 'fixture-extractor', 'elapsed_ms': 1})
        self.processor.extractor = self.extractor

    def submit(self, key='preview', **extra):
        return self.store.submit('workflow', {'client_turn_id': key,
            'text': 'File a P0 bead in sdk: keep every requirement\nincluding “quoted” text', 'mode': 'dry_run', **extra})

    def preview(self, **extra):
        turn = self.submit(**extra)
        self.drain()
        turn = self.store.get(turn['id'])
        self.assertEqual(turn['phase'], 'dry_run')
        self.assertEqual(len(turn['proposal_revision']), 64)
        return turn

    def approval_body(self, preview, **extra):
        return {'client_turn_id': 'approval', 'text': 'Yes, do exactly that.', 'mode': 'live', 'operation': 'approve',
                'reference_turn': preview['id'], 'expected_revision': preview['proposal_revision'], **extra}

    def approve(self, preview, **extra):
        return self.store.submit('workflow', self.approval_body(preview, **extra))

    def assert_error(self, code, call, status=409):
        with self.assertRaises(triage_store.Error) as error:
            call()
        self.assertEqual(error.exception.status, status)
        self.assertEqual(error.exception.code, code)
        return error.exception

    def rows(self):
        with self.archive.db() as db:
            return {table: [dict(r) for r in db.execute('SELECT * FROM ' + table)] for table in (
                'command_inputs', 'command_requests', 'command_actions', 'command_speech', 'command_audit', 'triage_turns', 'triage_events')}

    def barrier(self, method):
        """Pause one read-only stage / just before or after the durable fence."""
        entered, release = threading.Event(), threading.Event()
        def paused(*args, **kwargs):
            entered.set()
            if not release.wait(4):
                raise AssertionError('barrier release timed out')
            return method(*args, **kwargs)
        return entered, release, paused

    def run_paused_step(self, owner, name, during):
        original = getattr(owner, name)
        entered, release, paused = self.barrier(original)
        with patch.object(owner, name, paused), concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.processor.step)
            try:
                self.assertTrue(entered.wait(3), 'worker never reached barrier')
                during()
            finally:
                release.set()
            self.assertTrue(future.result(timeout=5))

    def assert_cancelled(self, turn):
        result = self.store.get(turn['id'])
        self.assertEqual(result['phase'], 'skipped')
        self.assertTrue(result['done'])
        self.assertEqual(result['receipt']['cancellation']['state'], 'cancelled')
        self.assertTrue(result['receipt']['cancellation']['no_action_command_started'])
        self.assertIn('No action command started', self.store.response_text(result))
        self.assertIsNone(result['proposal_revision'])
        self.assertFalse(self.processor.step())
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.voice.calls, [])
        return result

    def test_approval_provenance_fresh_identity_no_model_calls_or_preview_changes(self):
        self.config['commands']['extractor']['enabled'] = True
        preview = self.preview()
        before = self.rows()
        old_id = preview['receipt']['id']
        old_recipe = json.loads(self.archive.action(old_id)['recipe'])
        self.planner.side_effect = AssertionError('approval must not reroute')
        self.extractor.side_effect = AssertionError('approval must not re-extract')
        approved = self.approve(preview)
        new_id = approved['receipt']['id']
        self.assertEqual(approved['phase'], 'planned')
        self.assertEqual(approved['text'], 'Yes, do exactly that.')
        self.assertEqual(approved['expected_revision'], preview['proposal_revision'])
        self.assertEqual(approved['reference_turn'], preview['id'])
        self.assertIsNone(approved['proposal_revision'])
        self.assertNotEqual(old_id, new_id)
        source = self.archive.get(new_id)
        self.assertEqual(source['text'], preview['text'])
        self.assertEqual(json.loads(source['metadata'])['transcription'], preview['text'])
        self.assertEqual(json.loads(source['metadata'])['approval']['reference_turn'], preview['id'])
        self.assertEqual(source['plan'], self.archive.get(old_id)['plan'])
        self.assertEqual(source['context'], self.archive.get(old_id)['context'])
        recipe = json.loads(self.archive.action(new_id)['recipe'])
        self.assertEqual(recipe, {**old_recipe, 'input_id': new_id, 'request_id': 'pebble-command-' + new_id, 'dry_run': False})
        self.assertEqual(recipe['description'], 'keep every requirement\nincluding “quoted” text')
        self.assertEqual(recipe['original_text'], preview['text'])
        self.assertEqual(recipe['priority'], 0)
        self.drain()
        self.assertEqual(self.planner.call_count, 1)
        self.assertEqual(self.extractor.call_count, 1)
        self.assertEqual(self.sender.calls, [recipe])
        final = self.store.get(approved['id'])
        self.assertEqual(final['phase'], 'created')
        self.assertIn('dispatching', [e['kind'] for e in final['events']])
        self.assertIn('created', [e['kind'] for e in final['events']])
        after = self.rows()
        for table, rows in before.items():
            self.assertEqual(after[table][:len(rows)], rows, table + ' changed preview rows')
        observed = self.store.get(preview['id'])
        for key, value in preview.items():
            if key not in ('events', 'cursor', 'consumed_by'):
                self.assertEqual(observed[key], value, key)
        self.assertEqual(observed['consumed_by'], approved['id'])
        self.assertEqual(observed['events'][:len(preview['events'])], preview['events'])
        self.assertEqual(sum(e['kind'] == 'reference_consumed' for e in observed['events']), 1)
        self.assertEqual(self.voice.calls, [])

    def test_approval_concurrent_retry_dedupes_after_restart_and_authority_change(self):
        preview = self.preview()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            turns = list(pool.map(lambda _: self.approve(preview), range(8)))
        self.assertEqual(len({t['id'] for t in turns}), 1)
        self.drain()
        self.store = triage_store.Turns(intake.CommandArchive(self.legacy.archive))
        self.config['commands']['jev']['provider'] = 'custom'
        again = self.approve(preview)
        self.assertEqual(again['id'], turns[0]['id'])
        self.assertEqual(again['phase'], 'created')
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(len(self.rows()['command_requests']), 2)
        for change in ({'text': 'different'}, {'mode': 'dry_run'}, {'expected_revision': 'f' * 64},
                       {'reference_turn': 'another'}, {'route': 'pebble'}):
            self.assert_error('turn_identity_conflict', lambda: self.approve(preview, **change))
        self.assertEqual(len(self.rows()['command_inputs']), 2)

    def test_atomic_approval_rollback_at_link_recipe_and_event(self):
        preview = self.preview()
        before = self.rows()
        for table in ('triage_turns', 'command_actions', 'triage_events'):
            with self.subTest(table=table):
                with self.archive.db() as db:
                    db.execute(f"CREATE TRIGGER fixture_abort BEFORE INSERT ON {table} BEGIN SELECT RAISE(ABORT,'fixture'); END")
                try:
                    with self.assertRaises(sqlite3.IntegrityError): self.approve(preview)
                    self.assertEqual(self.rows(), before)
                finally:
                    with self.archive.db() as db: db.execute('DROP TRIGGER fixture_abort')
        # A failure after plan inserted its recipe + transition events also rolls everything back.
        original = self.archive.plan
        def failed(*args, **kwargs):
            original(*args, **kwargs)
            raise sqlite3.IntegrityError('fixture after plan')
        with patch.object(self.archive, 'plan', failed):
            with self.assertRaises(sqlite3.IntegrityError): self.approve(preview)
        self.assertEqual(self.rows(), before)
        self.assertFalse(self.processor.step())
        self.approve(preview); self.drain()
        self.assertEqual(len(self.sender.calls), 1)

    def test_approval_requires_exact_revision_live_mode_and_direct_preview(self):
        pending = self.submit()
        inspect = self.submit('inspect', operation='inspect', reference_turn=pending['id'])
        self.assertTrue(inspect['done'])
        self.assertIsNone(inspect['proposal_revision'])
        for target in (pending, inspect):
            self.assert_error('proposal_not_approvable', lambda: self.approve(target, expected_revision='a' * 64))
        self.drain(); preview = self.store.get(pending['id'])
        for change, code, status in (({'expected_revision': 'a' * 64}, 'stale_proposal_revision', 409),
                ({'expected_revision': None}, 'approval_reference_and_revision_required', 400),
                ({'mode': 'dry_run'}, 'approval_requires_live_mode', 400),
                ({'route': 'bd-file'}, 'approval_route_override_forbidden', 400)):
            self.assert_error(code, lambda: self.approve(preview, **change), status)
        self.store.session('other')
        self.assert_error('proposal_not_approvable', lambda: self.store.submit('other', self.approval_body(preview)))
        # Completed observations, live turns and approvals are never proposals.
        inspect = self.store.get(inspect['id'])
        self.assertIsNone(inspect['proposal_revision'])
        approved = self.approve(preview)
        self.drain()
        self.assert_error('proposal_not_approvable', lambda: self.approve(approved, client_turn_id='second', expected_revision='b' * 64))

    def test_failed_unresolved_corrupt_or_cancelled_preview_not_approvable(self):
        preview = self.preview()
        source = self.archive.get(preview['receipt']['id'])
        modifications = [
            ('error', 'fixture_failure'), ('phase', 'dry_blocked'),
            ('plan', json.dumps({**json.loads(source['plan']), 'fallback_reason': 'target_needs_resolution'})),
            ('plan', json.dumps({**json.loads(source['plan']), 'project': 'wrong-project'})),
            ('text', 'different source'), ('cancelled_at', time.time()),
        ]
        for key, value in modifications:
            with self.subTest(key=key, value=value):
                with self.archive.db() as db: db.execute(f'UPDATE command_inputs SET {key}=? WHERE id=?', (value, source['id']))
                self.assertIsNone(self.store.get(preview['id'])['proposal_revision'])
                self.assert_error('proposal_not_approvable', lambda: self.approve(preview))
                with self.archive.db() as db: db.execute(f'UPDATE command_inputs SET {key}=? WHERE id=?', (source[key], source['id']))
        self.assertEqual(self.sender.calls, [])

    def test_corrupt_extraction_proof_cannot_be_approved_or_repaired(self):
        self.config['commands']['extractor']['enabled'] = True
        preview = self.preview()
        source = self.archive.get(preview['receipt']['id'])
        plan = json.loads(source['plan'])
        plan['extraction']['result']['segments'] = ['invented by a client']
        with self.archive.db() as db:
            db.execute('UPDATE command_inputs SET plan=? WHERE id=?', (json.dumps(plan), source['id']))
        before = self.rows()
        self.assertIsNone(self.store.get(preview['id'])['proposal_revision'])
        self.assert_error('proposal_not_approvable', lambda: self.approve(preview))
        self.assertEqual(self.rows(), before)
        self.assertEqual(self.planner.call_count, 1)
        self.assertEqual(self.extractor.call_count, 1)
        self.assertEqual(self.sender.calls, [])

    def test_changed_authority_policy_requires_new_preview(self):
        preview = self.preview()
        original = copy.deepcopy(self.config['commands'])
        changes = [('jev', 'provider', 'custom'), ('jev', 'model', 'changed-model'),
                   ('caco', 'daemon_url', 'http://changed.invalid'), ('caco', 'token_file', '/different/reference'),
                   ('extractor', 'model', 'changed-model'), ('extractor', 'base_url', 'http://changed.invalid')]
        for section, key, value in changes:
            with self.subTest(section=section, key=key):
                self.config['commands'][section][key] = value
                self.assert_error('stale_proposal_authority', lambda: self.approve(preview))
                self.config['commands'] = copy.deepcopy(original)
        self.config['commands']['pebble_target'] = 'other:pebble:other'
        self.assert_error('stale_proposal_authority', lambda: self.approve(preview))
        self.assertEqual(len(self.rows()['command_inputs']), 1)
        self.assertEqual(self.sender.calls, [])

    def test_custom_catalog_snapshot_changes_rejected_even_after_admission(self):
        path = self.root / 'actions.json'
        spec = {'description': 'Fixture note', 'command': ['/fixture/never-run', 'note'], 'extract': 'Exact note'}
        path.write_text(json.dumps({'actions': {'note': spec}}))
        self.config['commands']['actions_file'] = str(path)
        self.config['commands']['extractor']['enabled'] = True
        self.processor.custom_sender = Mock(return_value={'state': 'completed', 'action': 'custom:note', 'exit_code': 0})
        preview = self.preview(route='custom:note')
        before = self.rows()
        for change in ({'command': ['/fixture/other-code']}, {'extract': 'new meaning'}, {'enabled': False}):
            path.write_text(json.dumps({'actions': {'note': {**spec, **change}}}))
            self.assert_error('stale_proposal_authority', lambda: self.approve(preview))
            self.assertEqual(self.rows(), before)
        path.write_text(json.dumps({'actions': {'note': spec}}))
        approved = self.approve(preview)
        path.write_text(json.dumps({'actions': {'note': {**spec, 'command': ['/fixture/changed-after-approval']}}}))
        self.drain()
        result = self.store.get(approved['id'])
        self.assertEqual(result['phase'], 'blocked')
        self.assertEqual(result['receipt']['error'], 'stale_proposal_authority')
        self.assertEqual(result['receipt']['action']['attempts'], 0)
        self.processor.custom_sender.assert_not_called()
        assert_preserved_preview(self, preview, self.store.get(preview['id']), approved['id'])

    def test_missing_project_or_agent_and_stale_context_never_retarget(self):
        preview = self.preview(route='agent-msg')
        path = Path(self.config['commands']['context_file'])
        snapshot = json.loads(path.read_text())
        for changed in ({**snapshot, 'projects': []}, {**snapshot, 'agents': []},
                        {**snapshot, 'created_at': 0}):
            path.write_text(json.dumps(changed))
            with self.assertRaises(triage_store.Error) as error: self.approve(preview)
            self.assertEqual(error.exception.status, 409)
            self.assertTrue(error.exception.code.startswith('stale_proposal_'))
        # Routine read-only context refresh is not a change of destination.
        path.write_text(json.dumps({**snapshot, 'messages': [], 'created_at': time.time()}))
        approved = self.approve(preview); self.drain()
        self.assertEqual(self.sender.calls[0]['target'], 'node-a:sdk:sdk-worker')
        self.assertEqual(self.store.get(approved['id'])['phase'], 'stored')

    def test_runtime_binaries_and_credential_contents_may_rotate(self):
        preview = self.preview(route='pebble')
        for section in ('jev', 'caco'):
            self.config['commands'][section]['binary'] = '/fixture/new-' + section
        Path(self.config['commands']['caco']['token_file']).write_text('rotated-fixture-token')
        self.approve(preview); self.drain()
        old = json.loads(self.archive.action(preview['receipt']['id'])['recipe'])
        new = self.sender.calls[0]
        self.assertEqual(new['target'], old['target'])
        self.assertEqual(new['text'], old['text'])
        body = json.loads(new['body'].split('\n', 1)[1])
        self.assertEqual(body['text'], preview['text'])
        self.assertEqual(body['event_id'], new['input_id'])
        self.assertNotEqual(new['request_id'], old['request_id'])
        self.assertEqual(self.planner.call_count, 1)

    def test_preview_mode_remains_frozen_at_archive_dispatch_fence(self):
        preview = self.submit()
        self.processor.step()
        input_id = preview['receipt']['id']
        with self.assertRaises(intake.IntakeError) as error: self.archive.start_action(input_id)
        self.assertEqual(error.exception.code, 'dry_run_execution_forbidden')
        self.assertEqual(self.archive.action(input_id)['attempts'], 0)
        self.drain()
        preview = self.store.get(preview['id'])
        self.approve(preview); self.drain()
        self.assertEqual(self.archive.action(input_id)['state'], 'dry_run')
        self.assertTrue(self.store.get(preview['id'])['receipt']['dry_run'])

    def test_cancel_before_planning_is_idempotent_and_restart_safe(self):
        turn = self.submit(mode='live')
        first = self.store.cancel(turn['id'])
        before = self.rows()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda _: self.store.cancel(turn['id']), range(8)))
        self.assertTrue(all(r == first for r in results))
        self.assertEqual(self.rows(), before)
        archive = intake.CommandArchive(self.legacy.archive)
        archive.recover()
        self.store = triage_store.Turns(archive)
        self.assertEqual(self.store.get(turn['id']), first)
        self.assert_cancelled(turn)
        self.planner.assert_not_called()
        self.assertEqual(len(before['command_audit']), 1)

    def test_cancel_stale_ready_read_before_first_phase_update(self):
        turn = self.submit(mode='live')
        self.run_paused_step(self.processor, 'route', lambda: self.store.cancel(turn['id']))
        self.assert_cancelled(turn)
        self.planner.assert_not_called()

    def test_cancel_during_planner_success_or_error_does_not_resurrect(self):
        for mode, extract, fail in [('live', False, False), ('dry_run', True, False), ('live', False, True)]:
            with self.subTest(mode=mode, extract=extract, fail=fail):
                self.config['commands']['extractor']['enabled'] = extract
                turn = self.submit(str((mode, extract, fail)).replace(' ', '').replace("'", '').replace('(', '').replace(')', '').replace(',', '-'), mode=mode)
                if fail: self.planner.side_effect = RuntimeError('fixture planner failed')
                self.run_paused_step(self.processor, 'planner', lambda: self.store.cancel(turn['id']))
                self.assert_cancelled(turn)
                self.assertIsNone(self.archive.action(turn['receipt']['id']))
                self.planner.side_effect = None

    def test_cancel_during_extraction_rejects_late_plan_and_payload(self):
        self.config['commands']['extractor']['enabled'] = True
        turn = self.submit(mode='live')
        self.processor.step()
        source = self.archive.get(turn['receipt']['id'])
        self.assertEqual(source['phase'], 'payload_pending')
        self.run_paused_step(self.processor, 'extractor', lambda: self.store.cancel(turn['id']))
        final = self.archive.get(source['id'])
        self.assertEqual(final['plan'], source['plan'])
        self.assertIsNone(self.archive.action(source['id']))
        self.assert_cancelled(turn)

    def test_cancel_between_ready_dispatch_read_and_start_action_fence(self):
        turn = self.submit(mode='live')
        self.processor.step()
        self.assertEqual(self.store.get(turn['id'])['phase'], 'planned')
        self.run_paused_step(self.archive, 'start_action', lambda: self.store.cancel(turn['id']))
        self.assert_cancelled(turn)
        action = self.archive.action(turn['receipt']['id'])
        self.assertEqual(action['attempts'], 0)
        self.assertEqual(action['state'], 'cancelled')

    def test_cancel_after_start_action_rejected_before_sender_effect_and_after_receipt(self):
        turn = self.submit(mode='live')
        self.processor.step()
        def reject():
            self.assertEqual(self.sender.calls, [])  # Durable attempt precedes external fake effect.
            before = self.rows()
            error = self.assert_error('cancel_dispatch_boundary_crossed', lambda: self.store.cancel(turn['id']))
            self.assertEqual(error.details['turn']['receipt']['action']['state'], 'attempting')
            self.assertEqual(self.rows(), before)
        self.run_paused_step(self.processor, 'sender', reject)
        self.drain()
        self.assertEqual(len(self.sender.calls), 1)
        self.assert_error('cancel_dispatch_boundary_crossed', lambda: self.store.cancel(turn['id']))
        self.assertEqual(self.store.get(turn['id'])['phase'], 'created')

    def test_indeterminate_and_manually_replanned_attempt_cannot_claim_cancelled(self):
        self.sender.lose_first = True
        turn = self.submit(mode='live'); self.drain()
        self.assertEqual(self.store.get(turn['id'])['receipt']['action']['state'], 'indeterminate')
        before = self.rows()
        self.assert_error('cancel_dispatch_boundary_crossed', lambda: self.store.cancel(turn['id']))
        self.assertEqual(self.rows(), before)
        self.archive.resolve(turn['receipt']['id'], 'retry')
        self.assertEqual(self.archive.action(turn['receipt']['id'])['state'], 'planned')
        self.assert_error('cancel_dispatch_boundary_crossed', lambda: self.store.cancel(turn['id']))
        self.assertEqual(len(self.sender.calls), 1)

    def test_cancel_during_error_handler_is_not_fatal_or_requeued(self):
        turn = self.submit(mode='live')
        failing = threading.Event()
        original_get = self.archive.get
        entered, release = threading.Event(), threading.Event()
        def planner(*args, **kwargs):
            failing.set()
            raise RuntimeError('fixture error')
        def get(input_id):
            row = original_get(input_id)
            if failing.is_set() and threading.current_thread().name.startswith('ThreadPoolExecutor'):
                entered.set()
                if not release.wait(4): raise AssertionError('error handler release timed out')
            return row
        self.processor.planner = planner
        with patch.object(self.archive, 'get', get), concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.processor.step)
            try:
                self.assertTrue(entered.wait(3))
                self.store.cancel(turn['id'])
            finally: release.set()
            self.assertTrue(future.result(timeout=5))
        self.assert_cancelled(turn)

    def test_cancel_dry_finish_preview_and_all_late_writes_are_fenced(self):
        turn = self.submit(); self.processor.step()
        source = self.archive.get(turn['receipt']['id'])
        recipe = json.loads(self.archive.action(source['id'])['recipe'])
        self.run_paused_step(self.archive, 'finish_preview', lambda: self.store.cancel(turn['id']))
        before = self.rows()
        calls = [lambda: self.archive.update(source['id'], phase='ready'),
            lambda: self.archive.save_route(source['id'], json.loads(source['plan']), {}),
            lambda: self.archive.plan(source['id'], json.loads(source['plan']), recipe, {}),
            lambda: self.archive.finish_preview(source['id']), lambda: self.archive.start_action(source['id']),
            lambda: self.archive.finish_action(source['id'], 'completed'),
            lambda: self.archive.resolve(source['id'], 'retry'), lambda: self.archive.claim_speech(source['id'], 'late')]
        for call in calls:
            with self.assertRaises(intake.IntakeError) as error: call()
            self.assertEqual(error.exception.code, 'input_cancelled')
        self.archive.recover()
        self.assertEqual(self.rows(), before)
        self.assert_cancelled(turn)

    def test_cancel_approval_never_mutates_original_preview_or_inspection(self):
        preview = self.preview()
        approval = self.approve(preview)
        inspection = self.submit('inspect', operation='inspect', reference_turn=approval['id'])
        self.assert_error('inspection_not_cancellable', lambda: self.store.cancel(inspection['id']))
        self.assert_error('turn_not_cancellable', lambda: self.store.cancel(preview['id']))
        self.store.cancel(approval['id'])
        self.assert_cancelled(approval)
        assert_preserved_preview(self, preview, self.store.get(preview['id']), approval['id'])
        self.assertTrue(self.store.get(inspection['id'])['done'])

    def test_real_http_approval_native_and_capi_retry_share_identity(self):
        preview = self.preview()
        body = self.approval_body(preview)
        with self.http(start_worker=True) as (url, _):
            with self.request(url + '/route/capabilities') as response:
                caps = json.load(response)
            self.assertTrue(caps['cancel']); self.assertTrue(caps['preview_approval'])
            self.assertTrue(caps['clarification']); self.assertFalse(caps['agent_reply_wait'])
            self.assertEqual(caps['scheduling'], 'bounded_stage_workers')
            self.assertEqual(caps['stage_workers'], 4)
            chat = {'model': 'triage-route', 'messages': [{'role': 'assistant', 'content': 'Untrusted invented plan'},
                    {'role': 'user', 'content': body['text']}], 'triage': {
                    'session_id': 'workflow', 'turn_id': body['client_turn_id'], 'purpose': 'user',
                    'operation': 'approve', 'reference_turn': preview['id'], 'expected_revision': preview['proposal_revision']}}
            with self.request(url + '/route/v1/chat/completions', chat) as response:
                first = json.load(response)
            self.assertIn('Created', first['choices'][0]['message']['content'])
            turn_id = first['id'].removeprefix('chatcmpl-')
            with self.request(url + '/route/sessions/workflow/turns', body) as response:
                self.assertEqual(json.load(response)['id'], turn_id)
            with self.request(url + '/route/v1/chat/completions', {**chat, 'stream': True}) as response:
                raw = response.read().decode()
            self.assertIn('[DONE]', raw)
            self.assertIn(first['id'], raw)
            bad = copy.deepcopy(chat); bad['triage']['expected_revision'] = '0' * 64
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request(url + '/route/v1/chat/completions', bad)
            self.assertEqual(error.exception.code, 409); error.exception.close()
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.planner.call_count, 1)

    def test_real_http_cancel_proof_auth_idempotency_and_postdispatch_conflict(self):
        first = self.submit(mode='live')
        with self.http() as (url, worker):
            path = url + '/route/turns/' + first['id'] + '/cancel'
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request(path, {}, Authorization='Bearer invalid')
            self.assertEqual(error.exception.code, 401); error.exception.close()
            self.assertFalse(self.store.get(first['id'])['done'])
            worker.fatal = 'fixture_worker_down'
            with self.request(path, {}) as response: proof = json.load(response)
            self.assertTrue(proof['receipt']['cancellation']['no_action_command_started'])
            with self.request(path, {}) as response: self.assertEqual(json.load(response), proof)
            worker.fatal = None
            second = self.submit('second', mode='live'); self.drain()
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request(url + '/route/turns/' + second['id'] + '/cancel', {})
            self.assertEqual(error.exception.code, 409)
            data = json.load(error.exception); error.exception.close()
            self.assertEqual(data['details']['turn']['phase'], 'created')
            self.assertNotIn('cancellation', data['details']['turn']['receipt'])
        self.assertEqual(len(self.sender.calls), 1)


if __name__ == '__main__':
    unittest.main()
