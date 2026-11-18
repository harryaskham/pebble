"""Every retained failure is isolated from later work, without replaying it."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
import test_command_pipeline as fixtures
import dispatch
import intake


class FailureBarrierTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    text = fixtures.PipelineTest.text
    drain = fixtures.PipelineTest.drain

    def test_effect_evidence_matrix(self):
        for state, attempts in [(None, None), ('planned', 0), ('not_started', 0), ('not_started', 1)]:
            self.assertTrue(intake.failed_before_effect(state, attempts), (state, attempts))
        for state, attempts in [('planned', 1), ('not_started', 2), ('indeterminate', 1), ('pending', 1), ('stored', 1), ('created', 1)]:
            self.assertFalse(intake.failed_before_effect(state, attempts), (state, attempts))

    def test_blocked_planned_zero_attempts_retains_error_but_releases_same_lane(self):
        failed=self.text('first request',action='pebble',recorded='9001')
        self.processor.step()
        self.assertEqual(self.archive.action(failed['id'])['attempts'],0)
        self.archive.update(failed['id'],phase='blocked',error='fixture_plan_refused',error_stage='execute')
        before=self.archive.get(failed['id'])
        later=self.text('fresh request',action='pebble',recorded='9002')
        self.drain()
        self.assertEqual(self.archive.get(later['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(failed['id']),before)
        self.assertFalse(self.archive.receipt(failed['id'])['queue_guard']['blocks_following'])
        self.assertEqual([r['input_id'] for r in self.sender.calls],[later['id']])

    def test_first_known_not_started_releases_lane_without_retrying_it(self):
        failed=self.text('failed preflight',action='pebble',recorded='9011')
        def never_started(_): raise dispatch.NotStarted('fixture_capability_missing')
        self.processor.sender=never_started
        self.drain()
        self.assertEqual(self.archive.action(failed['id'])['state'],'not_started')
        self.assertEqual(self.archive.action(failed['id'])['attempts'],1)
        self.processor.sender=self.sender
        later=self.text('fresh same destination',action='pebble',recorded='9012')
        self.drain()
        self.assertEqual(self.archive.get(later['id'])['phase'],'stored')
        self.assertEqual(self.archive.action(failed['id'])['attempts'],1)
        self.assertEqual([r['input_id'] for r in self.sender.calls],[later['id']])

    def test_uncertain_effect_releases_same_lane_without_retrying_old_input(self):
        first=self.text('uncertain message',action='pebble',recorded='9021')
        self.sender.lose_first=True
        self.drain()
        self.assertEqual(self.archive.action(first['id'])['state'],'indeterminate')
        self.assertFalse(self.archive.receipt(first['id'])['queue_guard']['blocks_following'])
        before=self.archive.get(first['id']);old_action=self.archive.action(first['id'])
        self.sender.lose_first=False
        later=self.text('same destination',action='pebble',recorded='9022')
        bead=self.text('independent bead',action='bd-file',recorded='9023')
        self.drain(limit=30)
        self.assertEqual(self.archive.get(later['id'])['phase'],'stored')
        self.assertEqual(self.archive.action(later['id'])['attempts'],1)
        self.assertEqual(self.archive.get(bead['id'])['phase'],'created')
        self.assertEqual(self.archive.get(first['id']),before)
        self.assertEqual(self.archive.action(first['id']),old_action)
        self.assertEqual(len(self.sender.calls),3)
        self.assertIsNone(self.archive.head())
        self.assertIsNone(self.archive.status()['head'])
        self.assertFalse(self.archive.status()['failed_input_barrier'])

    def test_latest_not_started_does_not_erase_prior_uncertain_attempt(self):
        first=self.text('uncertain then failed preflight',action='pebble',recorded='9031')
        self.sender.lose_first=True;self.drain()
        self.archive.resolve(first['id'],'retry')  # Explicit fixture reconciliation path, never production.
        def never_started(_): raise dispatch.NotStarted('fixture_capability_missing')
        self.processor.sender=never_started;self.drain()
        self.assertEqual(self.archive.action(first['id'])['attempts'],2)
        receipt=self.archive.receipt(first['id'])
        self.assertFalse(receipt['queue_guard']['blocks_following'])
        self.assertEqual(receipt['queue_guard']['reason'],'unconfirmed_prior_effect')
        self.processor.sender=self.sender;self.sender.lose_first=False
        later=self.text('fresh input must proceed',action='pebble',recorded='9032');self.drain()
        self.assertEqual(self.archive.get(later['id'])['phase'],'stored')
        self.assertEqual(self.archive.action(later['id'])['attempts'],1)
        self.assertEqual(self.archive.action(first['id'])['attempts'],2)

    def test_failed_action_states_survive_restart_without_reserving_lanes(self):
        for index,(action,state) in enumerate([('pebble','indeterminate'),('agent-msg','stored'),
                                               ('bd-file','pending'),('bd-expand','indeterminate'),
                                               ('broadcast','stored')]):
            with self.subTest(action=action,state=state):
                first=self.text('old retained outcome',action=action,recorded=str(9100+index*2))
                self.processor.step()
                self.archive.start_action(first['id'])
                self.archive.finish_action(first['id'],state,{'fixture':'unconfirmed'},error='fixture_retained')
                self.archive.update(first['id'],phase='blocked')
                original=self.archive.get(first['id']);operation=self.archive.action(first['id'])
                self.archive.recover()
                later=self.text('fresh same destination',action=action,recorded=str(9101+index*2))
                self.drain()
                self.assertIn(self.archive.get(later['id'])['phase'],('stored','created'))
                self.assertEqual(self.archive.action(later['id'])['attempts'],1)
                self.assertEqual(self.archive.get(first['id']),original)
                self.assertEqual(self.archive.action(first['id']),operation)
                self.assertNotIn(first['id'],[r['input_id'] for r in self.sender.calls])
                self.assertFalse(self.archive.receipt(first['id'])['queue_guard']['blocks_following'])

    def test_cli_cancel_raw_input_requires_confirmation_and_keeps_original(self):
        turn=self.text('stale ring request',action='pebble',recorded='9041')
        self.processor.step();before=self.archive.get(turn['id'])
        config=self.root/'cancel-config.json';config.write_text(json.dumps(self.config))
        command=[sys.executable,str(Path(__file__).resolve().parents[1]/'receiver.py'),'--config',str(config),'cancel','--id',turn['id']]
        refused=subprocess.run(command,capture_output=True,text=True,timeout=5)
        self.assertEqual(refused.returncode,2)
        self.assertEqual(self.archive.get(turn['id'])['phase'],'planned')
        result=subprocess.run(command+['--confirmed'],capture_output=True,text=True,timeout=5)
        self.assertEqual(result.returncode,0,result.stderr+result.stdout)
        proof=json.loads(result.stdout)['receipt']
        self.assertEqual(proof['phase'],'skipped')
        self.assertTrue(proof['cancellation']['no_action_command_started'])
        after=self.archive.get(turn['id'])
        for key in ('text','audio','metadata','plan','settings','identity'):
            self.assertEqual(after[key],before[key])
        self.assertEqual(self.sender.calls,[])
