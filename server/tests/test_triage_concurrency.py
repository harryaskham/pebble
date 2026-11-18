"""Real coordinator threads, deterministic barriers, isolated fake effects."""
import contextlib
import copy
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch

import test_command_pipeline as fixtures
import command_pipeline
import intake
import receiver
import triage_store
from process_io import ProcessError


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        fixtures.PipelineTest.setUp(self)
        self.store = triage_store.Turns(self.archive)
        self.store.session('parallel')
        self.calls = []
        self.call_lock = threading.Lock()
        def planner(text, snapshot, action, **kwargs):
            result = fixtures.fake_plan(text, snapshot, action, **kwargs)
            project = 'website' if text.startswith('B') else 'sdk'
            if result['action'] in ('bd-file', 'bd-expand', 'broadcast', 'agent-msg'):
                result['project'] = project
            if result['action'] == 'agent-msg':
                result['target'] = 'node-a:' + project + ':worker'
            if result['action'] == 'broadcast' and text.startswith('G'):
                result.update(scope='global', project=None)
            return result
        self.processor.planner = planner
        self.plan = planner
        self.processor.extractor = lambda plan, _: {'result': {'status': 'ok', 'segments': [plan['text']]}}
        def send(recipe):
            with self.call_lock:
                self.calls.append(recipe['text'])
                return self.sender(recipe)
        self.processor.sender = send
        self.send = send

    def turn(self, text, route='route', mode='live', server=False):
        if server:
            result = self.archive.accept(json.dumps({'text': text, 'client': 'fixture'}).encode(), 'application/json', route)
            return result['id']
        return self.store.submit('parallel', {'client_turn_id': text.replace(' ', '-'), 'text': text, 'route': route, 'mode': mode})['receipt']['id']

    def until(self, predicate, timeout=4):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate(): return
            time.sleep(.01)
        self.fail('bounded condition did not settle')

    def phase(self, input_id):
        return self.archive.get(input_id)['phase']

    @contextlib.contextmanager
    def worker(self, release=None):
        worker = receiver.Worker(self.archive, processor=self.processor)
        worker.thread.start()
        try:
            yield worker
        finally:
            worker.stop.set(); worker.wake.set()
            if release: release.set()
            worker.thread.join(6)
            self.assertFalse(worker.thread.is_alive(), 'coordinator did not join')
            self.assertEqual(worker.activity['active_stages'], 0)
            self.assertFalse(any(t.name.startswith('pebble-stage') and t.is_alive() for t in threading.enumerate()))

    def hold(self, entered, release):
        entered.set()
        if not release.wait(5): raise AssertionError('fixture gate timed out')

    def test_pool_bound_unique_input_claims_and_single_worker_compatibility(self):
        entered, release = threading.Event(), threading.Event()
        active, seen, peak = set(), [], [0]
        lock = threading.Lock()
        def planner(text, *args, **kwargs):
            with lock:
                self.assertNotIn(text, active)
                active.add(text); seen.append(text); peak[0] = max(peak[0], len(active))
                if len(active) == 4: entered.set()
            try:
                if not release.wait(5): raise AssertionError('gate timed out')
                return self.plan(text, *args, **kwargs)
            finally:
                with lock: active.remove(text)
        self.processor.planner = planner
        ids = [self.turn('A' + str(i)) for i in range(8)]
        with self.worker(release) as worker:
            self.assertTrue(entered.wait(3))
            self.assertEqual(worker.stage_workers, 4)
            self.assertEqual(len(seen), 4)
            self.assertEqual(worker.activity['active_stages'], 4)
            release.set()
            self.until(lambda: all(self.phase(i) == 'created' for i in ids))
            self.assertIsNone(worker.fatal)
        self.assertEqual(peak[0], 4)
        self.assertEqual(len(set(seen)), 8)
        self.assertEqual(len(self.calls), 8)
        self.assertTrue(all(self.archive.action(i)['attempts'] == 1 for i in ids))
        self.config['commands']['stage_workers'] = 1
        release.clear(); seen.clear(); entered.clear()
        ids = [self.turn('B' + str(i)) for i in range(2)]
        with self.worker(release):
            self.until(lambda: len(seen) == 1)
            self.assertEqual(self.phase(ids[1]), 'ready')
            release.set()
            self.until(lambda: all(self.phase(i) == 'created' for i in ids))
        for value in (0, 9, True, '4'):
            self.config['commands']['stage_workers'] = value
            with self.assertRaisesRegex(intake.IntakeError, 'invalid_stage_workers'):
                command_pipeline.validate_settings(self.config['commands'])

    def test_stalled_executor_preserves_same_lane_but_other_destination_finishes(self):
        entered, release = threading.Event(), threading.Event()
        def send(recipe):
            if recipe['text'] == 'A-first': self.hold(entered, release)
            return self.send(recipe)
        self.processor.sender = send
        first = self.turn('A-first'); second = self.turn('A-second'); other = self.turn('B-other')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(other) == 'created')
            self.assertEqual(self.calls, ['B-other'])
            self.assertEqual(self.phase(second), 'planned')
            self.assertEqual(self.archive.action(first)['state'], 'attempting')
            release.set()
            self.until(lambda: self.phase(second) == 'created')
        self.assertEqual(self.calls, ['B-other', 'A-first', 'A-second'])

    def test_same_target_waits_only_until_running_send_finishes_even_if_it_fails(self):
        entered, release = threading.Event(), threading.Event()
        def send(recipe):
            if recipe['text'] == 'A-first': self.hold(entered, release)
            result = self.send(recipe)
            if recipe['text'] == 'A-first': raise ProcessError('command_timeout')
            return result
        self.processor.sender = send
        first = self.turn('A-first', route='pebble')
        later = self.turn('A-later', route='pebble')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(later) == 'planned')
            self.assertEqual(self.calls, [])
            release.set()
            self.until(lambda: self.phase(first) == 'blocked' and self.phase(later) == 'stored')
        self.assertEqual(self.calls, ['A-first', 'A-later'])
        self.assertEqual(self.archive.action(first)['state'], 'indeterminate')
        self.assertEqual(self.archive.action(first)['attempts'], 1)
        self.assertFalse(self.archive.receipt(first)['queue_guard']['blocks_following'])
        self.assertEqual(self.archive.action(later)['attempts'], 1)

    def test_failed_unknown_audio_does_not_hold_later_assistant_or_beads(self):
        body, kind = fixtures.legacy_tests.multipart(text='')
        failed = self.archive.accept(body, kind, 'route')['id']
        self.processor.transcriber = lambda _: {'text': '', 'response': {}}
        later = self.turn('B after failed audio')
        assistant = self.turn('A fresh assistant request', route='pebble', server=True)
        with self.worker() as worker:
            self.until(lambda: self.phase(failed) == 'blocked')
            self.until(lambda: self.phase(later) == 'created')
            self.until(lambda: self.phase(assistant) == 'stored')
            self.assertIsNone(worker.fatal)
        self.assertCountEqual(self.calls, ['B after failed audio', 'A fresh assistant request'])
        self.assertIsNone(self.archive.action(failed))

    def test_unknown_planner_blocks_all_live_dispatch_but_not_prep_preview_or_cancel(self):
        entered, release = threading.Event(), threading.Event()
        def planner(text, *args, **kwargs):
            if text == 'A-unknown': self.hold(entered, release)
            return self.plan(text, *args, **kwargs)
        self.processor.planner = planner
        unknown = self.turn('A-unknown'); later = self.turn('B-live'); preview = self.turn('B-preview', mode='dry_run')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(preview) == 'dry_run' and self.phase(later) == 'planned')
            self.assertEqual(self.calls, [])
            with self.archive.db() as db:
                turn = db.execute('SELECT id FROM triage_turns WHERE input_id=?', (unknown,)).fetchone()[0]
            self.store.cancel(turn)
            self.until(lambda: self.phase(later) == 'created')  # Before planner is released!
            self.assertFalse(release.is_set())
            release.set()
            self.until(lambda: self.phase(unknown) == 'skipped')
        self.archive.recover()
        self.assertEqual(self.phase(unknown), 'skipped')
        self.assertIsNone(self.archive.action(unknown))
        self.assertEqual(self.calls, ['B-live'])

    def test_known_extractor_stall_is_not_a_global_dispatch_barrier(self):
        self.config['commands']['extractor']['enabled'] = True
        entered, release = threading.Event(), threading.Event()
        def extract(plan, _):
            if plan['text'] == 'A-extract': self.hold(entered, release)
            return {'result': {'status': 'ok', 'segments': [plan['text']]}}
        self.processor.extractor = extract
        first = self.turn('A-extract'); later = self.turn('A-later'); other = self.turn('B-live')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(other) == 'created')
            self.assertEqual(self.phase(first), 'extracting')
            self.assertNotIn('A-later', self.calls)
            release.set()
            self.until(lambda: self.phase(later) == 'created')
        self.assertEqual(self.calls, ['B-live', 'A-extract', 'A-later'])

    def test_waiting_global_broadcast_propagates_overlap_without_blocking_beads(self):
        entered, release = threading.Event(), threading.Event()
        def send(recipe):
            if recipe['text'] == 'A-message': self.hold(entered, release)
            return self.send(recipe)
        self.processor.sender = send
        self.turn('A-message', 'agent-msg')
        broadcast = self.turn('G-all', 'broadcast')
        later = self.turn('B-message', 'agent-msg')
        bead = self.turn('B-bead', 'bd-file')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(bead) == 'created')
            self.assertNotIn('G-all', self.calls)
            self.assertNotIn('B-message', self.calls)
            release.set()
            self.until(lambda: self.phase(later) == 'stored')
        self.assertLess(self.calls.index('A-message'), self.calls.index('G-all'))
        self.assertLess(self.calls.index('G-all'), self.calls.index('B-message'))
        self.assertEqual(self.phase(broadcast), 'stored')

    def test_custom_lanes_and_preview_bypass_without_real_custom_commands(self):
        self.config['commands']['extractor']['enabled'] = True
        self.config['commands']['actions'] = {name: {'description': name, 'command': ['/fixture/never-run'], 'lane': lane}
                                             for name, lane in [('one', 'same'), ('two', 'same'), ('three', 'other')]}
        entered, release = threading.Event(), threading.Event()
        custom_calls = []
        def custom(recipe, **kwargs):
            if recipe['text'] == 'A-first': self.hold(entered, release)
            with self.call_lock: custom_calls.append(recipe['text'])
            return {'state': 'completed', 'action': recipe['action'], 'exit_code': 0}
        self.processor.custom_sender = custom
        first = self.turn('A-first', 'custom:one'); later = self.turn('A-second', 'custom:two')
        other = self.turn('B-other', 'custom:three'); preview = self.turn('A-preview', 'custom:two', mode='dry_run')
        with self.worker(release):
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(other) == 'completed' and self.phase(preview) == 'dry_run')
            self.assertEqual(custom_calls, ['B-other'])
            release.set()
            self.until(lambda: self.phase(later) == 'completed')
        self.assertEqual(custom_calls, ['B-other', 'A-first', 'A-second'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.archive.action(first)['attempts'], 1)
        self.assertEqual(self.archive.action(preview)['attempts'], 0)

    def test_server_speech_stages_serial_while_client_owned_turn_progresses(self):
        entered, release = threading.Event(), threading.Event()
        original = self.voice.prefix
        def prefix():
            if not entered.is_set(): self.hold(entered, release)
            original()
        self.voice.prefix = prefix
        first = self.turn('A-server1', 'pebble', server=True)
        second = self.turn('A-server2', 'pebble', server=True)
        client = self.turn('B-client', 'bd-file')
        with self.worker(release) as worker:
            self.assertTrue(entered.wait(3))
            self.until(lambda: self.phase(client) == 'created')
            self.assertTrue(worker.activity['server_speech_active'])
            self.assertEqual(self.phase(second), 'ready')
            self.assertEqual(self.voice.calls, [])
            release.set()
            self.until(lambda: self.phase(second) == 'stored')
        self.assertEqual(self.phase(first), 'stored')
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 2)
        self.assertEqual(self.calls, ['B-client', 'A-server1', 'A-server2'])

    def test_pool_cancel_before_and_after_dispatch_fence_preserves_truth(self):
        for before in (True, False):
            with self.subTest(before=before):
                entered, release = threading.Event(), threading.Event()
                input_id = self.turn('A-fence-' + str(before))
                with self.archive.db() as db:
                    turn_id = db.execute('SELECT id FROM triage_turns WHERE input_id=?', (input_id,)).fetchone()[0]
                owner, method = (self.archive, 'start_action') if before else (self.processor, 'sender')
                original = getattr(owner, method)
                def paused(*args, **kwargs):
                    self.hold(entered, release)
                    return original(*args, **kwargs)
                with patch.object(owner, method, paused), self.worker(release) as worker:
                    self.assertTrue(entered.wait(3))
                    if before:
                        result = self.store.cancel(turn_id)
                        self.assertTrue(result['receipt']['cancellation']['no_action_command_started'])
                    else:
                        with self.assertRaises(triage_store.Error) as error: self.store.cancel(turn_id)
                        self.assertEqual(error.exception.code, 'cancel_dispatch_boundary_crossed')
                        self.assertEqual(error.exception.details['turn']['receipt']['action']['state'], 'attempting')
                    release.set()
                    self.until(lambda: self.phase(input_id) == ('skipped' if before else 'created'))
                    self.assertIsNone(worker.fatal)
                self.assertEqual(self.archive.action(input_id)['attempts'], 0 if before else 1)
        self.assertEqual(self.calls, ['A-fence-False'])

    def test_stop_after_selection_prevents_launch_and_leaves_durable_work(self):
        entered, release = threading.Event(), threading.Event()
        item = self.turn('A-stop')
        select = self.archive.next_work
        def next_work(*args, **kwargs):
            record = select(*args, **kwargs)
            if record: self.hold(entered, release)
            return record
        with patch.object(self.archive, 'next_work', next_work), self.worker(release) as worker:
            self.assertTrue(entered.wait(3))
            worker.stop.set(); worker.wake.set(); release.set()
            worker.thread.join(3)
            self.assertEqual(self.phase(item), 'ready')
            self.assertEqual(self.calls, [])

    def test_shutdown_joins_active_stages_no_new_jobs_and_restart_resumes(self):
        entered, release = threading.Event(), threading.Event()
        count = [0]
        def planner(*args, **kwargs):
            with self.call_lock:
                count[0] += 1
                if count[0] == 2: entered.set()
            if not release.wait(5): raise AssertionError('gate timed out')
            return self.plan(*args, **kwargs)
        self.processor.planner = planner
        a = self.turn('A-stop'); b = self.turn('B-stop')
        with self.worker(release) as worker:
            self.assertTrue(entered.wait(3))
            worker.stop.set(); worker.wake.set()
            c = self.turn('B-after-stop')
            release.set(); worker.thread.join(3)
            self.assertEqual(count[0], 2)
            self.assertEqual(self.calls, [])
            self.assertEqual(self.phase(c), 'ready')
        self.archive.recover()
        with self.worker():
            self.until(lambda: all(self.phase(i) == 'created' for i in (a, b, c)))
        self.assertEqual(count[0], 3)  # Frozen planned stages were not rerouted.
        self.assertEqual(len(self.calls), 3)

    def test_fatal_future_stops_claims_and_drains_other_active_stage(self):
        entered, release, fail = threading.Event(), threading.Event(), threading.Event()
        def planner(text, *args, **kwargs):
            if text == 'A-active': self.hold(entered, release)
            else:
                if not entered.wait(3): raise AssertionError('active stage absent')
                fail.set()
                raise sqlite3.OperationalError('fixture persistence failure')
            return self.plan(text, *args, **kwargs)
        self.processor.planner = planner
        active = self.turn('A-active'); self.turn('B-fatal')
        with self.worker(release) as worker:
            self.assertTrue(fail.wait(3))
            self.until(lambda: worker.fatal is not None)
            self.assertTrue(worker.stop.is_set())
            pending = self.turn('B-pending')
            self.assertEqual(self.phase(pending), 'ready')
            release.set(); worker.thread.join(3)
            self.assertEqual(self.phase(active), 'planned')
            self.assertEqual(self.calls, [])

    def test_caco_clients_are_thread_local_without_serializing_execution(self):
        entered, release = threading.Barrier(2), threading.Event()
        clients = []
        outer = self
        class FakeCaco:
            def __init__(self, **kwargs):
                self.owner = threading.get_ident()
                clients.append(self)
            def execute(self, recipe):
                outer.assertEqual(self.owner, threading.get_ident())
                entered.wait(timeout=3)
                release.set()
                return outer.send(recipe)
        self.processor.sender = None
        a = self.turn('A-cache'); b = self.turn('B-cache')
        with patch.object(command_pipeline.dispatch, 'Caco', FakeCaco), self.worker():
            self.assertTrue(release.wait(4), 'client calls serialized or shared')
            self.until(lambda: self.phase(a) == self.phase(b) == 'created')
        self.assertEqual(len(clients), 2)
        self.assertEqual(len({c.owner for c in clients}), 2)

    def test_process_death_releases_owner_and_recovers_parallel_inflight_stages(self):
        first = self.turn('A-dispatched')
        self.processor.step()  # Prepare first, leaving second available for parallel routing.
        second = self.turn('B-routing')
        config = self.root / 'child-config.json'
        config.write_text(json.dumps(self.config))
        source = str(fixtures.ROOT)
        script = '''import fcntl,json,sys,time
from pathlib import Path
import receiver,intake,command_pipeline
config=json.loads(Path(sys.argv[1]).read_text()); root=Path(sys.argv[2])
archive=intake.CommandArchive(receiver.Archive(config))
def planner(*args,**kwargs):
 (root/'routing-started').write_text('fixture'); time.sleep(30)
def sender(recipe):
 (root/'dispatch-started').write_text(recipe['request_id']); time.sleep(30)
with (archive.root/'worker.lock').open('a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 archive.recover()
 processor=command_pipeline.Processor(archive,lambda _:None,planner=planner,sender=sender,
  resolver=lambda *_: {'source':'fixture','answers':{'intent':{'choice':'new'}}})
 worker=receiver.Worker(archive,processor=processor); worker.thread.start(); worker.thread.join()
'''
        child = subprocess.Popen([sys.executable, '-c', script, str(config), str(self.root)],
                                 env={**os.environ, 'PYTHONPATH': source, 'PYTHONDONTWRITEBYTECODE': '1'},
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.until(lambda: (self.root / 'routing-started').exists() and (self.root / 'dispatch-started').exists())
            child.kill(); child.wait(timeout=3)
        finally:
            if child.poll() is None: child.kill(); child.wait(timeout=3)
        with (self.archive.root / 'worker.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.archive.recover()
            self.assertEqual(self.phase(second), 'ready')
            self.assertEqual(self.archive.action(first)['state'], 'indeterminate')
            with self.worker():
                self.until(lambda: self.phase(first) == 'blocked' and self.phase(second) == 'created')
        self.assertEqual(self.calls, ['B-routing'])
        self.assertEqual(self.archive.action(first)['attempts'], 1)


if __name__ == '__main__': unittest.main()
