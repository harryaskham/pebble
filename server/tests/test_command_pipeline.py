import contextlib
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import command_pipeline
import dispatch
import intake
import jev_router
import receiver
from process_io import ProcessError, Result
import test_receiver as legacy_tests
from test_jev_router import context as fixture_context

ROOT = Path(__file__).resolve().parents[1]


class Voice:
    self_voice = 'fixture-self-voice'

    def __init__(self):
        self.calls = []
        self.fail = False

    def record(self, *entry):
        self.calls.append(entry)
        if self.fail:
            raise ProcessError('command_unavailable')

    def prefix(self): self.record('prefix')
    def speak(self, text, voice=None): self.record('speak', text, voice)
    def routing(self, action): self.record('routing', action)
    def beads_created(self, beads): self.record('beads', beads)
    def message_stored(self, target, admission): self.record('message', target, admission)


def fake_plan(text, snapshot, action, **kwargs):
    selected = 'bd-file' if action == 'route' else action
    plan = {'version': 1, 'action': selected, 'text': text, 'executed': False, 'decision': {'source': 'fixture'}}
    if selected in ('bd-file', 'bd-expand'):
        plan.update(project='sdk', priority=0)
    if selected == 'bd-file':
        plan.update(title=' '.join(text[:80].split()), description=text, labels=['from-pebble'])
    if selected == 'agent-msg':
        plan.update(project='sdk', target='node-a:sdk:sdk-worker', body=text)
    if selected == 'broadcast':
        plan.update(scope='project', project='sdk', body=text)
    return plan


class Sender:
    def __init__(self):
        self.calls, self.effects = [], {}
        self.lose_first = False
        self.pending = False
        self.admission = {'storage_state': 'stored', 'runtime_delivery_state': 'pending_confirmation', 'pi_admission_state': 'unavailable'}

    def __call__(self, recipe):
        self.calls.append(copy.deepcopy(recipe))
        key = recipe['request_id']
        if key not in self.effects:
            if recipe['action'] in ('pebble', 'agent-msg'):
                data = {'id': 'msg-fixture-' + recipe['input_id'], 'target': recipe['target'], 'admission': self.admission.copy()}
            elif recipe['action'] == 'broadcast':
                data = {'id': 'msg-fixture-' + recipe['input_id'], 'kind': 'broadcast', 'scope': recipe['scope'],
                        'project': recipe['project'] or '', 'target': None}
            elif self.pending:
                data = {'status': 'queued', 'request_id': key}
            elif recipe['action'] == 'bd-file':
                data = {'id': f'bd-{len(self.effects)+1:06d}', 'title': recipe['title']}
            else:
                data = {'created': [{'id': 'bd-expand1', 'title': 'First expanded bead'}, {'id': 'bd-expand2', 'title': 'Second expanded bead'}], 'count': 2}
            self.effects[key] = dispatch.receipt(recipe, {'ok': True, 'data': data})
            if self.lose_first:
                raise ProcessError('command_timeout')
        return copy.deepcopy(self.effects[key])


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.legacy = legacy_tests.ReceiverTest('runTest')
        self.legacy.setUp()
        self.addCleanup(self.legacy.tearDown)
        self.config, self.root = self.legacy.config, self.legacy.root
        self.config['commands'] = json.loads((ROOT / 'tests/fixtures/commands-caco.json').read_text())
        command = self.config['commands']
        command['extractor']['enabled'] = False  # Legacy/no-extraction coverage; dedicated tests enable it.
        command.update(enabled=True, context_file=str(self.root / 'context.json'), legacy_text_archive='')
        command['speech']['cache_dir'] = str(self.root / 'speech')
        command['caco']['token_file'] = str(self.root / 'webhook-token')
        snapshot = fixture_context()
        snapshot['created_at'] = time.time()
        Path(command['context_file']).write_text(json.dumps(snapshot))
        self.archive = intake.CommandArchive(self.legacy.archive)
        self.voice, self.sender = Voice(), Sender()
        self.transcriptions = []
        def transcriber(record):
            self.transcriptions.append(record)
            return {'text': 'MAI words, not device words', 'response': {'fixture': True}}
        self.processor = command_pipeline.Processor(self.archive, transcriber, planner=fake_plan,
                                                   sender=self.sender, feedback_factory=lambda settings: self.voice,
                                                   resolver=lambda *_: {'source': 'fixture', 'answers': {'intent': {'choice': 'new'}}})

    def text(self, text='File a p0 SDK bug', *, action='route', recorded='1770000000001', key=None, **extra):
        body = json.dumps({'transcription': text, 'recordedAt': recorded, 'client': 'ring', **extra}).encode()
        return self.archive.accept(body, 'application/json', action, key)

    def drain(self, limit=12):
        for _ in range(limit):
            if not self.processor.step():
                return
        self.fail('processor did not settle within bounded fixture steps')

    @contextlib.contextmanager
    def http(self, start_worker=False):
        worker = receiver.Worker(self.archive, processor=self.processor)
        server = receiver.Server(self.archive, worker, ('127.0.0.1', 0), ingestor=self.archive.accept, routes=command_pipeline.ROUTES)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        if start_worker:
            worker.thread.start()
        try:
            yield f'http://127.0.0.1:{server.server_port}', worker
        finally:
            server.shutdown()
            server.server_close()
            worker.stop.set()
            worker.wake.set()
            if worker.thread.is_alive(): worker.thread.join(10)
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertFalse(worker.thread.is_alive())
            with self.assertRaises(OSError): socket.create_connection(server.server_address, timeout=.1)

    def post(self, url, body, kind='application/json', key=None, token=None):
        headers = {'Authorization': 'Bearer ' + (token or self.legacy.token), 'Content-Type': kind}
        if key:
            headers['Idempotency-Key'] = key
        request = urllib.request.Request(url, data=body, headers=headers)
        with urllib.request.urlopen(request, timeout=4) as response:
            return response.status, json.load(response)

    def test_shared_parser_keeps_text_verbatim_and_accepts_audio_without_device_text(self):
        text = "  don't run $(anything); keep ‘these’ words\nverbatim  "
        metadata, audio = intake.parse_input(json.dumps({'transcript': text}).encode(), 'application/json')
        self.assertEqual(metadata['transcription'], text)
        self.assertIsNone(audio)
        metadata, audio = intake.parse_input(*legacy_tests.multipart(text=''))
        self.assertTrue(audio)
        self.assertEqual(metadata['transcription'], '')

    def test_index_probes_do_not_mutate_or_speak_in_both_modes(self):
        before = self.archive.status()
        for flag in (True, 'true'):
            body = json.dumps({'test': flag, 'transcription': 'fixed connection test'}).encode()
            self.assertEqual(self.archive.accept(body, 'application/json')['phase'], 'probe')
            old = receiver.admit(self.legacy.archive, body, 'application/json', sound=lambda _: self.fail('no chime for probes'))
            self.assertEqual(old['phase'], 'probe')
        self.assertEqual(self.archive.status(), before)
        self.assertEqual(self.voice.calls, [])
        self.assertEqual(self.sender.calls, [])

    def test_all_six_endpoints_accept_text_and_raw_audio_through_one_path(self):
        with self.http() as (url, _):
            for index, (path, action) in enumerate(command_pipeline.ROUTES.items()):
                if path == '/': continue
                body = json.dumps({'transcription': 'explicit request', 'recordedAt': 100 + index}).encode()
                status, result = self.post(url + path, body)
                self.assertEqual(status, 202)
                self.assertEqual(self.archive.get(result['id'])['endpoint_action'], action)
                raw, kind = legacy_tests.multipart(recorded=str(200 + index))
                status, result = self.post(url + path + '/', raw, kind)
                self.assertEqual(status, 202)
                self.assertEqual(self.archive.get(result['id'])['audio'], legacy_tests.wav_bytes())
            status, probe = self.post(url + '/hooks/global/pebble-bd-file', b'{"test":true}')
            self.assertEqual(status, 200)
            self.assertFalse(probe['archived'])
            before = self.archive.status()['requests']
            with self.assertRaises(urllib.error.HTTPError) as denied:
                self.post(url + '/hooks/global/pebble', b'{"text":"no"}', token='wrong')
            self.assertEqual(denied.exception.code, 401)
            denied.exception.close()
            self.assertEqual(self.archive.status()['requests'], before)

    def test_http_receipt_is_durable_before_slow_transcription(self):
        entered, release = threading.Event(), threading.Event()
        def slow(record):
            entered.set()
            release.wait(5)
            return {'text': 'MAI authoritative transcript', 'response': {}}
        self.processor.transcriber = slow
        try:
            with self.http(start_worker=True) as (url, _):
                raw, kind = legacy_tests.multipart()
                status, result = self.post(url + '/hooks/global/pebble-audio', raw, kind)
                self.assertEqual(status, 202)
                self.assertEqual(self.archive.get(result['id'])['audio'], legacy_tests.wav_bytes())
                self.assertTrue(entered.wait(3))
                self.assertEqual(self.sender.calls, [])
                self.assertEqual(self.voice.calls, [('prefix',)])
                self.assertEqual(self.archive.get(result['id'])['phase'], 'transcribing')
                release.set()
                deadline = time.monotonic() + 5
                while self.archive.get(result['id'])['phase'] != 'created' and time.monotonic() < deadline:
                    time.sleep(.01)
                req = urllib.request.Request(url + '/receipts/' + result['id'], headers={'Authorization': 'Bearer ' + self.legacy.token})
                with urllib.request.urlopen(req, timeout=3) as response:
                    receipt = json.load(response)
                self.assertEqual(receipt['phase'], 'created')
                self.assertEqual(receipt['action']['attempts'], 1)
        finally:
            release.set()

    def test_duplicate_audio_ignores_boundary_and_device_transcript_changes(self):
        first = self.archive.accept(*legacy_tests.multipart(), 'bd-file')
        self.drain()
        second = self.archive.accept(*legacy_tests.multipart(boundary='another', text='revised device words'), 'bd-file')
        self.drain()
        self.assertEqual(first['id'], second['id'])
        self.assertTrue(second['duplicate'])
        self.assertEqual(len(self.transcriptions), 1)
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
        self.assertIn(('speak', 'MAI words, not device words', self.voice.self_voice), self.voice.calls)
        self.assertEqual(self.sender.calls[0]['description'], 'MAI words, not device words')
        self.assertEqual(self.archive.status()['requests'], 2)

    def test_key_conflict_and_route_conflict_archive_but_do_not_retarget(self):
        first = self.text('original', action='bd-file', key='stable-identity')
        second = self.text('changed', action='agent-msg', key='stable-identity')
        self.assertEqual(second['error'], 'input_identity_conflict')
        self.assertEqual(first['id'], second['id'])
        self.assertEqual(self.archive.get(first['id'])['text'], 'original')
        self.assertEqual(self.archive.get(first['id'])['endpoint_action'], 'bd-file')
        self.drain()
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.sender.calls[0]['description'], 'original')

    def test_legacy_audio_is_retained_never_recovered_or_replayed(self):
        old = self.legacy.archive.accept(*legacy_tests.multipart())
        self.legacy.archive.update(old['id'], phase='forwarding', forward_body=b'old frozen body')
        result = self.archive.accept(*legacy_tests.multipart(), 'route')
        self.assertEqual(result['phase'], 'retained')
        self.archive.recover()
        self.drain()
        self.assertEqual(self.legacy.archive.get(old['id'])['phase'], 'forwarding')
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.voice.calls, [])
        self.assertEqual(json.loads(self.archive.get(result['id'])['legacy_reference'])['id'], old['id'])

    def test_legacy_text_identity_is_retained_without_interpreting_delivered_as_executed(self):
        import hashlib
        source = self.root / 'legacy-text.sqlite3'
        metadata = {'transcription': 'old words', 'recordedAt': '1770000000001', 'client': 'ring'}
        identity = hashlib.sha256(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
        with contextlib.closing(sqlite3.connect(source)) as db:
            with db:
                db.execute('CREATE TABLE jobs(id TEXT, dedupe TEXT, stage TEXT)')
                db.execute('INSERT INTO jobs VALUES(?,?,?)', ('old-job', identity, 'delivered'))
        self.config['commands']['legacy_text_archive'] = str(source)
        result = self.text('old words')
        self.assertEqual(result['phase'], 'retained')
        self.drain()
        self.assertEqual(self.sender.calls, [])
        reference = json.loads(self.archive.get(result['id'])['legacy_reference'])
        self.assertEqual(reference['observed_phase'], 'delivered')
        self.assertEqual(self.archive.get(result['id'])['phase'], 'retained')

    def test_legacy_lookup_failure_still_archives_and_blocks(self):
        source = self.root / 'wrong-legacy-database'
        source.write_bytes(b'not a database')
        self.config['commands']['legacy_text_archive'] = str(source)
        result = self.text()
        record = self.archive.get(result['id'])
        self.assertEqual(record['phase'], 'blocked')
        self.assertEqual(record['error_stage'], 'legacy_check')
        self.assertEqual(self.archive.status()['requests'], 1)
        self.drain()
        self.assertEqual(self.sender.calls, [])
        with self.assertRaises(sqlite3.Error): self.archive.resolve(result['id'], 'retry')

    def test_speech_failure_never_discards_text_or_repeats_ack(self):
        self.voice.fail = True
        result = self.text('keep this verbatim')
        self.drain()
        self.archive.recover()
        self.text('keep this verbatim')
        self.drain()
        record = self.archive.receipt(result['id'])
        self.assertEqual(record['phase'], 'created')
        self.assertTrue(all(item['state'] == 'failed' for item in record['speech']))
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
        self.assertEqual(sum(call[:2] == ('speak', 'keep this verbatim') for call in self.voice.calls), 1)
        self.assertEqual(len(self.sender.calls), 1)

    def test_blocked_action_does_not_silence_or_block_new_same_project_input(self):
        self.sender.lose_first = True
        first = self.text('first uncertain action', action='bd-file')
        self.drain()
        before = self.archive.receipt(first['id'])
        self.sender.lose_first = False
        second = self.text('second archived request', action='bd-file', recorded='444')
        self.drain()
        self.assertEqual(self.archive.get(second['id'])['phase'], 'created')
        self.assertEqual(self.archive.action(second['id'])['attempts'], 1)
        self.assertFalse(any(call[0] == 'speak' and 'this destination is waiting' in call[1] for call in self.voice.calls))
        self.assertEqual(self.archive.receipt(first['id']), before)
        self.assertEqual([r['input_id'] for r in self.sender.calls], [first['id'], second['id']])
        self.assertEqual(sum(call[:2] == ('speak', 'second archived request') for call in self.voice.calls), 1)

    def test_failed_agent_does_not_block_later_same_or_other_agent(self):
        self.sender.admission['runtime_delivery_state'] = 'rejected'
        first = self.text('first agent unavailable', action='agent-msg')
        self.drain()
        self.assertEqual(self.archive.get(first['id'])['phase'], 'blocked')
        self.sender.admission['runtime_delivery_state'] = 'pending_confirmation'
        def planner(text, snapshot, action, **kwargs):
            result = fake_plan(text, snapshot, action, **kwargs)
            if text == 'other recipient':
                result.update(project='website', target='node-b:website:site-worker')
            return result
        self.processor.planner = planner
        bead = self.text('independent bead', action='bd-file', recorded='502')
        other = self.text('other recipient', action='agent-msg', recorded='503')
        same = self.text('same recipient proceeds', action='agent-msg', recorded='504')
        self.drain(limit=24)
        self.assertEqual(self.archive.get(bead['id'])['phase'], 'created')
        self.assertEqual(self.archive.get(other['id'])['phase'], 'stored')
        self.assertEqual(self.archive.get(same['id'])['phase'], 'stored')
        self.assertEqual(self.archive.action(same['id'])['attempts'], 1)
        self.assertEqual(len(self.sender.calls), 4)
        self.assertEqual(self.archive.get(first['id'])['phase'], 'blocked')
        self.assertEqual(self.archive.action(first['id'])['attempts'], 1)
        self.assertEqual(self.archive.receipt(same['id'])['action']['target'], 'node-a:sdk:sdk-worker')

    def test_failed_unknown_audio_does_not_hold_assistant_or_bead_work(self):
        first = self.archive.accept(*legacy_tests.multipart(), 'route')
        self.processor.transcriber = lambda record: (_ for _ in ()).throw(receiver.Failure('mai_http_403'))
        self.drain()
        assistant = self.text('status', action='pebble', recorded='602')
        bead = self.text('independent bead', action='bd-file', recorded='603')
        self.drain(limit=24)
        self.assertEqual(self.archive.get(first['id'])['phase'], 'blocked')
        self.assertEqual(self.archive.get(first['id'])['error'], 'mai_http_403')
        self.assertIsNone(self.archive.action(first['id']))
        self.assertFalse(self.archive.receipt(first['id'])['queue_guard']['blocks_following'])
        self.assertEqual(self.archive.get(assistant['id'])['phase'], 'stored')
        self.assertEqual(self.archive.action(assistant['id'])['attempts'], 1)
        self.assertEqual(self.archive.get(bead['id'])['phase'], 'created')
        self.assertEqual(len(self.sender.calls), 2)

    def test_recording_acknowledgement_precedes_transcription_and_is_not_repeated(self):
        result = self.archive.accept(*legacy_tests.multipart(), 'pebble')
        transcribe = self.processor.transcriber
        def observed_transcribe(record):
            self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
            self.assertEqual(self.archive.get(result['id'])['phase'], 'transcribing')
            speech = self.archive.receipt(result['id'])['speech']
            self.assertEqual(next(item['state'] for item in speech if item['kind'] == 'ack-prefix'), 'played')
            return transcribe(record)
        self.processor.transcriber = observed_transcribe
        self.drain()
        self.assertEqual(self.archive.get(result['id'])['phase'], 'stored')
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
        self.assertEqual(sum(call[:2] == ('speak', 'MAI words, not device words') for call in self.voice.calls), 1)

    def test_transcription_failure_keeps_the_early_ack_claim_across_explicit_retry(self):
        result = self.archive.accept(*legacy_tests.multipart(), 'pebble')
        transcribe = self.processor.transcriber
        self.processor.transcriber = lambda _: (_ for _ in ()).throw(receiver.Failure('fixture_stt_failure'))
        self.drain()
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
        self.archive.resolve(result['id'], 'retry')
        self.processor.transcriber = transcribe
        self.drain()
        self.assertEqual(self.archive.get(result['id'])['phase'], 'stored')
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)

    def test_interrupted_readback_is_not_replayed(self):
        result = self.text()
        self.archive.claim_speech(result['id'], 'ack-prefix')
        self.archive.finish_speech(result['id'], 'ack-prefix', 'played')
        self.archive.claim_speech(result['id'], 'readback')
        self.archive.update(result['id'], phase='routing')
        self.archive.recover()
        self.drain()
        self.assertFalse(any(call[0] in ('prefix', 'speak') for call in self.voice.calls))
        self.assertEqual(next(x for x in self.archive.receipt(result['id'])['speech'] if x['kind'] == 'readback')['state'], 'interrupted')

    def test_lost_receipt_stops_and_exact_retry_keeps_one_effect(self):
        self.sender.lose_first = True
        result = self.text('one bead only', action='bd-file')
        self.drain()
        first = self.archive.receipt(result['id'])
        self.assertEqual(first['phase'], 'blocked')
        self.assertEqual(first['action']['state'], 'indeterminate')
        recipe = self.archive.action(result['id'])['recipe']
        self.archive.recover()
        self.drain()
        self.assertEqual(len(self.sender.calls), 1)
        self.archive.resolve(result['id'], 'retry')
        self.drain()
        self.assertEqual(self.archive.action(result['id'])['recipe'], recipe)
        self.assertEqual(len(self.sender.calls), 2)
        self.assertEqual(len(self.sender.effects), 1)
        self.assertEqual(self.archive.receipt(result['id'])['phase'], 'created')
        self.assertEqual(sum(call[0] == 'prefix' for call in self.voice.calls), 1)
        self.assertEqual(sum(call[0] == 'routing' for call in self.voice.calls), 1)
        self.assertEqual(sum(call[0] == 'beads' for call in self.voice.calls), 1)

    def test_interrupted_dispatch_is_indeterminate_not_automatically_reissued(self):
        result = self.text(action='agent-msg')
        self.processor.step()  # plan committed
        recipe = self.archive.start_action(result['id'])
        self.sender(recipe)  # external success, then process dies before receipt
        self.archive.recover()
        self.drain()
        receipt = self.archive.receipt(result['id'])
        self.assertEqual(receipt['action']['state'], 'indeterminate')
        self.assertEqual(receipt['phase'], 'blocked')
        self.assertEqual(len(self.sender.calls), 1)
        self.assertEqual(self.archive.action(result['id'])['recipe'], json.dumps(recipe, ensure_ascii=False))

    def test_uncertain_expand_can_never_be_retried_as_a_new_expansion(self):
        self.sender.lose_first = True
        result = self.text('Expand SDK tasks', action='bd-expand')
        self.drain()
        self.assertEqual(self.archive.receipt(result['id'])['action']['state'], 'indeterminate')
        with self.assertRaisesRegex(intake.IntakeError, 'authoritative_reconciliation_no_replay'):
            self.archive.resolve(result['id'], 'retry')
        self.archive.recover()
        self.drain()
        self.assertEqual(len(self.sender.effects), 1)
        self.assertEqual(len(self.sender.calls), 1)

    def test_expand_not_started_can_be_retried_without_inventing_a_receipt(self):
        result = self.text('Expand SDK tasks', action='bd-expand')
        original = self.processor.sender
        self.processor.sender = lambda recipe: (_ for _ in ()).throw(ProcessError('command_unavailable'))
        self.drain()
        self.assertEqual(self.archive.receipt(result['id'])['action']['state'], 'not_started')
        self.archive.resolve(result['id'], 'retry')
        self.processor.sender = original
        self.drain()
        self.assertEqual(len(self.sender.effects), 1)
        self.assertEqual(len(self.archive.receipt(result['id'])['action']['beads']), 2)

    def test_device_text_is_not_a_fallback_after_failed_audio_transcription(self):
        result = self.archive.accept(*legacy_tests.multipart(text='tempting wrong device text'), 'bd-file')
        self.processor.transcriber = lambda record: (_ for _ in ()).throw(receiver.Failure('mai_http_403'))
        self.drain()
        self.assertEqual(self.archive.get(result['id'])['phase'], 'blocked')
        self.assertIsNone(self.archive.get(result['id'])['text'])
        self.assertEqual(self.sender.calls, [])
        self.assertFalse(any('tempting' in str(call) for call in self.voice.calls))

    def test_jev_failure_on_fixed_endpoint_retains_and_automatic_falls_back_explicitly(self):
        self.processor.planner = lambda *a, **kw: (_ for _ in ()).throw(jev_router.RouterError('jev_command_timeout'))
        fixed = self.text(action='bd-file')
        self.drain()
        self.assertEqual(self.archive.get(fixed['id'])['phase'], 'blocked')
        self.assertEqual(self.sender.calls, [])
        self.archive.resolve(fixed['id'], 'skip')
        automatic = self.text('find and attach to the SDK agent', action='route', recorded='222')
        self.drain()
        recipe = self.sender.calls[0]
        self.assertEqual(recipe['action'], 'pebble')
        self.assertEqual(json.loads(recipe['body'].split('\n', 1)[1])['text'], 'find and attach to the SDK agent')
        self.assertEqual(json.loads(self.archive.get(automatic['id'])['plan'])['fallback_reason'], 'jev_command_timeout')

    def test_queued_bead_creation_and_rejected_delivery_are_not_success_claims(self):
        self.sender.pending = True
        bead = self.text(action='bd-file')
        self.drain()
        receipt = self.archive.receipt(bead['id'])
        self.assertEqual((receipt['phase'], receipt['action']['state']), ('blocked', 'pending'))
        self.assertEqual(receipt['action']['beads'], [])
        self.assertFalse(any(call[0] == 'beads' for call in self.voice.calls))
        self.archive.resolve(bead['id'], 'skip')
        self.sender.admission['runtime_delivery_state'] = 'rejected'
        message = self.text(action='agent-msg', recorded='333')
        self.drain()
        receipt = self.archive.receipt(message['id'])
        self.assertEqual((receipt['phase'], receipt['action']['state']), ('blocked', 'stored'))
        self.assertEqual(receipt['action']['admission']['runtime_delivery_state'], 'rejected')
        with self.assertRaises(intake.IntakeError): self.archive.resolve(message['id'], 'retry')

    def test_frozen_decision_survives_config_and_context_changes(self):
        result = self.text(action='agent-msg')
        self.processor.step()
        recipe = self.archive.action(result['id'])['recipe']
        Path(self.config['commands']['context_file']).write_text('broken later context')
        self.config['commands']['pebble_target'] = 'different:project:agent'
        self.config['commands']['pebble_project'] = 'project'
        self.processor.planner = lambda *a, **kw: self.fail('do not reroute a committed plan')
        self.archive.recover()
        self.drain()
        self.assertEqual(self.archive.action(result['id'])['recipe'], recipe)
        self.assertEqual(self.sender.calls[0]['target'], 'node-a:sdk:sdk-worker')
        self.assertEqual(self.archive.get(result['id'])['phase'], 'stored')

    def test_export_preserves_raw_input_and_decision_without_bearer_secrets(self):
        raw, kind = legacy_tests.multipart(audio=b'\0\xff\x80binary')
        result = self.archive.accept(raw, kind, 'bd-file')
        self.drain()
        destination = self.root / 'command-export'
        self.archive.export(result['id'], destination)
        self.assertEqual((destination / 'audio.wav').read_bytes(), b'\0\xff\x80binary')
        self.assertEqual((destination / 'request-1.body').read_bytes(), raw)
        for path in destination.iterdir():
            self.assertNotIn(self.legacy.token.encode(), path.read_bytes())
        self.assertIn('recipe', json.loads((destination / 'action.json').read_text()))

    def test_config_keeps_valid_managed_symlinks_and_rejects_unknown_settings(self):
        config = self.root / 'commands.json'
        config.write_text(json.dumps(self.config))
        config.chmod(0o666)
        link = self.root / 'commands-link.json'
        link.symlink_to(config)
        self.assertEqual(receiver.load_config(link), self.config)
        with patch.dict(os.environ, {'PEBBLE_UI_SPEED': '1.7', 'PEBBLE_SELF_EMBEDDING': 'fixture-profile'}):
            effective = receiver.load_config(link)
        self.assertEqual(effective['commands']['speech']['voices']['PEBBLE_UI_SPEED'], '1.7')
        self.assertEqual(effective['commands']['speech']['voices']['PEBBLE_SELF_EMBEDDING'], 'fixture-profile')
        invalid = copy.deepcopy(self.config)
        invalid['commands']['arbitrary_shell'] = 'not a setting'
        config.write_text(json.dumps(invalid))
        with self.assertRaises(intake.IntakeError): receiver.load_config(link)


class DispatchTest(unittest.TestCase):
    def setUp(self):
        self.settings = json.loads((ROOT / 'tests/fixtures/commands-caco.json').read_text())
        self.input_id = 'a' * 32

    def test_actual_supported_argv_and_original_payload(self):
        text = "don't evaluate $(touch never);\nthis is data"
        for action in ('pebble', 'agent-msg', 'bd-file', 'bd-expand'):
            recipe = dispatch.prepare(fake_plan(text, None, action), self.input_id, self.settings)
            argv, payload = dispatch.command(recipe, '/reviewed/caco')
            self.assertEqual(argv[0], '/reviewed/caco')
            self.assertNotIn('/bin/sh', argv)
            if action in ('agent-msg', 'bd-file'):
                self.assertEqual(payload.decode(), text)
                self.assertNotIn(text, argv)
            if action == 'bd-expand':
                self.assertEqual(argv[argv.index('--text') + 1], text)
                self.assertFalse(recipe['idempotent'])
            elif action == 'bd-file':
                self.assertEqual(argv[argv.index('--request-id') + 1], 'pebble-command-' + self.input_id)
            else:
                self.assertIn('--wake', argv)
                self.assertIn('--idempotency-key', argv)
                self.assertEqual('--raw' in argv, action == 'pebble')

    def test_plan_validation_prevents_changed_body_target_or_title(self):
        plan = fake_plan('Original text', None, 'bd-file')
        plan['description'] = 'rewritten'
        with self.assertRaises(dispatch.DispatchError): dispatch.prepare(plan, self.input_id, self.settings)
        plan = fake_plan('Original text', None, 'agent-msg')
        plan['project'] = 'different'
        with self.assertRaises(dispatch.DispatchError): dispatch.prepare(plan, self.input_id, self.settings)

    def test_valid_json_success_without_mutation_proof_is_not_a_receipt(self):
        recipe = dispatch.prepare(fake_plan('File this', None, 'bd-file'), self.input_id, self.settings)
        for value in ({'ok': True, 'data': {}}, {'ok': True, 'data': {'dry_run': True, 'id': 'bd-fixture', 'title': 'Not created'}}):
            with self.assertRaises(dispatch.DispatchError): dispatch.receipt(recipe, value)
        recipe = dispatch.prepare(fake_plan('Send this', None, 'agent-msg'), self.input_id, self.settings)
        with self.assertRaises(dispatch.DispatchError):
            dispatch.receipt(recipe, {'ok': True, 'data': {'id': 'msg-fixture', 'target': 'wrong:project:recipient'}})


@unittest.skipUnless(os.environ.get('PEBBLE_TEST_CACO'), 'set PEBBLE_TEST_CACO to exercise the real Caco CLI against a fixture')
class ActualCacoTest(unittest.TestCase):
    def test_real_binary_request_grammar_and_receipts_on_isolated_http_peer(self):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                self.send_response(200)
                data = json.dumps({'ok': True, 'data': {'node': 'fixture', 'name': 'fixture', 'agents': [], 'projects': []}}).encode()
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                requests.append((self.path, dict(self.headers), body))
                if self.path.endswith('/messages/broadcast'):
                    global_scope = self.path == '/api/v1/messages/broadcast'
                    data = {'id': 'msg-broadcast-fixture', 'kind': 'broadcast', 'target': None,
                            'scope': 'global' if global_scope else 'project', 'project': '' if global_scope else 'sdk'}
                elif self.path.endswith('/beads/expand'):
                    data = {'created': [{'id': 'bd-expand', 'title': 'Expanded fixture'}], 'count': 1}
                elif self.path.endswith('/beads'):
                    data = {'id': 'bd-create', 'title': body['title']}
                else:
                    data = {'id': 'msg-fixture', 'target': body.get('target'),
                            'admission': {'storage_state': 'stored', 'runtime_delivery_state': 'pending_confirmation', 'pi_admission_state': 'unavailable'}}
                encoded = json.dumps({'ok': True, 'data': data}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                config = Path(directory) / '.cacophony/config.yaml'
                config.parent.mkdir()
                config.write_text(json.dumps({'version': 1, 'services': {'caco-daemon': {'node': 'fixture', 'port': server.server_port, 'cluster_port': server.server_port + 1}},
                    'nodes': [{'name': 'fixture', 'host': '127.0.0.1'}],
                    'projects': [{'name': 'sdk', 'remote': 'https://example.invalid/sdk.git'}]}))
                env = {key: value for key, value in os.environ.items() if not key.startswith(('CACO_', 'CACOPHONY_'))}
                env.update(HOME=directory, XDG_CONFIG_HOME=directory, CACO_DAEMON_URL=f'http://127.0.0.1:{server.server_port}',
                           CACO_AGENT_TOKEN='fixture-caco-secret', CACO_NODE='fixture')
                settings = json.loads((ROOT / 'tests/fixtures/commands-caco.json').read_text())
                settings.update(pebble_project='sdk', pebble_target='fixture:sdk:assistant')
                cli = dispatch.Caco(binary=os.environ['PEBBLE_TEST_CACO'], env=env, timeout=15)
                for index, action in enumerate(('bd-file', 'bd-expand', 'agent-msg', 'pebble', 'broadcast', 'broadcast')):
                    text = "Don't execute $(this); preserve\nthe body"
                    plan = fake_plan(text, None, action)
                    if index == 5: plan.update(scope='global', project=None)
                    recipe = dispatch.prepare(plan, f'{index+1:032x}', settings)
                    outcome = cli.execute(recipe)
                    self.assertEqual(outcome['state'], 'created' if action.startswith('bd-') else 'stored')
                self.assertEqual(len(requests), 6)
                self.assertEqual(requests[0][2]['description'], text)
                headers = {key.lower(): value for key, value in requests[0][1].items()}
                self.assertEqual(headers['authorization'], 'Bearer fixture-caco-secret')
                self.assertEqual(headers['x-caco-request-id'], 'pebble-command-' + f'{1:032x}')
                self.assertEqual(requests[1][2]['text'], text)
                self.assertEqual(requests[2][2]['body'], text)
                self.assertEqual(json.loads(requests[3][2]['body'].split('\n', 1)[1])['text'], text)
                self.assertEqual(requests[4][0], '/api/v1/projects/sdk/messages/broadcast')
                self.assertEqual(requests[5][0], '/api/v1/messages/broadcast')
                for entry in requests[4:]:
                    self.assertEqual(entry[2]['body'], text)
                    self.assertTrue(entry[2]['confirm_broadcast'])
                    self.assertTrue(entry[2]['wake'])
                    self.assertFalse(entry[2]['raw'])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
