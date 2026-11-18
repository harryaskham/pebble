"""Preview goes through real ingestion/ledger stages, never through Caco."""
import json
import http.client
import unittest
import urllib.request
import urllib.error
from unittest.mock import patch

import test_command_pipeline as fixtures
from test_command_pipeline import legacy_tests
import dispatch
import intake


class DryRunTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    drain = fixtures.PipelineTest.drain
    http = fixtures.PipelineTest.http

    def accept(self, action='route', *, dry_run=True, recorded='1770000000300'):
        body = json.dumps({'transcription': 'Preview these exact words', 'recordedAt': recorded}).encode()
        return self.archive.accept(body, 'application/json', action, dry_run=dry_run)

    def test_all_actions_preview_with_speech_and_zero_sender_calls(self):
        with patch.object(dispatch.Caco, 'execute', side_effect=AssertionError('no Caco')):
            for i, action in enumerate(('route', 'pebble', 'agent-msg', 'bd-file', 'bd-expand')):
                r = self.accept(action, recorded=str(1770000000300+i))
                self.drain()
                receipt = self.archive.receipt(r['id'])
                self.assertEqual(receipt['phase'], 'dry_run')
                self.assertTrue(receipt['dry_run'])
                self.assertFalse(receipt['action']['executed'])
                self.assertEqual(receipt['action']['attempts'], 0)
                self.assertNotIn('beads', receipt['action'])
                self.assertNotIn('message_id', receipt['action'])
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(sum(c[0] == 'prefix' for c in self.voice.calls), 5)
        self.assertTrue(any('No action command was executed' in str(c) for c in self.voice.calls))
        self.assertIsNone(self.archive.head())

    def test_audio_uses_mai_and_readback(self):
        body, kind = legacy_tests.multipart()
        r = self.archive.accept(body, kind, dry_run=True)
        self.drain()
        self.assertEqual(len(self.transcriptions), 1)
        self.assertEqual(self.archive.get(r['id'])['text'], 'MAI words, not device words')
        self.assertIn(('speak', 'MAI words, not device words', self.voice.self_voice), self.voice.calls)
        self.assertEqual(self.sender.calls, [])

    def test_mode_is_frozen_and_cannot_be_promoted_by_retry(self):
        r = self.accept()
        self.drain()
        before = list(self.voice.calls)
        conflict = self.accept(dry_run=False)
        self.assertEqual(conflict['id'], r['id'])
        self.assertEqual(conflict['error'], 'input_identity_conflict')
        self.assertTrue(conflict['dry_run'])
        self.assertTrue(self.accept()['duplicate'])
        self.archive.recover()
        self.drain()
        self.assertEqual(self.voice.calls, before)
        self.assertEqual(self.sender.calls, [])

    def test_executor_and_ledger_refuse_preview_before_any_subprocess(self):
        r = self.accept()
        self.processor.step()
        recipe = json.loads(self.archive.action(r['id'])['recipe'])
        def trap(*a, **k): self.fail('Caco invoked')
        with self.assertRaisesRegex(dispatch.NotStarted, 'dry_run_execution_forbidden'):
            dispatch.Caco(runner=trap).execute(recipe)
        with self.assertRaisesRegex(intake.IntakeError, 'dry_run_execution_forbidden'):
            self.archive.start_action(r['id'])

    def test_preview_bypasses_blocked_live_destination(self):
        live = self.accept(dry_run=False, recorded='1770000000200')
        self.processor.step()
        self.archive.update(live['id'], phase='blocked')
        preview = self.accept()
        self.drain()
        self.assertEqual(self.archive.get(preview['id'])['phase'], 'dry_run')
        self.assertEqual(self.sender.calls, [])

    def test_failed_preview_does_not_hold_live_destination(self):
        preview = self.accept(recorded='1770000000200')
        self.processor.step()
        self.archive.update(preview['id'], phase='blocked')
        live = self.accept(dry_run=False)
        self.drain()
        self.assertEqual(self.archive.get(live['id'])['phase'], 'created')
        self.assertEqual(len(self.sender.calls), 1)

    def test_restart_before_preview_completion_keeps_mode_and_speech_claims(self):
        r = self.accept()
        self.processor.step()
        self.archive.recover()
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_run')
        self.assertEqual(sum(c[0] == 'prefix' for c in self.voice.calls), 1)
        self.assertEqual(self.sender.calls, [])

    def test_http_header_query_auth_and_invalid_values(self):
        with self.http() as (url, _):
            for i, (query, headers, expected) in enumerate([
                ('', {'X-Pebble-Dry-Run': 'true'}, 202),
                ('?dry_run=true', {}, 202),
                ('?dry_run=false', {'X-Pebble-Dry-Run': 'true'}, 400),
                ('?dry_run=', {}, 400),
                ('?dry_run=true&dry_run=true', {}, 400),
                ('?dryrun=true', {}, 400),
                ('', {'X-Pebble-Dry-Run': 'maybe'}, 400),
                ('', {'X-Pebble-Dry-Run': 'true', 'Authorization': 'Bearer wrong'}, 401),
            ]):
                body = json.dumps({'transcription': 'test request', 'recordedAt': str(1770000000500+i)}).encode()
                request = urllib.request.Request(url+'/hooks/global/pebble-route'+query, data=body,
                    headers={'Authorization': 'Bearer '+self.legacy.token, 'Content-Type': 'application/json', **headers})
                try:
                    response = urllib.request.urlopen(request, timeout=4)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response:
                    value = json.load(response)
                    self.assertEqual(response.code, expected)
                    if expected == 202: self.assertTrue(value['dry_run'])
        self.drain()
        self.assertEqual(self.sender.calls, [])

    def test_failed_routing_retry_keeps_preview_mode(self):
        r = self.accept('bd-file')
        planner = self.processor.planner
        def unavailable(*a, **k): raise OSError('fixture unavailable')
        self.processor.planner = unavailable
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_blocked')
        self.archive.resolve(r['id'], 'retry')
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_ready')
        self.processor.planner = planner
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_run')
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(sum(c[0] == 'prefix' for c in self.voice.calls), 1)

    def test_preview_uses_distinct_phases_that_old_workers_cannot_dispatch(self):
        r = self.accept()
        self.assertEqual(r['phase'], 'dry_ready')
        self.processor.step()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_planned')
        self.processor.step()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_confirming')
        self.processor.step()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_run')

    def test_failed_speech_does_not_lose_preview_or_invoke_caco(self):
        self.voice.fail = True
        r = self.accept()
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'], 'dry_run')
        self.assertTrue(all(s['state'] == 'failed' for s in self.archive.receipt(r['id'])['speech']))
        self.assertEqual(self.sender.calls, [])

    def test_duplicate_headers_rejected_without_archival(self):
        before = self.archive.status()
        with self.http() as (url, _):
            connection = http.client.HTTPConnection(url.removeprefix('http://'), timeout=4)
            try:
                body = b'{"transcription":"never send this"}'
                connection.putrequest('POST', '/hooks/global/pebble-route')
                connection.putheader('Authorization', 'Bearer '+self.legacy.token)
                connection.putheader('Content-Type', 'application/json')
                connection.putheader('Content-Length', str(len(body)))
                connection.putheader('X-Pebble-Dry-Run', 'true')
                connection.putheader('X-Pebble-Dry-Run', 'true')
                connection.endheaders(body)
                response = connection.getresponse()
                self.assertEqual(response.status, 400)
                response.read()
            finally:
                connection.close()
        self.assertEqual(self.archive.status(), before)

    def test_legacy_receiver_refuses_dry_run_instead_of_forwarding(self):
        before = self.legacy.archive.status()
        with self.legacy.http_server() as (_, url):
            request = urllib.request.Request(url+'/hooks/global/pebble-audio', data=b'{"transcription":"never forward"}',
                headers={'Authorization':'Bearer '+self.legacy.token, 'Content-Type':'application/json', 'X-Pebble-Dry-Run':'true'})
            with self.assertRaises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request, timeout=4)
            self.assertEqual(caught.exception.code, 400)
            caught.exception.close()
        self.assertEqual(self.legacy.archive.status(), before)

    def test_dry_probe_has_no_archive_or_speech(self):
        before = self.archive.status()
        r = self.archive.accept(b'{"test":true}', 'application/json', dry_run=True)
        self.assertEqual(r['phase'], 'probe')
        self.assertEqual(self.archive.status(), before)
        self.assertEqual(self.voice.calls, [])
