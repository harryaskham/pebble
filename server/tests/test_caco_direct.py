"""Caco is invoked directly, including after a local binary replacement."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
import urllib.request

import test_command_pipeline as fixtures
import dispatch
from process_io import ProcessError, Result


class DirectCacoTest(unittest.TestCase):
    def test_all_actions_invoke_only_the_requested_command_without_initialization(self):
        settings = json.loads((fixtures.ROOT / 'tests/fixtures/commands-caco.json').read_text())
        for action in ('pebble', 'agent-msg', 'broadcast', 'bd-file', 'bd-expand'):
            with self.subTest(action=action):
                recipe = dispatch.prepare(fixtures.fake_plan('fixture data', None, action), 'a' * 32, settings)
                expected, body = dispatch.command(recipe, '/fixture/caco')
                calls = []
                def runner(argv, payload, **kwargs):
                    calls.append(argv)
                    self.assertEqual(argv, expected)
                    self.assertEqual(payload, body)
                    self.assertEqual(kwargs['timeout'], 130)
                    return Result(json.dumps(fixtures.Sender()(recipe)['raw']).encode(), 1)
                cli = dispatch.Caco(binary='/fixture/caco', runner=runner)
                self.assertEqual(calls, [])
                self.assertIn(cli.execute(recipe)['state'], ('created', 'stored'))
                self.assertEqual(calls, [expected])

    def test_same_client_uses_replaced_binary_without_restart_or_probes(self):
        with tempfile.TemporaryDirectory() as root:
            binary, log = Path(root) / 'caco', Path(root) / 'calls'
            def install(version):
                next_binary = Path(root) / 'next-caco'
                data = {'ok': True, 'data': {'id': 'msg-' + version, 'kind': 'broadcast',
                        'scope': 'project', 'project': 'sdk', 'target': None}}
                next_binary.write_text(f'''#!{sys.executable}
import sys,json
from pathlib import Path
with Path({str(log)!r}).open('a') as output:
    output.write(json.dumps(sys.argv[1:])+'\\n')
if sys.argv[1:3] != ['msg','broadcast']: raise SystemExit(91)
print({json.dumps(data)!r})
''')
                next_binary.chmod(0o700)
                os.replace(next_binary, binary)
            install('before')
            cli = dispatch.Caco(binary=str(binary))
            for index, version in enumerate(('before', 'after'), 1):
                if version == 'after': install(version)
                plan = fixtures.fake_plan('fixture broadcast', None, 'broadcast')
                recipe = dispatch.prepare(plan, f'{index:032x}', {})
                self.assertEqual(cli.execute(recipe)['message_id'], 'msg-' + version)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual(len(calls), 2)
            self.assertTrue(all(call[:2] == ['msg', 'broadcast'] for call in calls))

    def test_real_command_failure_is_reported_without_an_extra_probe_or_retry(self):
        recipe = dispatch.prepare(fixtures.fake_plan('fixture', None, 'broadcast'), 'b' * 32, {})
        for code in ('command_unavailable', 'command_failed', 'command_timeout'):
            calls = []
            def runner(argv, *args, **kwargs):
                calls.append(argv)
                raise ProcessError(code)
            with self.assertRaisesRegex(ProcessError, code):
                dispatch.Caco(binary='/fixture/caco', runner=runner).execute(recipe)
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0][1:3], ['msg', 'broadcast'])


class DirectPipelineTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    http = fixtures.PipelineTest.http
    post = fixtures.PipelineTest.post
    text = fixtures.PipelineTest.text
    drain = fixtures.PipelineTest.drain

    def test_service_start_health_and_preview_never_invoke_caco(self):
        log, binary = self.root / 'calls', self.root / 'caco-trap'
        binary.write_text(f'#!{sys.executable}\nfrom pathlib import Path\nPath({str(log)!r}).touch()\nraise SystemExit(91)\n')
        binary.chmod(0o700)
        self.config['commands']['caco']['binary'] = str(binary)
        self.processor.sender = None
        with self.http(start_worker=True) as (url, worker):
            req = urllib.request.Request(url + '/health', headers={'Authorization': 'Bearer ' + self.legacy.token})
            with urllib.request.urlopen(req, timeout=2) as response:
                self.assertEqual(json.load(response)['caco_capability_checks'], 'none')
            body = json.dumps({'transcription': 'preview', 'recordedAt': '1001'}).encode()
            dry = self.archive.accept(body, 'application/json', 'broadcast', dry_run=True)
            worker.wake.set()
            deadline = time.monotonic() + 3
            while self.archive.get(dry['id'])['phase'] != 'dry_run' and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertEqual(self.archive.receipt(dry['id'])['action']['attempts'], 0)
            self.assertEqual(self.archive.get(dry['id'])['phase'], 'dry_run')
        self.assertFalse(log.exists())

    def test_removing_gate_does_not_replay_previously_blocked_broadcast(self):
        old = self.text('old fixture broadcast', action='broadcast', recorded='2001')
        sender = self.processor.sender
        def old_gate(_): raise dispatch.NotStarted('caco_binary_changed_restart_required')
        self.processor.sender = old_gate
        self.drain()
        before = self.archive.receipt(old['id'])
        self.assertEqual(before['phase'], 'blocked')
        self.archive.recover()
        self.processor.sender = sender
        fresh = self.text('new fixture broadcast', action='broadcast', recorded='2002')
        self.drain()
        self.assertEqual(self.archive.receipt(old['id']), before)
        self.assertEqual(self.archive.receipt(fresh['id'])['phase'], 'stored')
        self.assertEqual([call['input_id'] for call in sender.calls], [fresh['id']])


if __name__ == '__main__': unittest.main()
