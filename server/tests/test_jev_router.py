import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import jev_router as router
from process_io import ProcessError, Result, run


def context():
    return {
        'version': 1, 'created_at': 1000,
        'projects': [
            {'id': 'sdk', 'description': 'SDK integration and compatibility', 'beads': []},
            {'id': 'website', 'description': 'Public web site and documentation', 'beads': []},
        ],
        'agents': [
            {'target': 'node-a:sdk:sdk-worker', 'project': 'sdk', 'name': 'sdk-worker',
             'state': 'running', 'goal': 'Upgrade SDK dependencies', 'beads': ['bd-fixture']},
            {'target': 'node-b:website:site-worker', 'project': 'website', 'name': 'site-worker',
             'state': 'waiting', 'goal': 'Improve web accessibility'},
            {'target': 'node-a:sdk:retired-worker', 'project': 'sdk', 'state': 'completed'},
        ],
        'nodes': [{'id': 'node-a'}, {'id': 'node-b'}],
        'messages': [{'sender': 'node-a:sdk:sdk-worker', 'body': 'I plan to add the V5 SDK.', 'ts': 'fixture'}],
    }


def answer(request, picks=None):
    picks = {'route': 'bd-file', 'project': 'sdk', 'priority': '0', 'agent': 'node-a:sdk:sdk-worker', 'audience': 'sdk', **(picks or {})}
    if picks['agent'] != '__none__':
        picks['agent'] = router.agent_key(picks['agent'])
    return {'model': 'fixture-model', 'provider': 'fixture', 'answers': {
        name: {'type': 'choice', 'choice': picks[name], 'confidence': .9,
               'probabilities': {key: int(key == picks[name]) for key in q['criteria']}}
        for name, q in request['questions'].items()
    }}


class RouterTest(unittest.TestCase):
    def stub(self, picks=None):
        def runner(argv, payload, **kwargs):
            self.argv, self.payload = argv, json.loads(payload)
            self.requests = getattr(self, 'requests', []) + [self.payload]
            return Result(json.dumps(answer(self.payload, picks)).encode(), 12)
        return runner

    def test_staged_real_cli_grammar_and_verbatim_file(self):
        text = "File p0: don't evaluate $(false); keep these words\nunchanged!"
        result = router.plan(text, context(), binary='/fixture/jev', provider='openrouter', now=1000, runner=self.stub())
        self.assertEqual(self.argv, ['/fixture/jev', 'run', '-', '--provider', 'openrouter'])
        self.assertEqual(set(self.requests[0]['questions']), {'route'})
        self.assertEqual(self.requests[0]['state']['context'], {})
        self.assertEqual(set(self.payload['questions']), {'project', 'priority'})
        self.assertEqual(result['description'], text)
        self.assertEqual(result['text'], text)
        self.assertEqual(result['priority'], 0)
        self.assertEqual(result['labels'], ['from-pebble'])
        self.assertFalse(result['executed'])
        self.assertNotIn(text, self.argv)
        agent_request, _ = router.build_request(text, context(), 'agent-msg', now=1000)
        self.assertNotIn(router.agent_key('node-a:sdk:retired-worker'), agent_request['questions']['agent']['criteria'])

    def test_reply_uses_exact_target_and_retains_body(self):
        text = "no don't add the V5 SDK"
        result = router.plan(text, context(), 'agent-msg', now=1000, runner=self.stub())
        self.assertEqual(set(self.payload['questions']), {'agent'})
        self.assertEqual(result['target'], 'node-a:sdk:sdk-worker')
        self.assertEqual(result['body'], text)
        self.assertEqual(result['project'], 'sdk')
        evidence = self.payload['state']['context']['messages'][0]
        self.assertEqual(evidence['body'], context()['messages'][0]['body'])
        self.assertEqual(evidence['sender'], router.agent_key('node-a:sdk:sdk-worker'))

    def test_exact_canonical_target_is_visible_behind_opaque_candidate_key(self):
        snapshot = context()
        snapshot['agents'][0]['name'] = 'a'
        target = snapshot['agents'][0]['target']
        request, candidates = router.build_request(f'Send to ({target}): hello', snapshot, 'agent-msg', now=1000)
        self.assertEqual(request['state']['agent_identity_references'], {router.agent_key(target): target})
        self.assertEqual(len(candidates['agents']), 2, 'identity evidence must not drop other candidates')
        self.assertEqual(set(request['questions']['agent']['criteria']),
                         {'__none__', *candidates['agents']})
        # An exact mention is evidence, never permission to override Jev's
        # refusal or force delivery to a quoted/negated recipient.
        with self.assertRaisesRegex(router.RouterError, 'target_needs_resolution'):
            router.plan(f'Do not send to {target}', snapshot, 'agent-msg', now=1000,
                        runner=self.stub({'agent': '__none__'}))

    def test_partial_or_inactive_target_is_not_an_exact_identity_reference(self):
        for text in ('node-a:sdk:sdk-worker-extra', 'prefix:node-a:sdk:sdk-worker',
                     'node-a:sdk:sdk-worker:extra', '/node-a:sdk:sdk-worker',
                     'node-a:sdk:retired-worker'):
            with self.subTest(text=text):
                request, _ = router.build_request(text, context(), 'agent-msg', now=1000)
                self.assertNotIn('agent_identity_references', request['state'])
        request, _ = router.build_request('Send to node-a:sdk:sdk-worker: hello', context(), 'agent-msg', now=1000)
        self.assertEqual(request['state']['agent_identity_references'],
                         {router.agent_key('node-a:sdk:sdk-worker'): 'node-a:sdk:sdk-worker'})

    def test_identity_references_stay_out_of_unrelated_action_questions(self):
        for action in ('route', 'bd-file', 'broadcast'):
            request, _ = router.build_request('node-a:sdk:sdk-worker', context(), action, now=1000)
            self.assertNotIn('agent_identity_references', request['state'])

    def test_explicit_action_is_not_overridden(self):
        result = router.plan('Expand the SDK work', context(), 'bd-expand', now=1000, runner=self.stub())
        self.assertEqual(result['action'], 'bd-expand')
        self.assertEqual(set(self.payload['questions']), {'project', 'priority'})

    def test_none_target_falls_back_only_for_automatic_routing(self):
        result = router.plan('Tell someone about it', context(), now=1000,
                             runner=self.stub({'route': 'agent-msg', 'agent': '__none__'}))
        self.assertEqual(result['action'], 'pebble')
        self.assertNotIn('target', result)
        with self.assertRaisesRegex(router.RouterError, 'target_needs_resolution'):
            router.plan('Tell someone about it', context(), 'agent-msg', now=1000,
                        runner=self.stub({'agent': '__none__'}))

    def test_invalid_choice_cannot_become_a_command(self):
        def bad(argv, payload, **kwargs):
            response = answer(json.loads(payload))
            if 'project' in response['answers']:
                response['answers']['project']['choice'] = 'unconfigured'
            return Result(json.dumps(response).encode(), 0)
        with self.assertRaisesRegex(router.RouterError, 'invalid_jev_choice_project'):
            router.plan('file this', context(), now=1000, runner=bad)

    def test_stale_context_and_invalid_identities_never_call_jev(self):
        def unreachable(*a, **kw):
            self.fail('Jev must not be called')
        with self.assertRaisesRegex(router.RouterError, 'context_stale'):
            router.plan('file this', context(), now=2000, max_age=300, runner=unreachable)
        wrong = context()
        wrong['agents'][0]['target'] = 'ambiguous-short-name'
        with self.assertRaisesRegex(router.RouterError, 'invalid_agent_identity'):
            router.plan('file this', wrong, now=1000, runner=unreachable)
        wrong = context()
        wrong['projects'].append(copy.deepcopy(wrong['projects'][0]))
        with self.assertRaisesRegex(router.RouterError, 'invalid_project_identity'):
            router.plan('file this', wrong, now=1000, runner=unreachable)

    def test_target_project_must_match_its_full_identity(self):
        snapshot = context()
        snapshot['agents'][0]['project'] = 'wrong-project'
        with self.assertRaisesRegex(router.RouterError, 'agent_project_mismatch'):
            router.plan('send this', snapshot, now=1000, runner=self.stub())

    def test_cli_failure_is_not_a_negative_judgment(self):
        def failed(*a, **kw):
            raise ProcessError('command_failed', 4)
        with self.assertRaisesRegex(router.RouterError, 'jev_command_failed_4'):
            router.plan('file this', context(), now=1000, runner=failed)

    def test_explicit_pebble_does_not_run_classifier(self):
        def unreachable(*a, **kw):
            self.fail('Jev must not be called')
        result = router.plan('status', None, 'pebble', now=1000, runner=unreachable)
        self.assertEqual(result['decision']['source'], 'explicit_endpoint')

    def test_fleet_sized_snapshot_is_projected_without_dropping_candidates(self):
        snapshot = context()
        snapshot['projects'] = [{'id': f'p{i}', 'description': 'Project purpose ' * 30,
                                  'beads': [{'id': f'bd-{i}-{j}', 'title': 'Recent task ' * 20} for j in range(3)]}
                                 for i in range(36)]
        snapshot['agents'] = [{'target': f'node:p{i % 36}:worker-{i}', 'project': f'p{i % 36}',
                               'name': f'worker-{i}', 'state': 'running', 'goal': 'Current assignment ' * 25}
                              for i in range(66)]
        snapshot['messages'] = [{'id': f'msg-{i}', 'sender': snapshot['agents'][i]['target'],
                                 'body': 'Bounded recent work ' * 25, 'ts': str(i)} for i in range(40)]
        request, candidates = router.build_request('Send this to the worker', snapshot, 'agent-msg', now=1000)
        referenced_request, referenced_candidates = router.build_request(
            f"Send to ({snapshot['agents'][0]['target']}): hello", snapshot, 'agent-msg', now=1000)
        self.assertLessEqual(len(json.dumps(referenced_request, ensure_ascii=False).encode()), router.MAX_REQUEST_BYTES)
        self.assertEqual(len(referenced_candidates['agents']), 66)
        project_request, _ = router.build_request('File a p4 task in p3', snapshot, 'bd-file', now=1000)
        self.assertLessEqual(len(json.dumps(request, ensure_ascii=False).encode()), router.MAX_REQUEST_BYTES)
        self.assertLessEqual(len(json.dumps(project_request, ensure_ascii=False).encode()), router.MAX_REQUEST_BYTES)
        self.assertEqual(len(project_request['questions']['project']['criteria']), 37)
        self.assertEqual(len(request['questions']['agent']['criteria']), 67)
        self.assertEqual(len(candidates['agents']), 66)
        self.assertNotIn('agents', request['state']['context'])
        self.assertLessEqual(len(request['state']['context']['messages']), 4)

    def test_full_text_preserved_but_title_bounded(self):
        text = '🪨\n' * 100
        result = router.plan(text, context(), 'bd-file', now=1000, runner=self.stub())
        self.assertEqual(result['description'], text)
        self.assertEqual(result['title'], ' '.join(text[:80].split()))
        self.assertLessEqual(len(result['title']), 80)


class ProcessTest(unittest.TestCase):
    def test_stdin_is_not_shell_and_exit_failure_is_typed(self):
        text = b'$(touch never)\n; echo not-a-command'
        result = run([sys.executable, '-c', 'import sys;sys.stdout.buffer.write(sys.stdin.buffer.read())'], text)
        self.assertEqual(result.stdout, text)
        with self.assertRaises(ProcessError) as caught:
            run([sys.executable, '-c', 'raise SystemExit(4)'])
        self.assertEqual(caught.exception.exit_code, 4)

    def test_timeout_and_output_are_bounded(self):
        with self.assertRaisesRegex(ProcessError, 'command_timeout'):
            run([sys.executable, '-c', 'import time;time.sleep(10)'], timeout=.1)
        with self.assertRaisesRegex(ProcessError, 'command_output_limit'):
            run([sys.executable, '-c', 'print("x"*10000)'], max_output=100)


@unittest.skipUnless(os.environ.get('PEBBLE_TEST_JEV'), 'set PEBBLE_TEST_JEV to the actual jev binary')
class ActualJevCliTest(unittest.TestCase):
    def test_real_cli_stdin_request_and_response_against_isolated_peer(self):
        requests = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                self.assert_path = self.path
                payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                requests.append((self.headers.get('Authorization'), payload))
                body = json.dumps(answer(payload, {'route': 'agent-msg'})).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                env = {**os.environ, 'HOME': directory, 'XDG_CONFIG_HOME': directory,
                       'JEV_API_KEY': 'fixture-key', 'JEV_ENDPOINT': f'http://127.0.0.1:{server.server_port}/fixture',
                       'JEV_MODEL': 'fixture-model'}
                result = router.plan("no don't add V5", context(), binary=os.environ['PEBBLE_TEST_JEV'],
                                     provider='custom', env=env, now=1000)
                self.assertEqual(result['target'], 'node-a:sdk:sdk-worker')
                self.assertEqual(len(requests), 2)
                self.assertEqual(requests[0][0], 'Bearer fixture-key')
                self.assertEqual(requests[0][1]['model'], 'fixture-model')
                self.assertEqual(requests[0][1]['state']['operator_transcript'], "no don't add V5")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
