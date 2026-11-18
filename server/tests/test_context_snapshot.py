from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import context_snapshot as context
from process_io import ProcessError


class Fixture:
    def __init__(self):
        self.fail = set()
        self.paths = []
        self.local_agents = []

    def get(self, path):
        self.paths.append(path)
        if path in self.fail:
            raise context.ContextError('fixture_unavailable')
        if path == '/api/v1/projects':
            return [{'name': 'sdk', 'remote': 'ssh://git@github.com/example/sdk.git'}]
        if path == '/api/v1/nodes':
            return [{'name': 'node', 'liveness': 'alive', 'last_seen': 'fixture', 'private_setting': 'do not export'}]
        if path == '/api/v1/agents?local=true':
            return {'agents': self.local_agents}
        if path == '/api/v1/agents':
            return {'agents': [
                {'id': 'worker', 'node': 'node', 'project': 'sdk', 'state': 'running', 'agent_name': 'worker',
                 'goal': 'SDK compatibility', 'assigned_bead_ids': ['bd-test'], 'private_setting': 'do not export'},
                {'id': 'retired', 'node': 'node', 'project': 'sdk', 'state': 'completed'},
            ]}
        if path.startswith('/api/v1/messages/chat?'):
            return {'messages': [{'id': 'msg-test', 'project': 'sdk', 'sender': 'node:sdk:worker',
                                  'body': 'I plan to use SDK V5', 'kind': 'direct', 'ts': 'fixture'}]}
        if '/beads?' in path:
            return {'beads': [{'id': 'bd-test', 'title': 'SDK compatibility', 'status': 'in_progress',
                              'description': 'Fixture work', 'assignee': 'sdk:worker', 'updated_at': 'fixture'}]}
        raise AssertionError(path)


class SnapshotTest(unittest.TestCase):
    def test_collects_bounded_global_context_without_inbox_consumption(self):
        api = Fixture()
        value = context.refresh(api, now=1000, describe=lambda remote: 'SDK project')
        self.assertEqual(value['projects'][0]['description'], 'SDK project')
        self.assertEqual(value['agents'][0]['target'], 'node:sdk:worker')
        self.assertEqual(value['agents'][0]['beads'], ['bd-test'])
        self.assertEqual(len(value['agents']), 1)
        self.assertEqual(value['messages'][0]['body'], 'I plan to use SDK V5')
        self.assertNotIn('private_setting', json.dumps(value))
        self.assertFalse(any('inbox' in path for path in api.paths))

    def test_live_local_visitor_is_available_before_fleet_snapshot_catches_up(self):
        api = Fixture()
        api.local_agents = [{'id': 'visiting-new', 'node': 'node', 'project': 'sdk',
                             'state': 'running', 'short_name': 'a', 'inventory_source': 'local_live'}]
        value = context.refresh(api, now=1000, describe=lambda remote: '')
        visitor = next(a for a in value['agents'] if a['target'] == 'node:sdk:visiting-new')
        self.assertEqual(visitor['name'], 'a')
        self.assertEqual(visitor['inventory_source'], 'local_live')
        self.assertIn('/api/v1/agents?local=true', api.paths)

    def test_live_local_identity_wins_without_dropping_remote_namesakes(self):
        api = Fixture()
        api.local_agents = [
            {'id': 'worker', 'node': 'node', 'project': 'sdk', 'state': 'running', 'short_name': 'fresh'},
            {'id': 'worker', 'node': 'other-node', 'project': 'sdk', 'state': 'running', 'short_name': 'other'},
        ]
        agents = context.refresh(api, now=1000, describe=lambda remote: '')['agents']
        self.assertEqual({a['target']: a['name'] for a in agents},
                         {'node:sdk:worker': 'fresh', 'other-node:sdk:worker': 'other'})

    def test_live_terminal_state_is_not_resurrected_from_fleet_snapshot(self):
        api = Fixture()
        api.local_agents = [{'id': 'worker', 'node': 'node', 'project': 'sdk', 'state': 'completed'}]
        self.assertEqual(context.refresh(api, now=1000, describe=lambda remote: '')['agents'], [])

    def test_local_inventory_failure_preserves_previous_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'context.json'
            context.save(context.refresh(Fixture(), now=1000, describe=lambda remote: ''), path)
            original = path.read_bytes()
            api = Fixture()
            api.fail.add('/api/v1/agents?local=true')
            with self.assertRaises(context.ContextError):
                context.save(context.refresh(api, now=1300), path)
            self.assertEqual(path.read_bytes(), original)

    def test_repository_description_is_cached_separately(self):
        first = context.refresh(Fixture(), now=1000, describe=lambda remote: 'Cached description')
        def forbidden(remote):
            self.fail('Do not fetch repository description on every five-minute refresh')
        second = context.refresh(Fixture(), previous=first, now=1300, describe=forbidden)
        self.assertEqual(second['projects'][0]['description'], 'Cached description')
        self.assertEqual(second['projects'][0]['description_at'], 1000)

    def test_core_failure_does_not_replace_previous_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'context.json'
            first = context.refresh(Fixture(), now=1000, describe=lambda remote: '')
            context.save(first, path)
            original = path.read_bytes()
            api = Fixture()
            api.fail.add('/api/v1/agents')
            with self.assertRaises(context.ContextError):
                context.save(context.refresh(api, previous=first, now=1300), path)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_optional_failure_preserves_old_evidence_and_timestamp(self):
        first = context.refresh(Fixture(), now=1000, describe=lambda remote: 'Old description')
        api = Fixture()
        api.fail.add('/api/v1/projects/sdk/beads?sort=updated&limit=3&description_preview=240')
        second = context.refresh(api, previous=first, now=1300, describe=lambda remote: '')
        self.assertEqual(second['projects'][0]['beads'], first['projects'][0]['beads'])
        self.assertEqual(second['projects'][0]['beads_at'], 1000)
        self.assertIn('recent_beads_unavailable', second['projects'][0]['errors'])

    def test_snapshot_write_preserves_managed_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'real-context.json'
            target.write_text('{}')
            link = Path(directory) / 'context.json'
            link.symlink_to(target)
            value = context.refresh(Fixture(), now=1000, describe=lambda remote: '')
            context.save(value, link)
            self.assertTrue(link.is_symlink())
            self.assertEqual(json.loads(target.read_text()), value)

    def test_common_credentials_are_redacted_before_classification(self):
        original = 'Keep SDK V5; Authorization: Bearer abcdefghijklmnop1234 api_key=sk-abcdefghijklmnopqrstuvwx'
        cleaned = context.context_text(original, 500)
        self.assertIn('Keep SDK V5', cleaned)
        self.assertNotIn('abcdefghijklmnop', cleaned)
        self.assertIn('[redacted]', cleaned)

    def test_remote_parser_does_not_follow_arbitrary_hosts(self):
        self.assertEqual(context.repository('ssh://git@github.com/example/sdk.git'), ('github.com', 'example/sdk'))
        self.assertEqual(context.repository('git@github.com:example/sdk.git'), ('github.com', 'example/sdk'))
        self.assertIsNone(context.repository('https://credentials@example.invalid/repo'))
        self.assertIsNone(context.repository('https://github.com/example/sdk?token=do-not-export'))

    def test_real_http_reads_rotation_and_no_redirect_credential_leak(self):
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_GET(self):
                calls.append((self.path, self.headers.get('Authorization')))
                if self.path.endswith('redirect'):
                    self.send_response(302)
                    self.send_header('Location', '/elsewhere')
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                body = b'{"ok":true,"data":{"fixture":true}}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                secret = Path(directory) / 'secret'
                secret.write_text('fixture-one')
                link = Path(directory) / 'managed-token'
                link.symlink_to(secret)
                api = context.CacoReads(f'http://127.0.0.1:{server.server_port}', link, env={})
                self.assertEqual(api.get('/api/v1/example'), {'fixture': True})
                secret.write_text('fixture-two')
                api.get('/api/v1/example')
                with self.assertRaisesRegex(context.ContextError, 'caco_read_http_302'):
                    api.get('/api/v1/redirect')
                self.assertEqual([x[1] for x in calls], ['Bearer fixture-one', 'Bearer fixture-two', 'Bearer fixture-two'])
                self.assertFalse(any(path == '/elsewhere' for path, _ in calls))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(5)
            self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
