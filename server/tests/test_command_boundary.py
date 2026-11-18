"""Real receiver -> FFmpeg/Node -> Jev -> Caco, with isolated protocol peers."""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_receiver as legacy_tests
from test_jev_router import context as fixture_context, answer

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get('PEBBLE_TEST_JEV') and os.environ.get('PEBBLE_TEST_CACO'),
                     'set PEBBLE_TEST_JEV and PEBBLE_TEST_CACO for the full executable boundary')
class CommandBoundaryTest(unittest.TestCase):
    def test_full_real_process_chain_has_one_action_and_no_public_network_or_audio(self):
        self.process_chain(False)

    def test_dry_run_real_audio_and_jev_never_invoke_even_caco_help(self):
        self.process_chain(True)

    def test_real_custom_command_chain(self):
        self.process_chain(False, custom=True)

    def test_dry_custom_chain_never_runs_the_handler(self):
        self.process_chain(True, custom=True)

    def process_chain(self, dry_run, custom=False):
        fixture = legacy_tests.ReceiverTest('runTest')
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        mai = fixture.use_peer()
        calls = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                calls.append((self.path, body, self.headers.get('Authorization')))
                if self.path == '/jev':
                    payload = answer(body, {'route': 'custom:note'} if custom else None)
                elif self.path == '/responses':
                    payload = {'id':'extract-fixture','status':'completed','model':'example-extractor',
                               'output':[{'type':'message','content':[{'type':'output_text','text':json.dumps({'status':'ok','segments':['higher quality words']})}]}]}
                elif self.path == '/api/v1/projects/sdk/beads':
                    payload = {'ok': True, 'data': {'id': 'bd-boundary', 'title': body['title'], 'priority': body['priority']}}
                else:
                    self.send_error(404)
                    return
                encoded = json.dumps(payload).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(encoded)))
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(encoded)
        peer = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=peer.serve_forever)
        thread.start()
        def cleanup_peer():
            peer.shutdown()
            peer.server_close()
            thread.join(5)
            self.assertFalse(thread.is_alive())
        self.addCleanup(cleanup_peer)
        peer_url = f'http://127.0.0.1:{peer.server_port}'
        command = json.loads((ROOT / 'tests/fixtures/commands-caco.json').read_text())
        command.update(enabled=True, context_file=str(fixture.root / 'context.json'), legacy_text_archive='')
        command['jev'].update(binary=os.environ['PEBBLE_TEST_JEV'], provider='custom', model='fixture-model')
        command['extractor']['base_url'] = peer_url
        command['caco'].update(binary=os.environ['PEBBLE_TEST_CACO'], daemon_url=peer_url,
                              token_file=str(fixture.root / 'webhook-token'), timeout_secs=10)
        note = fixture.root / 'notes.org'
        if custom:
            command['actions'] = {'note': {'description': 'Save a private note.', 'extract': 'Copy the note text.',
                'command': [sys.executable, str(ROOT.parent / 'scripts/pebble-org-append'), '--file', str(note)]}}
        caco_marker = fixture.root / 'caco-was-invoked'
        if dry_run or custom:
            trap = fixture.root / 'caco-trap'
            trap.write_text(f'#!{sys.executable}\nfrom pathlib import Path\nPath({str(caco_marker)!r}).touch()\nraise SystemExit(91)\n')
            trap.chmod(0o700)
            command['caco']['binary'] = str(trap)
        command['speech']['enabled'] = False
        command['speech']['cache_dir'] = str(fixture.root / 'speech')
        snapshot = fixture_context()
        snapshot['created_at'] = time.time()
        Path(command['context_file']).write_text(json.dumps(snapshot))
        config = fixture.config
        if os.environ.get('PEBBLE_AGENT_UTILS'):
            config['transcription']['agent_utils_dir'] = os.environ['PEBBLE_AGENT_UTILS']
        config['commands'] = command
        config_file = fixture.root / 'receiver.json'
        config_file.write_text(json.dumps(config))
        home = fixture.root / 'isolated-home'
        caco_config = home / '.cacophony/config.yaml'
        caco_config.parent.mkdir(parents=True)
        caco_config.write_text(json.dumps({'version': 1,
            'services': {'caco-daemon': {'node': 'fixture', 'port': peer.server_port, 'cluster_port': peer.server_port + 1}},
            'nodes': [{'name': 'fixture', 'host': '127.0.0.1'}],
            'projects': [{'name': 'sdk', 'remote': 'https://example.invalid/sdk.git'}]}))
        env = {key: value for key, value in os.environ.items() if not key.startswith(('CACO_', 'CACOPHONY_', 'JEV_', 'PEBBLE_UI_', 'PEBBLE_SELF_'))}
        env.update(HOME=str(home), XDG_CONFIG_HOME=str(home), JEV_API_KEY='fixture-jev-key', JEV_ENDPOINT=peer_url + '/jev',
                   CACO_NODE='fixture', OPENAI_API_KEY='fixture-extractor-key', PYTHONDONTWRITEBYTECODE='1')
        process = subprocess.Popen([sys.executable, str(ROOT / 'receiver.py'), '--config', str(config_file), 'serve'],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, start_new_session=True)
        address = None
        try:
            ready, _, _ = select.select([process.stdout], [], [], 8)
            self.assertTrue(ready, 'receiver startup deadline')
            message = json.loads(process.stdout.readline())
            self.assertEqual(message.get('event'), 'pebble_audio_listening', message)
            address = ('127.0.0.1', message['port'])
            url = f'http://127.0.0.1:{address[1]}'
            def request(path, data=None, kind=None):
                headers = {'Authorization': 'Bearer ' + fixture.token}
                if kind: headers['Content-Type'] = kind
                if dry_run: headers['X-Pebble-Dry-Run'] = 'true'
                req = urllib.request.Request(url + path, data=data, headers=headers)
                with urllib.request.urlopen(req, timeout=5) as response:
                    return response.status, json.load(response)
            status, health = request('/health')
            self.assertEqual(status, 200)
            self.assertTrue(health['worker_alive'])
            self.assertEqual(health['caco_capability_checks'], 'none')
            self.assertNotIn('caco_capabilities', health)
            self.assertFalse(caco_marker.exists())
            body, content_type = legacy_tests.multipart()
            status, receipt = request('/hooks/global/pebble-audio', body, content_type)
            self.assertEqual(status, 202)
            deadline = time.monotonic() + 20
            while True:
                _, outcome = request('/receipts/' + receipt['id'])
                if outcome['phase'] in ('created', 'completed', 'blocked', 'dry_run') or time.monotonic() >= deadline:
                    break
                time.sleep(.05)
            self.assertEqual(outcome['phase'], 'dry_run' if dry_run else 'completed' if custom else 'created', outcome)
            if dry_run:
                self.assertFalse(outcome['action']['executed'])
                self.assertFalse(caco_marker.exists())
            elif custom:
                self.assertIn('higher quality words', note.read_text())
            else:
                self.assertEqual(outcome['action']['beads'][0]['id'], 'bd-boundary')
            if dry_run: self.assertFalse(note.exists())
            self.assertEqual(mai.mai_count, 1)
            self.assertEqual([entry[0] for entry in calls], ['/jev'] + ([] if custom else ['/jev']) + ['/responses'] + ([] if dry_run or custom else ['/api/v1/projects/sdk/beads']))
            self.assertEqual(calls[0][1]['state']['operator_transcript'], 'higher quality words')
            self.assertEqual(calls[0][2], 'Bearer fixture-jev-key')
            extract_call = calls[1 if custom else 2]
            self.assertEqual(extract_call[1]['model'], 'example-extractor')
            self.assertEqual(extract_call[2], 'Bearer fixture-extractor-key')
            self.assertEqual(outcome['extraction']['status'], 'complete')
            if not dry_run and not custom:
                self.assertEqual(calls[3][1]['description'], 'higher quality words')
                self.assertEqual(calls[3][1]['labels'], ['from-pebble'])
                self.assertEqual(calls[3][2], 'Bearer ' + fixture.token)
            duplicate, kind = legacy_tests.multipart(boundary='new-boundary', text='different rough words')
            _, second = request('/hooks/global/pebble-audio', duplicate, kind)
            self.assertEqual(second['id'], receipt['id'])
            self.assertTrue(second['duplicate'])
            status, probe = request('/hooks/global/pebble-route', b'{"test":true}', 'application/json')
            self.assertEqual(status, 200)
            self.assertFalse(probe['archived'])
            _, final = request('/receipts/' + receipt['id'])
            self.assertEqual(final['action']['attempts'], 0 if dry_run else 1)
            self.assertTrue(all(speech['state'] == 'disabled' for speech in final['speech']))
            self.assertEqual(len(calls), 2 if custom else 3 if dry_run else 4)
            self.assertFalse(caco_marker.exists())
        finally:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=3)
                self.fail('receiver did not shut down cleanly')
            process.stdout.close()
            process.stderr.close()
            if address:
                with self.assertRaises(OSError): socket.create_connection(address, timeout=.2)
        self.assertEqual(process.returncode, 0)


if __name__ == '__main__':
    unittest.main()
