import contextlib
import copy
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import command_pipeline
from feedback import Feedback
import intake
import jev_router
from process_io import Result
import receiver
import settings
import test_command_pipeline as fixtures
import test_receiver as receiver_fixtures


def executable(root, name, text):
    path = Path(root) / name
    path.write_text(text)
    path.chmod(0o700)
    return path


class Webhook:
    def __init__(self, status=204, body=b''):
        self.calls = []
        peer = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass
            def do_POST(self):
                data = self.rfile.read(int(self.headers['Content-Length']))
                peer.calls.append((dict(self.headers), data))
                self.send_response(status)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}/hook'

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)


class GenericConfigTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='pebble-generic-test-')
        self.root = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)

    def config(self):
        config = settings.normalize(json.loads((ROOT / 'tests/fixtures/config-v1.json').read_text()))
        config.update(token_file=str(self.root / 'ingress-token'), node='node')
        return config

    def test_version_one_upgrade_keeps_existing_provider_and_ingress_bearer(self):
        old = json.loads((ROOT / 'tests/fixtures/config-v1.json').read_text())
        upgraded = settings.normalize(old)
        self.assertEqual(upgraded['version'], 2)
        self.assertEqual(upgraded['transcription']['provider'], 'agent-utils-mai')
        self.assertEqual(upgraded['transcription']['model'], 'mai-transcribe-2')
        self.assertEqual(upgraded['forward']['auth'], 'bearer-ingress')
        self.assertEqual(upgraded['forward']['receipt'], 'caco-hook')
        self.assertNotIn('mai', upgraded)

    def test_public_examples_validate_without_private_adapters(self):
        for name in ('config-webhook.json', 'config-triage.json'):
            config = settings.normalize(json.loads((ROOT.parent / 'examples' / name).read_text()))
            if 'commands' in config:
                command_pipeline.validate_settings(config['commands'])
                self.assertFalse(config['commands']['caco']['enabled'])
                self.assertEqual(config['commands']['context_file'], '')

    def test_command_transcription_accepts_text_or_json_and_receives_wav_on_stdin(self):
        script = executable(self.root, 'stt', f'#!{sys.executable}\n'
            'import json, os, sys\n'
            'data = sys.stdin.buffer.read()\n'
            'assert data[:4] == b"RIFF" and data[8:12] == b"WAVE"\n'
            'print(json.dumps({"text": " json words ", "source": os.environ.get("PEBBLE_STT_FIXTURE")}) if sys.argv[1:] == ["json"] else " text words ")\n')
        config = self.config()
        config['transcription'] = {'provider': 'command', 'model': 'local-stt', 'timeout_secs': 5,
                                   'max_audio_secs': 10, 'language': 'en', 'command': [str(script)], 'output': 'text'}
        self.assertEqual(receiver.transcription_command(config, receiver_fixtures.wav_bytes())['text'], 'text words')
        config['transcription'].update(command=[str(script), 'json'], output='json')
        os.environ['PEBBLE_STT_FIXTURE'] = 'from-service-env'
        self.addCleanup(os.environ.pop, 'PEBBLE_STT_FIXTURE', None)
        result = receiver.transcription_command(config, receiver_fixtures.wav_bytes())
        self.assertEqual(result['text'], 'json words')
        self.assertEqual(result['model'], 'local-stt')
        self.assertEqual(result['response']['source'], 'from-service-env')

    def test_generic_webhook_can_be_unauthenticated_or_use_a_separate_bearer_file(self):
        peer = Webhook()
        self.addCleanup(peer.close)
        token = self.root / 'webhook-token'
        token.write_text('separate-webhook-token\n')
        token.chmod(0o600)
        for auth, expected in [('none', None), ('bearer-file', 'Bearer separate-webhook-token')]:
            config = self.config()
            config['node'] = 'node'
            config['forward'] = {'url': peer.url, 'timeout_secs': 5, 'auth': auth,
                                 'token_file': str(token) if auth == 'bearer-file' else '', 'receipt': 'http-2xx'}
            capture = {'settings': json.dumps(config), 'forward_body': json.dumps({'transcription': 'hello'}).encode()}
            result = receiver.forward(capture)
            self.assertTrue(result['ok'])
            headers = {k.lower(): v for k, v in peer.calls[-1][0].items()}
            self.assertEqual(headers.get('authorization'), expected)
            self.assertEqual(json.loads(peer.calls[-1][1]), {'transcription': 'hello'})

    def test_webhook_mode_archives_text_and_forwards_without_transcription(self):
        peer = Webhook()
        self.addCleanup(peer.close)
        token = self.root / 'ingress-token'
        token.write_text('ingress-token\n')
        token.chmod(0o600)
        config = self.config()
        config.update(state_dir=str(self.root / 'state'), min_free_bytes=1)
        config['forward'] = {'url': peer.url, 'timeout_secs': 5, 'auth': 'bearer-ingress', 'token_file': '', 'receipt': 'http-2xx'}
        archive = receiver.Archive(config)
        body = json.dumps({'transcription': 'typed request', 'recordedAt': '1770000000777', 'client': 'shortcut'}).encode()
        receipt = receiver.admit(archive, body, 'application/json', sound=lambda _: 'disabled')
        self.assertEqual((receipt['input'], receipt['phase']), ('text', 'ready'))
        transcriptions = []
        worker = receiver.Worker(archive, transcriber=lambda capture: transcriptions.append(capture))
        self.assertTrue(worker.step())
        self.assertEqual(transcriptions, [])
        self.assertEqual(archive.get(receipt['id'])['phase'], 'forwarded')
        headers, data = peer.calls[-1]
        self.assertEqual({k.lower(): v for k, v in headers.items()}['authorization'], 'Bearer ingress-token')
        forwarded = json.loads(data)
        self.assertEqual((forwarded['transcription'], forwarded['transcript_source']), ('typed request', 'client-text'))
        duplicate = receiver.admit(archive, body, 'application/json', sound=lambda _: 'disabled')
        self.assertEqual((duplicate['id'], duplicate['duplicate']), (receipt['id'], True))

    def test_custom_only_jev_route_never_offers_caco_actions(self):
        calls = []
        def runner(argv, payload, **_):
            request = json.loads(payload)
            calls.append(request)
            self.assertEqual(set(request['questions']['route']['criteria']), {'custom:note'})
            return Result(json.dumps({'answers': {'route': {'choice': 'custom:note', 'confidence': 1}}}).encode(), 1)
        snapshot = {'version': 1, 'created_at': time.time(), 'projects': [], 'agents': [], 'nodes': [], 'messages': []}
        plan = jev_router.plan('save this note', snapshot, runner=runner, builtins=False,
                               actions={'note': {'description': 'Save a note', 'command': ['note']}})
        self.assertEqual(plan['action'], 'custom:note')
        self.assertEqual(len(calls), 1)
        with self.assertRaises(jev_router.RouterError):
            jev_router.plan('ask the assistant', snapshot, 'pebble', builtins=False)

    def test_command_speech_receives_text_and_selected_voice(self):
        log = self.root / 'speech.json'
        script = executable(self.root, 'speak', f'#!{sys.executable}\n'
            'import json, os, pathlib, sys\n'
            f'pathlib.Path({str(log)!r}).write_text(json.dumps({{"text": sys.stdin.read(), "voice": os.environ["PEBBLE_SPEECH_VOICE"], "speed": os.environ["PEBBLE_SPEECH_SPEED"]}}))\n')
        voice = Feedback(self.root / 'cache', provider='command', command=[str(script)],
                         env={'PATH': os.environ['PATH'], 'PEBBLE_UI_VOICE': 'ui-voice', 'PEBBLE_UI_SPEED': '1.2'})
        voice.speak('done')
        self.assertEqual(json.loads(log.read_text()), {'text': 'done', 'voice': 'ui-voice', 'speed': '1.2'})


class RouteProfileTest(unittest.TestCase):
    def setUp(self):
        self.base = fixtures.PipelineTest('runTest')
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.config, self.legacy, self.voice = self.base.config, self.base.legacy, self.base.voice
        command = self.config['commands']
        command['caco']['enabled'] = False
        command['extractor']['enabled'] = True
        command.update(pebble_project='', pebble_target='', context_file='',
                       actions={'note': {'description': 'Save a note', 'command': ['note']},
                                'todo': {'description': 'Save a todo', 'command': ['todo']}},
                       routes={'/triage/notes': {'action': 'route', 'profile': 'notes'},
                               '/triage/todo': {'action': 'custom:todo', 'profile': 'default'}},
                       jev_profiles={'notes': {'provider': 'custom', 'model': 'small-router', 'actions': ['note']}})
        command_pipeline.validate_settings(command)
        self.archive = intake.CommandArchive(self.legacy.archive)
        self.calls = []
        def planner(text, snapshot, action, **kwargs):
            self.calls.append((snapshot, action, kwargs))
            return {'version': 1, 'action': 'custom:note', 'text': text, 'executed': False,
                    'action_description': 'Save a note', 'extraction_instruction': ''}
        self.processor = command_pipeline.Processor(self.archive, lambda record: {'text': 'unused'}, planner=planner,
                                                   custom_sender=lambda recipe, env: {'state': 'completed', 'action': recipe['action']},
                                                   feedback_factory=lambda settings: self.voice)

    def test_route_profile_is_frozen_and_selects_its_jev_model_and_actions(self):
        body = json.dumps({'transcription': 'remember the milk', 'recordedAt': '1770000000999', 'client': 'ring'}).encode()
        receipt = self.archive.accept(body, 'application/json', 'route', profile='notes')
        record = self.archive.get(receipt['id'])
        self.assertEqual(json.loads(record['metadata'])['route_profile'], 'notes')
        frozen = self.processor.settings(record)
        self.assertEqual(frozen['commands']['jev']['provider'], 'custom')
        self.assertEqual(frozen['commands']['jev']['model'], 'small-router')
        self.assertEqual(set(frozen['commands']['actions']), {'note'})
        self.processor.route(record, frozen)
        snapshot, action, kwargs = self.calls[-1]
        self.assertEqual(action, 'route')
        self.assertEqual(snapshot['projects'], [])
        self.assertIs(kwargs['builtins'], False)
        self.assertEqual(set(kwargs['actions']), {'note'})

    def test_disabled_caco_rejects_builtin_endpoints_and_profile_conflicts(self):
        body = json.dumps({'transcription': 'hello', 'recordedAt': '1770000000998', 'client': 'ring'}).encode()
        with self.assertRaises(intake.IntakeError) as error:
            self.archive.accept(body, 'application/json', 'pebble')
        self.assertEqual(error.exception.code, 'invalid_endpoint_action')
        first = self.archive.accept(body, 'application/json', 'route', 'same-key', profile='notes')
        second = self.archive.accept(body, 'application/json', 'route', 'same-key', profile='default')
        self.assertIsNone(first['error'])
        self.assertEqual(second['error'], 'input_identity_conflict')


if __name__ == '__main__':
    unittest.main()
