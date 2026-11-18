"""Real Agent Utils Azure helpers + a fixture az-token and HTTP peer; no audio playback."""
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import test_receiver as fixtures
import command_pipeline
from feedback import Feedback
from process_io import Result, run
import receiver

AGENT_UTILS = os.environ.get('PEBBLE_AGENT_UTILS', '')


@unittest.skipUnless(AGENT_UTILS and Path(AGENT_UTILS, 'extensions/lib/azure-speech-auth.js').exists(),
                     'set PEBBLE_AGENT_UTILS to the broker-aware Agent Utils checkout')
class SpeechBrokerTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ReceiverTest('runTest')
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root, self.config = self.fixture.root, self.fixture.config
        self.calls, self.commands = [], []
        self.token = 'fixture-entra-token-not-a-key'
        self.status = 200
        self.broker_log = self.root / 'broker-calls'
        self.broker = self.root / 'az-token'
        self.broker.write_text(f'''#!{sys.executable}
import json,sys,time
from pathlib import Path
with Path({str(self.broker_log)!r}).open('a') as log: log.write(json.dumps(sys.argv[1:])+'\\n')
if Path({str(self.root / 'broker-fail')!r}).exists(): raise SystemExit(1)
assert sys.argv[1:3] == ['account','get-access-token']
print(json.dumps({{'accessToken':{self.token!r},'expires_on':int(time.time())+3600}}))
''')
        self.broker.chmod(0o700)
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                owner.calls.append((self.path, dict(self.headers), body))
                if self.path == '/mai/v1/audio/transcriptions':
                    data = json.dumps({'text': 'fresh fixture transcription', 'echo': self.headers.get('Authorization')}).encode()
                    mime = 'application/json'
                else:
                    data, mime = fixtures.wav_bytes(), 'audio/wav'
                if owner.status != 200:
                    data, mime = json.dumps({'error': owner.token}).encode(), 'application/json'
                self.send_response(owner.status)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        def close():
            self.server.shutdown(); self.server.server_close(); self.thread.join(3)
        self.addCleanup(close)
        self.base = f'http://127.0.0.1:{self.server.server_port}'
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(('AZURE_', 'PI_TTS_', 'PI_STT_', 'PI_CASCADE_', 'AZ_TOKEN_'))}
        self.env.update(HOME=str(self.root), AZURE_SPEECH_AUTH='entra', AZURE_SPEECH_AZ=str(self.broker),
                        AZURE_SPEECH_ENDPOINT=self.base+'/tts')
        # Missing key is intentional: broker mode must not even read it.
        self.config['transcription'].update(agent_utils_dir=AGENT_UTILS, key_file=str(self.root/'disabled-missing-key'), timeout_secs=8)
        (self.root / 'endpoint').write_text(self.base)

    def transcribe(self):
        with patch.dict(os.environ, self.env, clear=True):
            return receiver.transcribe({'audio': fixtures.wav_bytes(), 'audio_mime': 'audio/wav', 'settings': json.dumps(self.config)})

    def feedback(self):
        def runner(argv, payload=b'', **kwargs):
            self.commands.append((argv, payload))
            if argv[0] == 'fixture-paplay':
                self.assertEqual(Path(argv[1]).read_bytes()[:4], b'RIFF')
                return Result(b'', 0)  # no physical output in tests
            return run(argv, payload, **kwargs)
        return Feedback(self.root/'cache', aztts='must-not-run-old-key-cli', player='fixture-paplay',
                        node=self.config['node'], agent_utils_dir=AGENT_UTILS, env=self.env, runner=runner, timeout=8)

    def assert_broker_auth(self, call):
        headers = {k.lower(): v for k, v in call[1].items()}
        self.assertEqual(headers['authorization'], 'Bearer '+self.token)
        self.assertNotIn('api-key', headers)
        self.assertNotIn('ocp-apim-subscription-key', headers)

    def test_mai_uses_broker_without_key_file_and_redacts_echoed_token(self):
        result = self.transcribe()
        self.assertEqual(result['text'], 'fresh fixture transcription')
        self.assertNotIn(self.token, json.dumps(result))
        self.assertIn('[redacted]', json.dumps(result))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][0], '/mai/v1/audio/transcriptions')
        self.assert_broker_auth(self.calls[0])
        body = fixtures.mai_form(self.calls[0][1], self.calls[0][2])
        self.assertEqual(body['model'], self.config['transcription']['deployment'])
        self.assertEqual(body['file'][:4], b'RIFF')
        self.assertEqual(len(self.broker_log.read_text().splitlines()), 1)

    def test_both_speech_roles_use_shared_broker_helper_and_keep_prefix_cache(self):
        feedback = self.feedback()
        feedback.acknowledge('fresh fixture speech')
        feedback.acknowledge('second fresh fixture speech')
        self.assertEqual(len(self.calls), 3)  # prefix once, two self readbacks
        for call in self.calls:
            self.assertEqual(call[0], '/tts/cognitiveservices/v1')
            self.assert_broker_auth(call)
        ui, own = self.calls[0][2].decode(), self.calls[1][2].decode()
        self.assertIn('de-DE-Mia:MAI-Voice-2.1-Flash', ui)
        self.assertNotIn('speakerProfileId=', ui)
        self.assertEqual(feedback.self_voice.embedding, '')
        self.assertIn('pt-BR-Caio:MAI-Voice-2.1-Flash', own)
        self.assertNotIn('speakerProfileId=', own)
        self.assertEqual(sum(argv[0] == 'fixture-paplay' for argv, _ in self.commands), 4)
        for argv, _ in self.commands:
            self.assertNotIn(self.token, ' '.join(argv))
            self.assertNotIn('fresh fixture speech', ' '.join(argv))
            self.assertNotIn('must-not-run-old-key-cli', argv)

    def test_broker_failure_has_no_provider_request_or_key_fallback(self):
        (self.root / 'broker-fail').touch()
        self.env['AZURE_SPEECH_API_KEY'] = 'disabled-old-key'
        with self.assertRaises(receiver.Failure) as error:
            self.transcribe()
        self.assertNotIn(self.token, str(error.exception))
        with self.assertRaises(Exception): self.feedback().speak('fixture')
        self.assertEqual(self.calls, [])
        self.assertFalse(any(argv[0] == 'fixture-paplay' for argv, _ in self.commands))

    def test_provider_rejection_is_redacted_and_does_not_replay(self):
        self.status = 401
        with self.assertRaises(receiver.Failure) as caught: self.transcribe()
        self.assertNotIn(self.token, json.dumps(caught.exception.response))
        self.assertEqual(len(self.calls), 1)
        self.assert_broker_auth(self.calls[0])

    def test_pipeline_supplies_current_node_and_adapter_to_feedback(self):
        self.config['commands'] = json.loads((fixtures.ROOT/'tests/fixtures/commands-caco.json').read_text())
        archive = command_pipeline.CommandArchive(self.fixture.archive)
        processor = command_pipeline.Processor(archive, lambda _: None)
        result = processor.feedback(self.config)
        self.assertEqual(result.node, self.config['node'])
        self.assertEqual(result.agent_utils_dir, AGENT_UTILS)


if __name__ == '__main__': unittest.main()
