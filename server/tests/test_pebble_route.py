"""Real client processes against a local webhook; never record a real microphone."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import pty
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.parse
import wave

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from intake import parse_input


class ClientTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='pebble-client-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.token = self.root/'token'
        self.token.write_text('fixture-pebble-token\n')
        self.token.chmod(0o600)
        self.calls = []
        self.supported = True
        self.redirect = None
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a): pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                kind = self.headers['Content-Type']
                meta, audio = parse_input(body, kind)
                fixture.calls.append({'path': self.path, 'headers': dict(self.headers), 'metadata': meta, 'audio': audio})
                if fixture.redirect:
                    self.send_response(307)
                    self.send_header('Location', fixture.redirect)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                dry = self.headers.get('X-Pebble-Dry-Run') == 'true'
                if meta['test']:
                    value = {'phase':'probe', 'archived':False, 'dry_run_supported':fixture.supported}
                    status = 200
                else:
                    value = {'id':'a'*32, 'phase':'dry_ready' if dry else 'ready', 'archived':True, 'dry_run':dry}
                    status = 202
                encoded = json.dumps(value).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        def cleanup():
            self.server.shutdown(); self.server.server_close(); self.thread.join(3)
            self.assertFalse(self.thread.is_alive())
        self.addCleanup(cleanup)
        self.url = f'http://127.0.0.1:{self.server.server_port}/hooks/global/pebble-route'
        self.ffmpeg = shutil.which('ffmpeg')

    def argv(self, *args):
        return [sys.executable, str(ROOT.parent/'scripts/pebble-route'), '--url', self.url, '--token-file', str(self.token), *args]

    def run_client(self, *args, data=None, env=None):
        return subprocess.run(self.argv(*args), input=data, stdin=subprocess.DEVNULL if data is None else None,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=12, env=env)

    def check_wav(self, call):
        with wave.open(io.BytesIO(call['audio']), 'rb') as audio:
            self.assertEqual((audio.getframerate(), audio.getnchannels(), audio.getsampwidth()), (16000, 1, 2))
            self.assertGreater(audio.getnframes(), 0)
        self.assertEqual(call['metadata']['client'], 'pebble-route')
        self.assertTrue(call['metadata']['recordedAt'].isdigit())

    def require_ffmpeg(self):
        self.assertIsNotNone(self.ffmpeg, 'ffmpeg required for executable-boundary tests')

    def test_text_preserved_and_dry_header_and_query(self):
        text = '  keep $(these) “words”\nverbatim  '
        result = self.run_client('--text', text, '--dry-run')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(result.stdout)['dry_run'])
        self.assertEqual(len(self.calls), 2)
        self.assertTrue(self.calls[0]['metadata']['test'])
        call = self.calls[1]
        self.assertEqual(call['metadata']['transcription'], text)
        self.assertIsNone(call['audio'])
        self.assertEqual(call['headers']['Authorization'], 'Bearer fixture-pebble-token')
        self.assertEqual(call['headers']['X-Pebble-Dry-Run'], 'true')
        self.assertEqual(urllib.parse.urlsplit(call['path']).query, 'dry_run=true')
        self.assertTrue(call['headers']['Idempotency-Key'].startswith('pebble-route-'))
        self.assertNotIn(b'fixture-pebble-token', result.stdout+result.stderr)

    def test_unsupported_dry_server_receives_only_probe(self):
        self.supported = False
        result = self.run_client('--text', 'must not execute', '--dry-run')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(self.calls[0]['metadata']['test'])

    def test_live_text_one_upload_and_managed_token_symlink(self):
        link = self.root/'linked-token'; link.symlink_to(self.token)
        result = self.run_client('--text', 'a live fixture request', '--token-file', str(link))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.calls), 1)
        self.assertNotIn('X-Pebble-Dry-Run', self.calls[0]['headers'])

    def test_redirect_is_not_followed_and_not_retried(self):
        self.redirect = self.url+'/foreign'
        result = self.run_client('--text', 'test')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.calls), 1)
        self.assertIn(b'HTTP 307', result.stderr)
        self.assertNotIn(b'fixture-pebble-token', result.stderr)

    def test_pcm_stdin_waits_for_eof_then_normalizes_pacat_defaults(self):
        self.require_ffmpeg()
        proc = subprocess.Popen(self.argv('--audio', '-'), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            proc.stdin.write(b'\0'*17640); proc.stdin.flush()  # .1s, 44100Hz stereo s16
            time.sleep(.2)
            self.assertEqual(self.calls, [])
            proc.stdin.close(); proc.stdin = None
            stdout, stderr = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 0, stderr)
            self.assertEqual(json.loads(stdout)['phase'], 'ready')
            self.check_wav(self.calls[0])
        finally:
            if proc.poll() is None: proc.kill()
            proc.communicate()

    def test_pcm_stdin_ctrl_c_finishes_and_sends(self):
        self.require_ffmpeg()
        proc = subprocess.Popen(self.argv('--audio', '-', '--rate', '16000', '--channels', '1'),
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            proc.stdin.write(b'\0'*3200); proc.stdin.flush()
            self.assertTrue(select.select([proc.stderr], [], [], 3)[0])
            self.assertIn(b'reading audio', proc.stderr.readline())
            time.sleep(.1)
            proc.send_signal(signal.SIGINT)
            stdout, stderr = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 0, stderr)
            self.check_wav(self.calls[0])
        finally:
            if proc.poll() is None: proc.kill()
            proc.communicate()

    def test_wav_file_upload_and_empty_bad_pcm_rejected(self):
        self.require_ffmpeg()
        path = self.root/'sample.wav'
        with wave.open(str(path), 'wb') as audio:
            audio.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
            audio.writeframes(b'\0'*3200)
        result = self.run_client('--audio', str(path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.check_wav(self.calls[0])
        self.calls.clear()
        for value in (b'', b'\0\0\0'):
            result = self.run_client('--audio', '-', data=value)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(self.calls, [])

    def test_m4a_file_is_decoded_before_upload(self):
        self.require_ffmpeg()
        source = self.root/'source.wav'
        with wave.open(str(source), 'wb') as audio:
            audio.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
            audio.writeframes(b'\0'*6400)
        encoded = self.root/'source.m4a'
        subprocess.run([self.ffmpeg, '-nostdin', '-v', 'error', '-i', str(source), '-c:a', 'aac', str(encoded)],
                       check=True, timeout=8, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        result = self.run_client('--audio', str(encoded))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.check_wav(self.calls[0])

    def test_overlong_pcm_is_rejected_without_upload(self):
        result = self.run_client('--audio', '-', '--rate', '8000', '--channels', '1', data=b'\0'*(8000*2*121))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'exceeds 120 seconds', result.stderr)
        self.assertEqual(self.calls, [])

    def fake_pacat(self):
        script = self.root/'pacat'
        marker = self.root/'capture.json'
        script.write_text(f'''#!{sys.executable}
import json,os,sys,time
from pathlib import Path
Path({str(marker)!r}).write_text(json.dumps({{'pid':os.getpid(),'argv':sys.argv[1:]}}))
while True:
 os.write(1, b'\\0'*640)
 time.sleep(.01)
''')
        script.chmod(0o700)
        return script, marker

    def wait_marker(self, marker):
        deadline = time.monotonic()+4
        while time.monotonic() < deadline:
            if marker.exists() and marker.stat().st_size:
                return json.loads(marker.read_text())
            time.sleep(.02)
        self.fail('capture did not start')

    def test_timed_pulse_capture_forwards_server_source_and_cleans_up(self):
        self.require_ffmpeg()
        pacat, marker = self.fake_pacat()
        result = self.run_client('--audio', '--seconds', '.1', '--server=fixture:4713', '--source=fixture-source', '--pacat', str(pacat))
        self.assertEqual(result.returncode, 0, result.stderr)
        meta = self.wait_marker(marker)
        self.assertIn('fixture:4713', meta['argv']); self.assertIn('fixture-source', meta['argv'])
        self.assertIn('--rate=16000', meta['argv']); self.assertIn('--channels=1', meta['argv'])
        with self.assertRaises(ProcessLookupError): os.kill(meta['pid'], 0)
        self.check_wav(self.calls[0])

    def test_terminal_ctrl_d_and_ctrl_c_finish_owned_capture(self):
        self.require_ffmpeg()
        for finish in ('ctrl-d', 'ctrl-c', 'enter'):
            with self.subTest(finish=finish):
                pacat, marker = self.fake_pacat()
                marker.unlink(missing_ok=True)
                master, slave = pty.openpty()
                proc = subprocess.Popen(self.argv('--audio', '--pacat', str(pacat)), stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                os.close(slave)
                try:
                    meta = self.wait_marker(marker)
                    time.sleep(.1)
                    if finish == 'ctrl-c': proc.send_signal(signal.SIGINT)
                    else: os.write(master, b'\x04' if finish == 'ctrl-d' else b'\n')
                    stdout, stderr = proc.communicate(timeout=10)
                    self.assertEqual(proc.returncode, 0, stderr)
                    self.check_wav(self.calls[-1])
                    with self.assertRaises(ProcessLookupError): os.kill(meta['pid'], 0)
                finally:
                    if proc.poll() is None: proc.kill()
                    proc.communicate()
                    os.close(master)

    def test_failed_pulse_capture_does_not_send_partial_audio(self):
        self.require_ffmpeg()
        script = self.root/'bad-pacat'
        script.write_text(f'#!{sys.executable}\nimport os\nos.write(1,b"\\0"*640)\nraise SystemExit(1)\n')
        script.chmod(0o700)
        result = self.run_client('--audio', '--seconds', '1', '--pacat', str(script))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.calls, [])

    def test_invalid_options_or_unsafe_token_do_not_send(self):
        for args in [('--text', ''), ('--text', 'x', '--audio'), ('--audio', '-', '--source', 'x'),
                     ('--audio', '--seconds', '121'), ('--text', 'x', '--dry-run', '--url', self.url+'?dry_run=false')]:
            result = self.run_client(*args)
            self.assertNotEqual(result.returncode, 0)
        self.token.chmod(0o644)
        self.assertNotEqual(self.run_client('--text', 'x').returncode, 0)
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
