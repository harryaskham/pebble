import os
from pathlib import Path
import subprocess
import unittest
import test_command_pipeline as fixtures


@unittest.skipUnless(os.environ.get('TRIAGE_OPENAI_SDK'), 'Set TRIAGE_OPENAI_SDK to the real OpenAI Node SDK index.mjs')
class TriageSdkTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    http = fixtures.PipelineTest.http

    def test_real_sdk_stream_nonstream_retry_and_conflict(self):
        self.legacy.token = 'fixture-sdk-token'
        Path(self.config['token_file']).write_text(self.legacy.token)
        with self.http(start_worker=True) as (url, _):
            result = subprocess.run(['node', str(Path(__file__).with_name('triage-sdk.mjs'))],
                env={**os.environ, 'TRIAGE_TEST_URL': url}, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        self.assertIn('"retry_identity":true', result.stdout)
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.voice.calls, [])
        with self.archive.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0], 1)
