"""Cross the real independently built trg CLI and actual HTTP/archive boundary."""
import json
import os
from pathlib import Path
import subprocess
import unittest
import urllib.request
import test_command_pipeline as fixtures


@unittest.skipUnless(os.environ.get('TRIAGE_CLI'), 'Set TRIAGE_CLI to the tools/cli/triage trg binary')
class TriageClientTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    http = fixtures.PipelineTest.http

    def test_cli_can_read_turn_created_by_generic_capi_client(self):
        with self.http(start_worker=True) as (url, _):
            request=urllib.request.Request(url+'/route/v1/chat/completions',
                data=json.dumps({'model':'triage-route-dry-run','messages':[{'role':'user','content':'Generic client fixture'}]}).encode(),
                headers={'Authorization':'Bearer '+self.legacy.token,'Content-Type':'application/json'})
            with urllib.request.urlopen(request,timeout=5) as response:turn=json.load(response)['id'].removeprefix('chatcmpl-')
            config=self.root/'read-config.json'
            config.write_text(json.dumps({'base_url':url+'/route','token_file':self.config['token_file']}))
            Path(self.config['token_file']).chmod(0o600)
            result=subprocess.run([os.environ['TRIAGE_CLI'],'--config',str(config),'--state-dir',str(self.root/'read-state'),
                '--json','turn','show',turn],capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            value=json.loads(result.stdout)['data']
            self.assertEqual(value['id'],turn)
            self.assertTrue(value['session_id'].startswith('compat-'))
            self.assertEqual(value['phase'],'dry_run')
        self.assertEqual(self.sender.calls,[])

    def test_real_cli_submit_inspect_and_resume_identity(self):
        with self.http(start_worker=True) as (url, _):
            config = self.root/'triage-config.json'
            config.write_text(json.dumps({'base_url':url+'/route', 'token_file':self.config['token_file'], 'mode':'dry_run'}))
            Path(self.config['token_file']).chmod(0o600)
            command = [os.environ['TRIAGE_CLI'], '--config', str(config), '--state-dir', str(self.root/'client-state'), '--json',
                'send', 'An isolated real-client request', '--session', '00000000-0000-4000-8000-000000000001', '--client-turn-id', '00000000-0000-4000-8000-000000000002']
            first = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(first.returncode, 0, first.stderr[-3000:])
            again = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(again.returncode, 0, again.stderr[-3000:])
            one, two = json.loads(first.stdout), json.loads(again.stdout)
            # House JSON envelope, with the exact same server turn under retries.
            self.assertEqual(one['data']['projection']['id'], two['data']['projection']['id'])
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.voice.calls, [])
        with self.archive.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0], 1)
