"""Compose the actual Pi provider with the actual Triage HTTP/archive adapter."""
import json
import os
from pathlib import Path
import subprocess
import unittest
import test_command_pipeline as fixtures


@unittest.skipUnless(os.environ.get('TRIAGE_PI_BIN'), 'Set TRIAGE_PI_BIN to the reviewed Pi 0.84.4 binary')
class PiServerTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    http = fixtures.PipelineTest.http

    def test_real_pi_against_real_router_adapter_has_one_dry_turn(self):
        root=self.root/'pi-client';root.mkdir(mode=0o700)
        Path(self.config['token_file']).chmod(0o600)
        text='File a bead in sdk: composed Pi router fixture'
        with self.http(start_worker=True) as (url,_):
            config=root/'connector.json'
            config.write_text(json.dumps({'baseUrl':url+'/route/v1','tokenFile':self.config['token_file'],
                'stateDir':str(root/'journal'),'allowLive':False,'timeoutMs':10000,'maxTokens':4096}))
            env={**os.environ,'HOME':str(root),'PI_CODING_AGENT_DIR':str(root/'agent'),
                'XDG_CONFIG_HOME':str(root/'config'),'XDG_CACHE_HOME':str(root/'cache'),
                'TRG_PI_CONFIG':str(config),'NO_PROXY':'127.0.0.1,localhost'}
            extension=Path(__file__).resolve().parents[1]/'pi/index.ts'
            result=subprocess.run([os.environ['TRIAGE_PI_BIN'],'--print','--no-session','--offline',
                '--no-extensions','--no-context-files','--no-skills','--no-prompt-templates',
                '--no-themes','--no-tools','-e',str(extension),'--provider','triage',
                '--model','triage-route-dry-run',text],env=env,cwd=root,capture_output=True,text=True,timeout=25)
        self.assertEqual(result.returncode,0,(result.stdout+result.stderr)[-2500:])
        self.assertIn('No action executed',result.stdout)
        self.assertNotIn(self.legacy.token,result.stdout+result.stderr)
        with self.archive.db() as db:
            rows=db.execute('SELECT text,phase FROM command_inputs').fetchall()
            self.assertEqual(len(rows),1)
            self.assertEqual(tuple(rows[0]),(text,'dry_run'))
            self.assertEqual(db.execute('SELECT sum(attempts) FROM command_actions').fetchone()[0],0)
        self.assertEqual(self.sender.calls,[]);self.assertEqual(self.voice.calls,[])
