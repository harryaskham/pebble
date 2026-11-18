"""Opt-in real ctx daemon/CLI watch integration, with private state and no fleet I/O."""
import copy
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest

from test_context_from_ctx import fixture, record

BINARY = os.environ.get('PEBBLE_TEST_CTX')
SOURCE = Path(__file__).resolve().parents[1]


@unittest.skipUnless(BINARY and sys.platform == 'linux', 'set PEBBLE_TEST_CTX for the Linux real-ctx integration')
class CtxContextIntegrationTest(unittest.TestCase):
    def test_subscription_reconnect_rotation_preservation_and_cleanup(self):
        with tempfile.TemporaryDirectory(prefix='pebble-ctx-integration-') as directory:
            root=Path(directory);home=root/'home';home.mkdir(mode=0o700)
            token=root/'private-token';token.write_text('fixture-ctx-private-token-0123456789');token.chmod(0o600)
            link=root/'managed-token';link.symlink_to(token)
            socket=root/'ctx.sock'
            config=root/'ctx.json'
            config.write_text(json.dumps({
                'client':{'daemon-url':'unix://'+str(socket),'token-file':str(link),'timeout-secs':2},
                'daemon':{'bind':None,'unix-socket':str(socket),'database':str(root/'ctx.sqlite'),
                          'token-file':str(link),'durability':'normal'},
            }))
            env={'HOME':str(home),'PATH':os.environ['PATH'],'PYTHONDONTWRITEBYTECODE':'1','NO_COLOR':'1'}
            command=[BINARY,'--config',str(config),'--json']
            daemon=None;watcher=None;child_pids=[]
            with (root/'daemon.log').open('wb') as daemon_log:
                def start():
                    return subprocess.Popen([*command,'daemon'],env=env,stdout=daemon_log,stderr=daemon_log,start_new_session=True)
                def cli(*args, payload=None):
                    result=subprocess.run([*command,*args],input=payload,env=env,capture_output=True,timeout=6)
                    self.assertEqual(result.returncode,0,result.stderr.decode()[:500])
                    return json.loads(result.stdout)
                def until(predicate, timeout=15):
                    deadline=time.monotonic()+timeout
                    while time.monotonic()<deadline:
                        if daemon is not None and daemon.poll() is not None:
                            self.fail('ctx fixture exited: '+(root/'daemon.log').read_text()[-1200:])
                        if predicate():return
                        time.sleep(.02)
                    self.fail('fixture did not reach expected state')
                def stop(proc):
                    if proc is not None and proc.poll() is None:
                        proc.terminate()
                        try:proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill();proc.wait(timeout=3)
                            self.fail('owned fixture process did not stop')
                def publish(data):
                    ops=[{'op':'set','key':entry['key'],'value':entry['value']} for entry in data['entries']]
                    file=root/'batch.json';file.write_text(json.dumps(ops))
                    return cli('batch',str(file))
                target=root/'cache/context.json';target.parent.mkdir()
                output=root/'context.json';output.symlink_to('cache/context.json')
                def snapshot():
                    try:return json.loads(output.read_text())
                    except (FileNotFoundError,ValueError):return {}
                def expect_error(code):
                    deadline=time.monotonic()+15
                    while time.monotonic()<deadline:
                        readable,_,_=select.select([watcher.stderr],[],[],.1)
                        if readable:
                            line=watcher.stderr.readline()
                            if line and json.loads(line).get('error')==code:return
                    self.fail('expected subscriber diagnostic not observed: '+code)
                try:
                    daemon=start();until(socket.exists)
                    self.assertEqual(cli('status')['data']['entries'],0)
                    data=fixture(time.time());publish(data)
                    watcher=subprocess.Popen([sys.executable,str(SOURCE/'context_from_ctx.py'),
                        '--ctx',BINARY,'--ctx-config',str(config),'--output',str(output),
                        '--watch','--watch-timeout','2'],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                        bufsize=0,start_new_session=True)
                    until(lambda:snapshot().get('version')==1)
                    first=snapshot();self.assertEqual(first['projects'][0]['id'],'a.skh.am')
                    self.assertEqual(first['agents'][0]['target'],'node-a:a.skh.am:visiting-one')
                    until(lambda:cli('status')['data']['watchers']>0)
                    # Real subscription, not a refresh timer: publish a complete
                    # update while the watch is already connected.
                    changed=copy.deepcopy(data)
                    record(changed,'cacophony.projects.a_skh_am-hash')['description']='Changed through subscription'
                    record(changed,'cacophony.agents.a_skh_am-hash.visiting-one')['state']='stopped'
                    sync=record(changed,'cacophony.sync');sync['observed_at']=time.time()
                    for section in sync['sections'].values():section['observed_at']=sync['observed_at']
                    started=time.monotonic();publish(changed)
                    until(lambda:snapshot().get('projects',[{}])[0].get('description')=='Changed through subscription')
                    latency=time.monotonic()-started
                    self.assertEqual(snapshot()['agents'],[])
                    before=output.read_bytes();stamp=target.stat().st_mtime_ns
                    bad=copy.deepcopy(changed);record(bad,'cacophony.sync')['status']='failed'
                    publish(bad);expect_error('ctx_sync_unavailable')
                    self.assertEqual(output.read_bytes(),before)
                    self.assertEqual(target.stat().st_mtime_ns,stamp)
                    self.assertTrue(output.is_symlink())
                    # Unavailable daemon cannot erase or rejuvenate the cache.
                    stop(daemon);daemon=None
                    expect_error('command_failed')
                    self.assertEqual(output.read_bytes(),before)
                    self.assertEqual(target.stat().st_mtime_ns,stamp)
                    # Rotate the managed token and restart the same durable ctx
                    # database. New CLI acquisitions must use the new token.
                    token.write_text('fixture-ctx-rotated-token-0123456789');token.chmod(0o600)
                    daemon=start();until(socket.exists)
                    good=copy.deepcopy(changed)
                    record(good,'cacophony.projects.a_skh_am-hash')['description']='Recovered with rotated credential'
                    sync=record(good,'cacophony.sync');sync['observed_at']=time.time()
                    for section in sync['sections'].values():section['observed_at']=sync['observed_at']
                    publish(good)
                    until(lambda:snapshot().get('projects',[{}])[0].get('description')=='Recovered with rotated credential',timeout=20)
                    self.assertTrue(link.is_symlink());self.assertTrue(output.is_symlink())
                    until(lambda:cli('status')['data']['watchers']>0)
                    children=Path(f'/proc/{watcher.pid}/task/{watcher.pid}/children')
                    child_pids=[int(pid) for pid in children.read_text().split()]
                    self.assertTrue(child_pids)
                    stop(watcher);self.assertEqual(watcher.returncode,0)
                    until(lambda:cli('status')['data']['watchers']==0)
                    for pid in child_pids:self.assertFalse(Path(f'/proc/{pid}').exists(),'ctx watch child leaked')
                    stdout=watcher.stdout.read().decode();stderr=watcher.stderr.read().decode()
                    self.assertNotIn('fixture-ctx-private-token',stdout+stderr)
                    self.assertNotIn('fixture-ctx-rotated-token',stdout+stderr)
                    self.assertNotIn('Changed through subscription',stdout+stderr)
                    print(f'ctx integration: subscription-to-file {latency:.3f}s; offline/rotation/cleanup passed')
                finally:
                    stop(watcher);stop(daemon)
                    if watcher:
                        watcher.stdout.close();watcher.stderr.close()
                self.assertFalse(socket.exists())


if __name__=='__main__':unittest.main()
