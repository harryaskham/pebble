import json
import os
from pathlib import Path
import time
import unittest
import urllib.request
import urllib.error

import test_command_pipeline as fixtures
import status_feed


class StatusFeedTest(unittest.TestCase):
    setUp=fixtures.PipelineTest.setUp
    drain=fixtures.PipelineTest.drain
    http=fixtures.PipelineTest.http
    text=fixtures.PipelineTest.text

    def state_file(self, value):
        path=self.root/'attachment.json'
        path.write_text(json.dumps(value))
        self.config['commands']['status']['attachment_file']=str(path)
        return path

    def get(self,url,token=None):
        request=urllib.request.Request(url,headers={'Authorization':'Bearer '+(token or self.legacy.token)})
        return urllib.request.urlopen(request,timeout=5)

    def test_snapshot_is_bounded_private_and_non_mutating(self):
        self.state_file({'version':1,'attachment':{'kind':'agent','project':'sdk','target':'node-a:sdk:sdk-worker'},'last_message':{'body':'DO NOT LEAK'},'recent':['secret']})
        for i in range(12):self.text('Private original '+str(i),recorded=str(i))
        self.drain(limit=60)
        before=self.archive.status();sound=list(self.voice.calls);sends=list(self.sender.calls)
        with self.http() as (url,worker):
            with self.get(url+'/pebble/status?limit=3') as response:
                value=json.load(response)
                self.assertEqual(response.headers['Cache-Control'],'no-store')
            self.assertEqual(len(value['recent']),3)
            self.assertEqual(value['attachment']['state'],'attached')
            self.assertFalse(value['attachment']['runtime_verified'])
            self.assertNotIn('DO NOT LEAK',json.dumps(value));self.assertNotIn('Private original',json.dumps(value))
            self.assertNotIn(self.legacy.token,json.dumps(value))
            self.assertTrue(all('preview' not in r for r in value['recent']))
            with self.get(url+'/pebble/status?include_text=true&limit=1') as response:explicit=json.load(response)
            self.assertEqual(explicit['recent'][0]['preview'],'Private original 11')
            self.assertEqual(status_feed.snapshot(self.archive,worker)['revision'],status_feed.snapshot(self.archive,worker)['revision'])
        self.assertEqual(self.archive.status(),before);self.assertEqual(self.voice.calls,sound);self.assertEqual(self.sender.calls,sends)

    def test_custom_registry_and_status_capabilities_work_together(self):
        self.config['commands']['actions'] = {'note': {'description': 'Save a note', 'command': ['never-expose-this-command']}}
        fixtures.command_pipeline.validate_settings(self.config['commands'])
        self.state_file({'version': 1, 'attachment': None})
        with self.http() as (url, _):
            with self.get(url+'/pebble/status') as response:value=json.load(response)
            self.assertIn('custom:note',value['actions'])
            self.assertNotIn('never-expose-this-command',json.dumps(value))
            request=urllib.request.Request(url+'/pebble/action/note',data=b'{"test":true}',headers={
                'Authorization':'Bearer '+self.legacy.token,'Content-Type':'application/json'})
            with urllib.request.urlopen(request,timeout=5) as response:probe=json.load(response)
            self.assertTrue(probe['status_feed_supported'])
            self.assertTrue(probe['action_manifest_supported'])
            self.assertIn('custom:note',probe['supported_actions'])
        self.assertEqual(self.sender.calls,[])

    def test_unknown_and_saved_detached_are_not_runtime_liveness(self):
        self.assertEqual(status_feed.attachment(self.root/'missing')['state'],'unknown')
        path=self.state_file({'version':1,'attachment':None,'recent':[]})
        self.assertEqual(status_feed.attachment(path)['state'],'detached')
        path.write_text('{bad')
        self.assertEqual(status_feed.attachment(path)['state'],'unknown')
        path.unlink();os.mkfifo(path)
        self.assertEqual(status_feed.attachment(path)['state'],'unknown')

    def test_authentication_query_validation_and_alias(self):
        self.state_file({'version':1,'attachment':None})
        with self.http() as (url,_):
            for suffix in ('/pebble/status','/pebble/events'):
                with self.assertRaises(urllib.error.HTTPError) as error:self.get(url+suffix,'wrong')
                self.assertEqual(error.exception.code,401);error.exception.close()
            for query in ('?limit=0','?limit=100','?include_text=maybe','?limit=2&limit=3','?token=secret','?seconds=26'):
                with self.assertRaises(urllib.error.HTTPError) as error:self.get(url+'/pebble/status'+query)
                self.assertEqual(error.exception.code,400);error.exception.close()
            with self.get(url+'/hooks/global/pebble-status') as response:self.assertEqual(json.load(response)['version'],1)

    def test_sse_change_feed_reserves_intake_and_observes_revocation(self):
        self.state_file({'version':1,'attachment':None})
        with self.http() as (url,_):
            with self.get(url+'/pebble/events?seconds=5') as stream:
                self.assertTrue(stream.headers['Content-Type'].startswith('text/event-stream'))
                self.assertEqual(stream.readline().strip(),b'retry: 5000')
                stream.readline()
                self.assertTrue(stream.readline().startswith(b'id: '))
                self.assertEqual(stream.readline().strip(),b'event: status')
                first=json.loads(stream.readline().removeprefix(b'data: '));stream.readline()
                self.assertEqual(first['service']['requests'],0)
                with self.assertRaises(urllib.error.HTTPError) as error:self.get(url+'/pebble/events?seconds=1')
                self.assertEqual(error.exception.code,503);error.exception.close()
                # A stream does not stop another authenticated reader or intake.
                r=self.text('fresh text')
                with self.get(url+'/pebble/status') as response:self.assertEqual(json.load(response)['recent'][0]['id'],r['id'])
                lines=[]
                while len(lines)<5:
                    line=stream.readline();lines.append(line)
                    if line.startswith(b'data: '):break
                self.assertTrue(any(line.startswith(b'data: ') and r['id'].encode() in line for line in lines))
                Path(self.config['token_file']).write_text('rotated-fixture-token')
                # Existing authorization is rechecked without a new HTTP header.
                start=time.monotonic();stream.read();self.assertLess(time.monotonic()-start,3)
            self.assertEqual(self.sender.calls,[])

    def test_stream_close_releases_slot_and_snapshot_counters(self):
        self.state_file({'version':1,'attachment':None})
        r=self.text('failed request')
        self.archive.update(r['id'],phase='blocked',error='routing_unavailable',error_stage='route')
        with self.http() as (url,_):
            with self.get(url+'/pebble/events?seconds=1') as stream:
                body=stream.read()
                self.assertIn(b'event: status',body)
                self.assertIn(b'heartbeat',body)
            with self.get(url+'/pebble/events?seconds=1') as stream:stream.read()
            with self.get(url+'/pebble/status') as response:value=json.load(response)
            self.assertEqual(value['service']['blocked'],1)
            self.assertEqual(value['recent'][0]['error'],'routing_unavailable')
