import concurrent.futures
import json
import sqlite3
import threading
import unittest
import urllib.error
import urllib.request
from email.message import Message
from unittest.mock import patch
import test_command_pipeline as fixtures
import triage_store
import triage_http


class TriageTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp
    drain = fixtures.PipelineTest.drain
    http = fixtures.PipelineTest.http

    def store(self):
        store = triage_store.Turns(self.archive)
        store.session('fixture-session')
        return store

    def turn(self, key='first', **extra):
        return {'client_turn_id': key, 'text': 'File a small test bead', 'mode': 'dry_run', **extra}

    def request(self, url, body=None, **headers):
        req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None,
            headers={'Authorization': 'Bearer '+self.legacy.token, 'Content-Type': 'application/json', **headers})
        return urllib.request.urlopen(req, timeout=6)

    def test_atomic_admission_concurrent_retry_and_conflict(self):
        store = self.store()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            rows = list(pool.map(lambda _: store.submit('fixture-session', self.turn()), range(8)))
        self.assertEqual(len({x['id'] for x in rows}), 1)
        with self.archive.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0], 1)
            self.assertEqual(db.execute('SELECT count(*) FROM command_requests').fetchone()[0], 1)
        with self.assertRaises(triage_store.Error) as err: store.submit('fixture-session', self.turn(mode='live'))
        self.assertEqual(err.exception.status, 409)
        self.drain()
        final = store.get(rows[0]['id'])
        self.assertEqual(final['phase'], 'dry_run')
        self.assertEqual(self.sender.calls, [])
        self.assertEqual(self.voice.calls, [])
        self.assertTrue(all(s['state']=='delegated' for s in final['receipt']['speech']))
        text = store.response_text(final)
        self.assertIn('Would execute bd-file', text)
        self.assertIn('No action executed', text)
        self.assertEqual(triage_store.Turns(self.archive).get(final['id'])['events'], final['events'])

    def test_projection_receipt_and_cursor_share_one_sqlite_snapshot(self):
        store=self.store(); original=store.submit('fixture-session',self.turn())
        entered,release=threading.Event(),threading.Event(); observed=[]
        receipt=self.archive.receipt
        def paused(*args,**kwargs):
            if threading.current_thread().name=='snapshot-reader':
                entered.set()
                if not release.wait(3):raise AssertionError('reader release timed out')
            return receipt(*args,**kwargs)
        with patch.object(self.archive,'receipt',side_effect=paused):
            reader=threading.Thread(name='snapshot-reader',target=lambda:observed.append(store.get(original['id'])))
            reader.start()
            try:
                self.assertTrue(entered.wait(2))
                cancelled=store.cancel(original['id'])
                self.assertEqual(cancelled['phase'],'skipped')
            finally:
                release.set();reader.join(3)
            self.assertFalse(reader.is_alive())
        self.assertEqual(observed[0]['phase'],original['phase'])
        self.assertEqual(observed[0]['receipt']['phase'],original['receipt']['phase'])
        self.assertEqual(observed[0]['cursor'],original['cursor'])
        self.assertEqual(observed[0]['events'],original['events'])
        self.assertEqual(store.get(original['id'])['phase'],'skipped')

    def test_transaction_failure_leaves_no_orphan_command(self):
        store = self.store()
        with self.archive.db() as db:
            db.execute("CREATE TRIGGER fail_turn BEFORE INSERT ON triage_turns BEGIN SELECT RAISE(ABORT,'fixture'); END")
        with self.assertRaises(sqlite3.IntegrityError): store.submit('fixture-session', self.turn())
        with self.archive.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0], 0)
        self.assertFalse(self.processor.step())

    def test_inspect_is_linked_read_only_and_scoped(self):
        store = self.store(); first = store.submit('fixture-session', self.turn(mode='live')); self.drain()
        result = store.submit('fixture-session', self.turn('status', operation='inspect', reference_turn=first['id']))
        self.assertEqual(len(self.sender.calls), 1)
        self.assertTrue(result['done'])
        self.assertIn('Created', store.response_text(result))
        store.session('other-session')
        with self.assertRaises(triage_store.Error): store.submit('other-session', self.turn(operation='inspect', reference_turn=first['id']))

    def test_inspection_quota_preserves_worker_storage_reserve(self):
        store=self.store(); first=store.submit('fixture-session',self.turn()); self.drain()
        used=sum(p.stat().st_size for p in self.archive.root.glob('archive.sqlite3*'))
        self.config['max_archive_bytes']=used+4096
        with self.assertRaises(triage_store.Error) as error:
            store.submit('fixture-session',self.turn('inspect',text='x'*16384,operation='inspect',reference_turn=first['id']))
        self.assertEqual(error.exception.code,'archive_quota_reached')
        with self.archive.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM triage_turns').fetchone()[0],1)

    def test_inspections_cannot_evict_source_events_and_pruning_is_explicit(self):
        store=self.store(); first=store.submit('fixture-session',self.turn())
        self.processor.step()
        for i in range(260):store.submit('fixture-session',self.turn('inspect-'+str(i),operation='inspect',reference_turn=first['id']))
        self.drain()
        events=store.events(first['id'],first['cursor'])
        self.assertTrue(any(e['kind']=='dry_routing' for e in events['events']))
        with self.archive.db() as db:input_id=db.execute('SELECT input_id FROM triage_turns WHERE id=?',(first['id'],)).fetchone()[0]
        for i in range(270):self.archive.update(input_id,phase='dry_ready' if i%2 else 'dry_routing')
        with self.assertRaises(triage_store.Error) as error:store.events(first['id'],first['cursor'])
        self.assertEqual(error.exception.code,'cursor_expired_resync_turn')
        self.assertEqual(len(store.events(first['id'])['events']),256)
        self.assertGreater(store.events(first['id'])['retention_floor'],0)

    def test_action_manifest_failure_is_structured_pre_admission(self):
        self.config['commands']['actions_file']=str(self.root/'missing.yaml')
        with self.http() as (url,_):
            with self.request(url+'/route/sessions',{'id':'manifest'}) as r:r.read()
            body={'model':'triage-route-dry-run','messages':[{'role':'user','content':'File a fixture'}]}
            for path,value in (('/route/sessions/manifest/turns',self.turn()),('/route/v1/chat/completions',body)):
                with self.assertRaises(urllib.error.HTTPError) as error:self.request(url+path,value)
                self.assertEqual(error.exception.code,503)
                self.assertEqual(json.load(error.exception)['error']['code'],'action_manifest_unavailable')
                error.exception.close()
        with self.archive.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0],0)

    def test_unknown_route_rolls_back_and_no_truncated_text(self):
        store = self.store()
        for extra in ({'route':'fake'}, {'text':'x'*16385}, {'reference_turn':'guessed'}, {'mode':'maybe'}):
            with self.assertRaises(triage_store.Error): store.submit('fixture-session', self.turn(**extra))
        with self.archive.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0], 0)

    def test_chat_history_compatibility_and_live_identity(self):
        headers=Message()
        body={'model':'triage-route-dry-run','messages':[{'role':'user','content':'same request'}]}
        first=triage_http.parse_chat(body,headers)
        body['messages'] += [{'role':'assistant','content':'reply'}, {'role':'tool','content':'untrusted output'}]
        self.assertEqual(first['request'],triage_http.parse_chat(body,headers)['request'])
        body['model']='triage-route'
        with self.assertRaises(triage_store.Error):triage_http.parse_chat(body,headers)
        headers['X-Triage-Session-ID']='stable';headers['Idempotency-Key']='turn';headers['X-Triage-Purpose']='user'
        self.assertEqual(triage_http.parse_chat(body,headers)['request']['mode'],'live')
        with self.assertRaises(triage_store.Error):triage_http.parse_chat({**body,'triage':{'purpose':'auxiliary'}},headers)
        headers['X-Pebble-Dry-Run']='true'
        with self.assertRaises(triage_store.Error):triage_http.parse_chat(body,headers)
        del headers['X-Pebble-Dry-Run']
        for invalid in ({'n':2},{'tool_choice':'required'},{'response_format':{'type':'json_object'}},{'max_tokens':0}):
            with self.assertRaises(triage_store.Error):triage_http.parse_chat({**body,**invalid},headers)
        body['messages'][0]['content']=[{'type':'image_url','image_url':{'url':'https://not-fetched.invalid'}}]
        with self.assertRaises(triage_store.Error):triage_http.parse_chat(body,headers)

    def test_actual_http_native_and_chat_stream(self):
        with self.http(start_worker=True) as (url, _):
            with self.request(url+'/route/capabilities') as response:self.assertTrue(json.load(response)['client_speech'])
            with self.request(url+'/route/sessions',{'id':'native'}) as response:self.assertEqual(json.load(response)['id'],'native')
            with self.request(url+'/route/sessions/native/turns',self.turn()) as response: turn=json.load(response)
            with self.request(url+'/route/turns/'+turn['id']+'/events?after=0') as response:self.assertTrue(json.load(response)['events'])
            body={'model':'triage-route-dry-run','messages':[{'role':'user','content':'A separate test'}],'stream':True}
            with self.request(url+'/route/v1/chat/completions',body) as response:
                raw=response.read().decode()
                self.assertEqual(response.headers['Content-Type'],'text/event-stream; charset=utf-8')
            frames=[line[6:] for line in raw.splitlines() if line.startswith('data: ')]
            self.assertEqual(frames[-1],'[DONE]')
            data=[json.loads(x) for x in frames[:-1]]
            self.assertEqual(data[-1]['choices'][0]['finish_reason'],'stop')
            self.assertEqual(data[0]['choices'][0]['delta'],{'role':'assistant'})
            text=''.join(x['choices'][0]['delta'].get('content','') for x in data)
            self.assertIn('No action executed',text)
            self.assertEqual(len({x['id'] for x in data}),1)
            with self.request(url+'/route/v1/chat/completions',{**body,'stream':False}) as response:again=json.load(response)
            self.assertEqual(again['id'],data[0]['id'])
        self.assertEqual(self.sender.calls,[]);self.assertEqual(self.voice.calls,[])

    def test_auxiliary_never_admits_and_invalid_auth(self):
        with self.http() as (url, _):
            with self.assertRaises(urllib.error.HTTPError) as error:
                self.request(url+'/route/v1/models',Authorization='Bearer wrong')
            self.assertEqual(error.exception.code,401);error.exception.close()
            body={'model':'triage-route','messages':[{'role':'user','content':'File an unwanted task'}],'triage':{'purpose':'auxiliary'}}
            with self.request(url+'/route/v1/chat/completions',body) as response:self.assertIn('No command',json.load(response)['choices'][0]['message']['content'])
        with self.archive.db() as db:self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0],0)

    def test_short_stream_detaches_without_cancelling_admitted_work(self):
        with self.http() as (url, _):
            body={'model':'triage-route-dry-run','messages':[{'role':'user','content':'Still pending after presentation stops'}],'max_tokens':1,'stream':True}
            with self.request(url+'/route/v1/chat/completions',body) as response:
                raw=response.read().decode(); turn_id=response.headers['X-Triage-Turn-ID']
            self.assertIn('"finish_reason": "length"',raw)
            with self.request(url+'/route/turns/'+turn_id) as response:self.assertFalse(json.load(response)['done'])
            self.drain()
            with self.request(url+'/route/turns/'+turn_id) as response:self.assertEqual(json.load(response)['phase'],'dry_run')
        self.assertEqual(self.sender.calls,[])

    def test_duplicate_json_and_stream_auxiliary_do_not_admit(self):
        with self.http() as (url, _):
            req=urllib.request.Request(url+'/route/sessions',data=b'{"id":"first","id":"second"}',headers={
                'Authorization':'Bearer '+self.legacy.token,'Content-Type':'application/json'})
            with self.assertRaises(urllib.error.HTTPError) as error:urllib.request.urlopen(req,timeout=3)
            self.assertEqual(error.exception.code,400);error.exception.close()
            body={'model':'triage-route','messages':[{'role':'user','content':'Do not execute background summary'}],'stream':True,'triage':{'purpose':'auxiliary'}}
            with self.request(url+'/route/v1/chat/completions',body) as response:
                raw=response.read().decode();self.assertIn('No command was submitted',raw);self.assertIn('[DONE]',raw)
            with self.request(url+'/route/v1/chat/completions',{**body,'max_tokens':1}) as response:
                frames=[json.loads(s[6:]) for s in response.read().decode().splitlines() if s.startswith('data: {')]
                self.assertEqual(frames[-1]['choices'][0]['finish_reason'],'length')
                self.assertEqual(''.join(x['choices'][0]['delta'].get('content','') for x in frames),'T')
        with self.archive.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM command_inputs').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM triage_sessions').fetchone()[0],0)

    def test_explicit_reply_endpoint_crosses_read_only_caco_chat_and_keeps_receipt(self):
        from test_triage_replies import peer, message, envelope
        holder = {}
        def response(_):
            action = holder['receipt']['action']
            row = message(project=action['project'], sender=action['target'], reply_to=action['message_id'],
                          body='Fixture correlated reply', body_len=len('Fixture correlated reply'))
            return {'json': envelope([row])}
        with peer([response]) as (caco_url, calls, _):
            self.config['commands']['caco']['daemon_url'] = caco_url
            store=self.store(); turn=store.submit('fixture-session',self.turn(mode='live',route='agent-msg'))
            self.drain(); holder['receipt']=store.get(turn['id'])['receipt']
            before=self.archive.action(holder['receipt']['id'])
            with self.http() as (url,_):
                with self.request(url+'/route/turns/'+turn['id']+'/replies') as r:result=json.load(r)
            self.assertEqual(result['observation']['status'],'correlated_reply')
            self.assertEqual(result['observation']['replies'][0]['provenance']['content_role'],'agent_assertion')
            self.assertEqual(len(calls),1)
            self.assertIn('/messages/chat?',calls[0]['path'])
            self.assertEqual(self.archive.action(holder['receipt']['id']),before)
        self.assertEqual(len(self.sender.calls),1)  # Only the fixture's original send.
        self.assertEqual(self.voice.calls,[])

    def test_output_limit_is_presentation_only(self):
        with self.http(start_worker=True) as (url,_):
            body={'model':'triage-route-dry-run','messages':[{'role':'user','content':'Test bounded result'}],'max_tokens':1}
            with self.request(url+'/route/v1/chat/completions',body) as response:data=json.load(response)
            self.assertEqual(data['choices'][0]['finish_reason'],'length')
            self.assertLessEqual(len(data['choices'][0]['message']['content'].encode()),1)
            with self.request(url+'/route/turns/'+data['id'].removeprefix('chatcmpl-')) as response:self.assertEqual(json.load(response)['phase'],'dry_run')
        self.assertEqual(self.sender.calls,[])
