from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import threading
import unittest

import test_command_pipeline as fixtures
import payload_extractor as extractor
import dispatch
from process_io import Result, ProcessError


def result(*segments):
    return {'requested_model':'example-extractor','model':'fixture-extractor','result':{'status':'ok','segments':list(segments)},'elapsed_ms':1}


class ExtractorTest(unittest.TestCase):
    def test_only_exact_ordered_source_segments_are_allowed(self):
        text='Broadcast: do not deploy; priority P4; keep the old API.'
        content,spans=extractor.validate_payload(text,{'status':'ok','segments':['do not deploy;','keep the old API.']})
        self.assertEqual(content,'do not deploy;\nkeep the old API.')
        self.assertEqual([text[s['start']:s['end']] for s in spans],['do not deploy;','keep the old API.'])
        for invalid in (["invented"],['keep the old API.','do not deploy;'],[''],['do not deploy;','do not deploy;']):
            with self.assertRaises(extractor.ExtractionError): extractor.validate_payload(text,{'status':'ok','segments':invalid})
        with self.assertRaisesRegex(extractor.ExtractionError,'payload_needs_resolution'):
            extractor.validate_payload(text,{'status':'needs_clarification','segments':[]})

    def test_extractor_cannot_choose_routing_fields(self):
        with self.assertRaises(extractor.ExtractionError):
            extractor.validate_payload('hello',{'status':'ok','segments':['hello'],'scope':'global'})
        plan=fixtures.fake_plan('Send to SDK: hello',None,'agent-msg')
        enriched=extractor.apply(plan,result('hello'))
        self.assertEqual(enriched['target'],plan['target'])
        self.assertEqual(enriched['text'],plan['text'])
        self.assertEqual(enriched['body'],'hello')
        self.assertEqual(extractor.payload_for(enriched),'hello')
        enriched['payload']='altered'
        with self.assertRaises(extractor.ExtractionError): extractor.payload_for(enriched)

    def test_all_non_assistant_recipes_use_extracted_payload(self):
        for action in ('agent-msg','broadcast','bd-file','bd-expand'):
            plan=fixtures.fake_plan('Route this: keep the exact body',None,action)
            plan['payload_policy']='extract-v1'
            with self.assertRaises(dispatch.DispatchError): dispatch.prepare(plan,'a'*32,{})
            enriched=extractor.apply(plan,result('keep the exact body'))
            recipe=dispatch.prepare(enriched,'a'*32,{})
            self.assertEqual(recipe['text'],'keep the exact body')
            self.assertEqual(recipe['original_text'],plan['text'])
            argv,body=dispatch.command(recipe,'caco')
            if action=='bd-expand':self.assertEqual(argv[argv.index('--text')+1],'keep the exact body')
            else:self.assertEqual(body,b'keep the exact body')

    def test_actual_http_worker_strict_schema_redirects_and_credential_redaction(self):
        calls=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*a):pass
            def do_POST(self):
                data=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                calls.append((self.path,dict(self.headers),data))
                if self.path.startswith('/redirect'):
                    self.send_response(307);self.send_header('Location','/responses');self.send_header('Content-Length','0');self.end_headers();return
                encoded=json.dumps({'id':'fixture','model':'example-extractor','status':'completed',
                    'output':[{'type':'message','content':[{'type':'output_text','text':'{"status":"ok","segments":["hello"]}'}]}]}).encode()
                self.send_response(200);self.send_header('Content-Length',str(len(encoded)));self.end_headers();self.wfile.write(encoded)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        try:
            settings={'enabled':True,'model':'example-extractor','base_url':f'http://127.0.0.1:{server.server_port}', 'timeout_secs':5,'max_output_tokens':1024}
            env={**os.environ,'OPENAI_API_KEY':'fixture-extraction-secret'}
            p=fixtures.fake_plan('Send to SDK: hello',None,'agent-msg')
            response=extractor.extract(p,settings,env=env)
            self.assertEqual(response['result']['segments'],['hello'])
            request=calls[0][2]
            self.assertFalse(request['store'])
            self.assertEqual(request['model'],'example-extractor')
            self.assertTrue(request['text']['format']['strict'])
            self.assertNotIn('tools',request)
            self.assertEqual(calls[0][1]['Authorization'],'Bearer fixture-extraction-secret')
            self.assertNotIn('fixture-extraction-secret',json.dumps(response))
            settings['base_url']+='/redirect'
            with self.assertRaisesRegex(extractor.ExtractionError,'extractor_http_307'):
                extractor.extract(p,settings,env=env)
            self.assertEqual(len(calls),2)
        finally:
            server.shutdown();server.server_close();thread.join(3)
            self.assertFalse(thread.is_alive())

    def test_timeout_never_returns_original_as_a_fallback_payload(self):
        def timeout(*a,**k):raise ProcessError('command_timeout')
        with self.assertRaisesRegex(extractor.ExtractionError,'extraction_command_timeout'):
            extractor.extract(fixtures.fake_plan('Send: hello',None,'agent-msg'),{'timeout_secs':1},runner=timeout)


class ExtractionPipelineTest(unittest.TestCase):
    setUp=fixtures.PipelineTest.setUp
    drain=fixtures.PipelineTest.drain

    def admit(self, action='agent-msg', dry=True, recorded='1770000200000'):
        self.config['commands']['extractor']['enabled']=True
        body=json.dumps({'transcription':'Send to the selected destination: hello','recordedAt':recorded}).encode()
        return self.archive.accept(body,'application/json',action,dry_run=dry)

    def test_fixed_pebble_skips_extraction(self):
        def trap(*a,**k):self.fail('assistant must not call payload model')
        self.processor.extractor=trap
        r=self.admit('pebble')
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'],'dry_run')
        self.assertNotIn('extraction',self.archive.receipt(r['id']))

    def test_automatic_assistant_and_routing_failure_skip_extraction(self):
        self.processor.extractor=lambda *a,**k:self.fail('assistant must not invoke extractor')
        for index, unavailable in enumerate((False,True)):
            def planner(text,*a,**k):
                if unavailable: raise fixtures.jev_router.RouterError('unavailable')
                return fixtures.fake_plan(text,None,'pebble')
            self.processor.planner=planner
            r=self.admit('route',recorded=str(1770000200100+index))
            self.drain()
            self.assertEqual(self.archive.get(r['id'])['phase'],'dry_run')
            self.assertNotIn('extraction',self.archive.receipt(r['id']))

    def test_route_freezes_before_extraction_and_restart_does_not_reroute(self):
        r=self.admit()
        self.processor.step()
        self.assertEqual(self.archive.get(r['id'])['phase'],'dry_payload_pending')
        route=json.loads(self.archive.get(r['id'])['plan'])
        self.archive.update(r['id'],phase='extracting')
        self.archive.recover()
        self.processor.planner=lambda *a,**k:self.fail('must not reroute')
        self.processor.extractor=lambda *a,**k:result('hello')
        self.drain()
        updated=json.loads(self.archive.get(r['id'])['plan'])
        self.assertEqual(updated['target'],route['target'])
        self.assertEqual(updated['text'],route['text'])
        self.assertEqual(updated['payload'],'hello')
        receipt=self.archive.receipt(r['id'])
        self.assertEqual(receipt['extraction']['payload_preview'],'hello')
        self.assertEqual(receipt['action']['attempts'],0)
        self.assertEqual(self.sender.calls,[])

    def test_extraction_failure_retains_input_and_retry_keeps_route_and_readback(self):
        r=self.admit()
        def fail(*a,**k):raise extractor.ExtractionError('extractor_unavailable')
        self.processor.extractor=fail
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['error_stage'],'extract')
        self.assertEqual(self.sender.calls,[])
        self.assertIsNone(self.archive.action(r['id']))
        self.archive.resolve(r['id'],'retry')
        self.processor.planner=lambda *a,**k:self.fail('must not reroute')
        self.processor.extractor=lambda *a,**k:result('hello')
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'],'dry_run')
        self.assertEqual(sum(c[0]=='prefix' for c in self.voice.calls),1)

    def test_frozen_payload_is_not_reextracted_on_action_retry(self):
        r=self.admit(dry=False)
        count=[]
        def clean(*a,**k):count.append(True);return result('hello')
        self.processor.extractor=clean
        self.sender.lose_first=True
        self.drain()
        self.assertEqual(self.archive.get(r['id'])['phase'],'blocked')
        self.archive.resolve(r['id'],'retry')
        self.drain()
        self.assertEqual(len(count),1)
        self.assertEqual(self.sender.calls[0]['body'],'hello')
        self.assertEqual(self.sender.calls[0],self.sender.calls[1])
