"""Cross a prefix-stripping HTTP proxy, not just the receiver or config JSON."""
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest
import urllib.request
import urllib.parse

import test_command_pipeline as fixtures
import command_pipeline
import funnel


class PrefixProxyTest(unittest.TestCase):
    setUp=fixtures.PipelineTest.setUp
    http=fixtures.PipelineTest.http
    post=fixtures.PipelineTest.post

    def test_public_style_proxy_preserves_every_fixed_action_status_and_sse(self):
        self.config['commands']['actions']={'note':{'description':'Save a note','command':['do-not-execute']}}
        with self.http() as (backend,_):
            host='fixture.example:443'
            state={'TCP':{'443':{'HTTPS':True}},'AllowFunnel':{host:True},'Web':{host:{'Handlers':{'/':{'Proxy':'http://unrelated.invalid'}}}}}
            # Include a deployed old mapping and duplicate slash key to verify migration.
            state['Web'][host]['Handlers']['/hooks/global/pebble']={'Proxy':backend}
            state['Web'][host]['Handlers']['/hooks/global/pebble/']={'Proxy':backend}
            def mutate(args,**kwargs):
                key=next(v.split('=',1)[1] for v in args if v.startswith('--set-path='))
                state['Web'][host]['Handlers'][key]={'Proxy':args[-1]}
            funnel.install('tailscale','fixture.example',backend,run=mutate,inspect=lambda _:copy.deepcopy(state),commands=True)
            self.assertEqual(state['Web'][host]['Handlers']['/'],{'Proxy':'http://unrelated.invalid'})
            handlers=state['Web'][host]['Handlers']
            class Proxy(BaseHTTPRequestHandler):
                def log_message(self,*a):pass
                def forward(self):
                    parts=urllib.parse.urlsplit(self.path)
                    mount=max((k for k in handlers if parts.path==k or parts.path.startswith(k.rstrip('/')+'/')),key=len)
                    target=handlers[mount]['Proxy']+parts.path[len(mount):]
                    if parts.query:target+='?'+parts.query
                    headers={'Authorization':self.headers['Authorization']}
                    data=None
                    if self.command=='POST':
                        data=self.rfile.read(int(self.headers['Content-Length']))
                        headers['Content-Type']=self.headers['Content-Type']
                    request=urllib.request.Request(target,data=data,headers=headers)
                    with urllib.request.urlopen(request,timeout=5) as result:
                        body=result.read();status=result.status;kind=result.headers['Content-Type']
                    self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(body)))
                    self.end_headers();self.wfile.write(body)
                do_GET=forward
                do_POST=forward
            server=ThreadingHTTPServer(('127.0.0.1',0),Proxy)
            thread=threading.Thread(target=server.serve_forever);thread.start()
            public=f'http://127.0.0.1:{server.server_port}'
            try:
                for path,action in command_pipeline.ROUTES.items():
                    if path=='/':continue
                    status,value=self.post(public+path,b'{"test":true}')
                    self.assertEqual(status,200);self.assertEqual(value['endpoint_action'],action,path)
                    self.assertFalse(value['archived'])
                for path in ('/pebble/action/note','/hooks/global/pebble/action/note'):
                    _,value=self.post(public+path,b'{"test":true}')
                    self.assertEqual(value['endpoint_action'],'custom:note')
                for path in ('/pebble/status','/pebble/status/','/hooks/global/pebble-status'):
                    req=urllib.request.Request(public+path,headers={'Authorization':'Bearer '+self.legacy.token})
                    with urllib.request.urlopen(req,timeout=5) as r:self.assertEqual(json.load(r)['version'],1)
                req=urllib.request.Request(public+'/pebble/events?seconds=1',headers={'Authorization':'Bearer '+self.legacy.token})
                with urllib.request.urlopen(req,timeout=5) as r:self.assertIn(b'event: status',r.read())
                req=urllib.request.Request(public+'/route/v1/models',headers={'Authorization':'Bearer '+self.legacy.token})
                with urllib.request.urlopen(req,timeout=5) as r:
                    self.assertIn('triage-route-dry-run',[m['id'] for m in json.load(r)['data']])
                req=urllib.request.Request(public+'/route/capabilities',headers={'Authorization':'Bearer '+self.legacy.token})
                with urllib.request.urlopen(req,timeout=5) as r:self.assertTrue(json.load(r)['native_turns'])
                self.assertEqual(self.archive.status()['requests'],0)
                self.assertEqual(self.sender.calls,[]);self.assertEqual(self.voice.calls,[])
            finally:
                server.shutdown();server.server_close();thread.join(5)
                self.assertFalse(thread.is_alive())
