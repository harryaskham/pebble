import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import importlib.util
import unittest
import urllib.error
from unittest.mock import patch

import test_command_pipeline as fixtures
from test_jev_router import context, answer
from test_payload_extractor import result as extraction
import action_registry as registry
import custom_dispatch
import dispatch
import intake
import jev_router
from process_io import Result, ProcessError

ROOT = Path(__file__).resolve().parents[2]


def spec(command=None):
    return {'description':'Store a private note when the user says note to self.',
            'command':command or ['printf','not-a-real-effect'], 'extract':'Extract only the note body.'}


class RegistryTest(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec('yaml'), 'YAML manifests need PyYAML (the packaged Python has it)')
    def test_yaml_and_json_have_same_schema(self):
        self.assertEqual(registry.parse(b'actions:\n  note:\n    description: Store a note\n    command: [cat]\n'),
                         registry.parse(b'{"actions":{"note":{"description":"Store a note","command":["cat"]}}}'))
        value=registry.parse(b'actions:\n  note:\n    description: Store a note\n    command: |\n      cat >> "$HOME/notes"\n')
        self.assertIsInstance(value['note']['command'],str)

    def test_check_cli_validates_a_manifest_and_names_its_routes(self):
        import contextlib, io, tempfile, os
        with tempfile.TemporaryDirectory() as d:
            good, bad = os.path.join(d, 'good.yaml'), os.path.join(d, 'bad.yaml')
            with open(good, 'w') as f:
                f.write('{"actions":{"track-feedback":{"description":"Feedback on the playing track","command":["cat"]}}}')
            with open(bad, 'w') as f:
                f.write('{"actions":{"pebble":{"description":"x","command":["cat"]}}}')
            out, err = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertEqual(registry.main([good]), 0)
                self.assertEqual(registry.main([bad]), 1)
                self.assertEqual(registry.main([os.path.join(d, 'missing.yaml')]), 1)
            self.assertIn('/pebble/action/track-feedback', out.getvalue())
            self.assertIn('invalid_custom_action_name', err.getvalue())
            self.assertIn('action_manifest_unavailable', err.getvalue())

    def test_invalid_yaml_names_aliases_collisions_and_fields_fail_closed(self):
        for raw in (b'actions: {}\nactions: {}',b'actions: &a {note: *a}',b'!!python/object:os.system {}',b'actions: {}\n---\nactions: {}'):
            with self.assertRaises(registry.RegistryError): registry.parse(raw)
        for actions in ({'../note':spec()},{'pebble':spec()},{'note':{**spec(),'unknown':True}},
                        {'note':{**spec(),'command':[]}}, {'note':{**spec(),'timeout_secs':True}},
                        {'note':{**spec(),'description':''}}, {'note':{**spec(),'enabled':'false'}}):
            with self.assertRaises(registry.RegistryError):registry.validate(actions)
        self.assertEqual(registry.catalog({'actions':{'note':{**spec(),'enabled':False}}}),{})

    def test_only_descriptions_enter_jev_and_no_extra_destination_call(self):
        requests=[]
        actions=registry.validate({'note':spec(['SHOULD_NOT_ENTER_PROMPT'])})
        def runner(argv,payload,**kw):
            r=json.loads(payload);requests.append(r)
            return Result(json.dumps(answer(r,{'route':'custom:note'})).encode(),1)
        p=jev_router.plan('Note to self: hello',context(),now=1000,actions=actions,runner=runner)
        self.assertEqual(len(requests),1)
        self.assertIn('custom:note',requests[0]['questions']['route']['criteria'])
        self.assertNotIn('SHOULD_NOT_ENTER_PROMPT',json.dumps(requests))
        self.assertEqual(p['extraction_instruction'],actions['note']['extract'])
        self.assertEqual(p['action'],'custom:note')
        p=jev_router.plan('hello',None,'custom:note',actions=actions,runner=lambda *a,**k:self.fail('fixed route does not need Jev'))
        self.assertEqual(p['decision']['source'],'explicit_endpoint')

    def test_many_custom_candidates_stay_within_budget(self):
        actions={f'action-{i}':{**spec(),'description':'very long action description '*20} for i in range(32)}
        r,_=jev_router.build_request('do the thing',context(),now=1000,actions=actions)
        self.assertLessEqual(len(json.dumps(r,ensure_ascii=False).encode()),jev_router.MAX_REQUEST_BYTES)
        self.assertEqual(len(r['questions']['route']['criteria']),len(jev_router.ACTIONS)+32)

    def test_custom_executor_stdin_and_metadata_never_payload_interpolation(self):
        p=jev_router.plan('hello $(touch /NOT-A-FILE)',None,'custom:note',actions={'note':spec(['cat'])})
        r=dispatch.prepare(p,'a'*32,{'actions':registry.validate({'note':spec(['cat'])})})
        calls=[]
        def runner(argv,body,**kwargs):calls.append((argv,body,kwargs));return Result(b'secret output not saved',3)
        out=custom_dispatch.execute(r,runner=runner,env={})
        self.assertEqual(calls[0][0],['cat'])
        self.assertEqual(calls[0][1],p['text'].encode())
        self.assertEqual(calls[0][2]['env']['PEBBLE_INPUT_ID'],'a'*32)
        self.assertEqual(out['state'],'completed');self.assertNotIn('secret',json.dumps(out))
        r['custom_spec']['command']='cat >> "$HOME/notes"'
        custom_dispatch.execute(r,runner=runner,env={})
        self.assertEqual(calls[-1][0],['bash','-euo','pipefail','-c','cat >> "$HOME/notes"'])
        r['dry_run']=True
        with self.assertRaisesRegex(dispatch.NotStarted,'dry_run_execution_forbidden'):
            custom_dispatch.execute(r,runner=lambda *a,**k:self.fail('dry run invoked command'))
        r['dry_run']=False
        with self.assertRaises(dispatch.NotStarted):dispatch.Caco().execute(r)


class CustomPipelineTest(unittest.TestCase):
    setUp=fixtures.PipelineTest.setUp
    drain=fixtures.PipelineTest.drain
    http=fixtures.PipelineTest.http
    post=fixtures.PipelineTest.post

    def setup_custom(self):
        self.marker=self.root/'notes.org'
        self.other=self.root/'new-command.org'
        self.file=self.root/'actions.yaml'
        self.config['commands']['actions_file']=str(self.file)
        self.config['commands']['extractor']['enabled']=True
        self.replace_manifest(self.marker)
        def planner(text,snapshot,action,**kwargs):
            return jev_router.plan(text,None,action if registry.custom(action) else 'custom:note',actions=kwargs['actions'])
        self.processor.planner=planner
        self.processor.extractor=lambda *a,**k:extraction('keep $(these) words')

    def replace_manifest(self,destination,enabled=True):
        command=[sys.executable,str(ROOT/'scripts/pebble-org-append'),'--file',str(destination)]
        raw=json.dumps({'actions':{'note':{**spec(command),'enabled':enabled}}})
        self.file.write_text(raw)

    def accept(self, *, action='custom:note',dry=False,recorded=1770010000000):
        body=json.dumps({'transcription':'Note to self: keep $(these) words','recordedAt':str(recorded)}).encode()
        return self.archive.accept(body,'application/json',action,dry_run=dry)

    def test_live_real_helper_and_dry_run_have_distinct_receipts(self):
        self.setup_custom()
        r=self.accept(dry=True)
        self.drain()
        self.assertFalse(self.marker.exists());self.assertEqual(self.sender.calls,[])
        self.assertEqual(self.archive.receipt(r['id'])['action']['attempts'],0)
        r=self.accept(recorded=1770010000001)
        self.drain()
        receipt=self.archive.receipt(r['id'])
        self.assertEqual(receipt['phase'],'completed',receipt)
        self.assertEqual(receipt['action']['exit_code'],0)
        self.assertIn('keep $(these) words',self.marker.read_text())
        self.assertNotIn('Note to self',self.marker.read_text())
        self.assertEqual(self.sender.calls,[])
        self.assertFalse(self.processor.step())

    def test_admission_freezes_catalog_before_any_routing_or_retries(self):
        self.setup_custom()
        r=self.accept()
        self.replace_manifest(self.other)
        self.drain()
        self.assertTrue(self.marker.exists());self.assertFalse(self.other.exists())
        self.assertTrue(self.accept()['duplicate'])
        self.assertFalse(self.processor.step())
        newer=self.accept(recorded=1770010000001)
        self.drain()
        self.assertTrue(self.other.exists())
        self.assertEqual(self.archive.get(newer['id'])['phase'],'completed')

    def test_uncertain_command_never_retries_but_new_same_lane_work_proceeds(self):
        self.setup_custom()
        sender=self.processor.custom_sender
        self.processor.custom_sender=lambda *a,**k:(_ for _ in ()).throw(ProcessError('command_timeout'))
        first=self.accept();self.drain()
        self.assertEqual(self.archive.get(first['id'])['phase'],'blocked')
        with self.assertRaisesRegex(intake.IntakeError,'authoritative_reconciliation'):
            self.archive.resolve(first['id'],'retry')
        before=self.archive.action(first['id'])
        self.processor.custom_sender=sender
        later=self.accept(recorded=1770010000001);self.drain()
        self.assertEqual(self.archive.get(later['id'])['phase'],'completed')
        self.assertEqual(self.archive.action(first['id']),before)
        self.assertEqual(self.marker.read_text().count('keep $(these) words'),1)
        preview=self.accept(dry=True,recorded=1770010000002);self.drain()
        self.assertEqual(self.archive.get(preview['id'])['phase'],'dry_run')

    def test_http_hot_reload_prefixes_auth_unknown_and_invalid_manifest(self):
        self.setup_custom()
        with self.http() as (url,_):
            for i,prefix in enumerate(registry.PREFIXES):
                status,r=self.post(url+prefix+'note?dry_run=true',json.dumps({'transcription':'Note to self: keep $(these) words','recordedAt':i+1}).encode())
                self.assertEqual(status,202);self.assertTrue(r['dry_run'])
            status,probe=self.post(url+'/hooks/global/pebble-route',b'{"test":true}')
            self.assertTrue(probe['action_manifest_supported']);self.assertIn('custom:note',probe['supported_actions'])
            self.replace_manifest(self.marker,enabled=False)
            with self.assertRaises(urllib.error.HTTPError) as caught:self.post(url+'/pebble/action/note',b'{"test":true}')
            self.assertEqual(caught.exception.code,404);caught.exception.close()
            self.file.write_text('invalid: manifest')
            with self.assertRaises(urllib.error.HTTPError) as caught:self.post(url+'/hooks/global/pebble-route',b'{"test":true}')
            self.assertEqual(caught.exception.code,503);caught.exception.close()
        # Broken current catalog does not destroy already accepted inputs.
        self.drain();self.assertFalse(self.marker.exists())

    def test_managed_manifest_link_rotation_and_fifo_refusal(self):
        self.setup_custom()
        linked=self.root/'managed.yaml';linked.symlink_to(self.file)
        self.config['commands']['actions_file']=str(linked)
        self.assertIn('note',registry.catalog(self.config['commands']))
        self.replace_manifest(self.other)
        self.assertEqual(registry.catalog(self.config['commands'])['note']['command'][-1],str(self.other))
        self.assertTrue(linked.is_symlink())
        fifo=self.root/'fifo';os.mkfifo(fifo)
        self.config['commands']['actions_file']=str(fifo)
        with self.assertRaisesRegex(registry.RegistryError,'not_regular'):
            registry.catalog(self.config['commands'])

    def test_org_helper_preserves_symlink_and_does_not_inject_headings(self):
        target=self.root/'target.org';target.write_text('existing without newline')
        link=self.root/'linked.org';link.symlink_to(target)
        proc=subprocess.run([sys.executable,str(ROOT/'scripts/pebble-org-append'),'--file',str(link),'--todo'],
                            input=b'call dentist tomorrow\n* not a new heading',capture_output=True,timeout=5)
        self.assertEqual(proc.returncode,0,proc.stderr);self.assertTrue(link.is_symlink())
        text=target.read_text();self.assertIn('existing without newline\n* TODO ',text)
        self.assertIn('\n  * not a new heading\n',text)
