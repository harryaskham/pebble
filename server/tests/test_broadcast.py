import copy
import json
import sys
import unittest
from unittest.mock import patch

import test_command_pipeline as fixtures
from test_jev_router import context, answer
import jev_router
import dispatch
import intake
from process_io import Result


def broadcast_plan(text, scope='project', project='sdk'):
    return {'version':1, 'action':'broadcast', 'text':text, 'body':text, 'scope':scope,
            'project':project if scope=='project' else None, 'executed':False,
            'decision':{'source':'fixture'}}


def broadcast_response(recipe, state='accepted'):
    return {'ok':True,'data':{
        'id':'msg-broadcast-fixture','kind':'broadcast','scope':recipe['scope'],'project':recipe['project'] or '',
        'target':None,'admission':{'message_id':'msg-broadcast-fixture','storage_state':'stored','runtime_delivery_state':'pending_confirmation','pi_admission_state':'unavailable'},
        'delivery_receipt':{'origin_id':'msg-broadcast-fixture','logical_accepted':True,'state':state,
            'total_recipients':3,'accepted_recipients':3,'delivered_recipients':0,'failed_recipients':0,
            'pending_recipients':3,'backpressured_recipients':0}}}


class BroadcastRoutingTest(unittest.TestCase):
    def runner(self, audience):
        def run(argv, payload, **kwargs):
            request=json.loads(payload)
            self.requests.append(request)
            return Result(json.dumps(answer(request, {'route':'broadcast','audience':audience})).encode(), 1)
        self.requests=[]
        return run

    def test_global_and_project_are_distinct_frozen_choices(self):
        for audience in ('__global__','sdk'):
            p=jev_router.plan('Tell everyone the exact text',context(),now=1000,runner=self.runner(audience))
            self.assertEqual(p['action'],'broadcast')
            self.assertEqual(p['scope'],'global' if audience=='__global__' else 'project')
            self.assertEqual(p['project'],None if audience=='__global__' else 'sdk')
            self.assertEqual(p['body'],p['text'])
            self.assertEqual(self.requests[0]['state']['context'],{})
            self.assertEqual(set(self.requests[1]['questions']),{'audience'})
            self.assertEqual(set(self.requests[1]['questions']['audience']['criteria']),{'__none__','__global__','sdk','website'})

    def test_unresolved_never_becomes_global(self):
        p=jev_router.plan('Broadcast this',context(),now=1000,runner=self.runner('__none__'))
        self.assertEqual(p['action'],'pebble')
        self.assertEqual(p['fallback_reason'],'target_needs_resolution')
        with self.assertRaisesRegex(jev_router.RouterError,'target_needs_resolution'):
            jev_router.plan('Broadcast this',context(),'broadcast',now=1000,runner=self.runner('__none__'))

    def test_unknown_audience_rejected(self):
        with self.assertRaisesRegex(jev_router.RouterError,'invalid_jev_choice_audience'):
            jev_router.plan('Broadcast this',context(),'broadcast',now=1000,runner=self.runner('not-configured'))

    def test_recipe_uses_real_cli_scope_and_never_invents_idempotency(self):
        for scope in ('project','global'):
            recipe=dispatch.prepare(broadcast_plan('$(keep)\nverbatim',scope),'a'*32,{})
            argv,payload=dispatch.command(recipe,'caco')
            self.assertEqual(payload,b'$(keep)\nverbatim')
            self.assertIn('--confirm-broadcast',argv)
            self.assertIn('--body-stdin',argv)
            self.assertIn('--wake',argv)
            self.assertNotIn('--raw',argv)
            self.assertNotIn('--idempotency-key',argv)
            self.assertFalse(recipe['idempotent'])
            self.assertEqual('--global' in argv,scope=='global')
            self.assertEqual('--project' in argv,scope=='project')

    def test_invalid_scope_or_changed_body_cannot_prepare(self):
        for change in ({'scope':'other'},{'scope':'global','project':'sdk'},{'body':'rewritten'},{'project':None}):
            with self.assertRaises(dispatch.DispatchError):
                dispatch.prepare({**broadcast_plan('original'),**change},'a'*32,{})

    def test_receipt_requires_identity_and_matching_scope(self):
        recipe=dispatch.prepare(broadcast_plan('original'),'a'*32,{})
        valid=broadcast_response(recipe)
        outcome=dispatch.receipt(recipe,valid)
        self.assertEqual(outcome['state'],'stored')
        self.assertEqual(outcome['delivery']['pending_recipients'],3)
        for change in ({'id':None},{'scope':'global'},{'project':'website'},{'kind':'direct'},
                       {'dry_run':True},{'target':'node:sdk:worker'},
                       {'admission':{'storage_state':'unknown'}},
                       {'delivery_receipt':{'state':'accepted'}}):
            with self.assertRaises(dispatch.DispatchError):
                dispatch.receipt(recipe,{'ok':True,'data':{**valid['data'],**change}})
        bad=copy.deepcopy(valid);bad['data']['delivery_receipt']['delivered_recipients']=4
        with self.assertRaises(dispatch.DispatchError):dispatch.receipt(recipe,bad)

    def test_broadcast_invokes_only_the_action_without_preflight(self):
        calls=[]
        recipe=dispatch.prepare(broadcast_plan('original','global'),'a'*32,{})
        def runner(argv,body,**kwargs):
            calls.append(argv)
            self.assertEqual(argv[1:3],['msg','broadcast'])
            self.assertIn('--confirm-broadcast',argv)
            self.assertEqual(body,b'original')
            return Result(json.dumps(broadcast_response(recipe)).encode(),1)
        result=dispatch.Caco(binary='/fixture/caco',runner=runner).execute(recipe)
        self.assertEqual(result['state'],'stored')
        self.assertEqual(len(calls),1)

    def test_dry_recipe_never_calls_caco(self):
        recipe=dispatch.prepare(broadcast_plan('original','global'),'a'*32,{},dry_run=True)
        with self.assertRaisesRegex(dispatch.NotStarted,'dry_run_execution_forbidden'):
            dispatch.Caco(runner=lambda *a,**k:self.fail('must not invoke Caco')).execute(recipe)


class BroadcastPipelineTest(unittest.TestCase):
    setUp=fixtures.PipelineTest.setUp
    drain=fixtures.PipelineTest.drain
    http=fixtures.PipelineTest.http
    post=fixtures.PipelineTest.post

    def admit(self, action='broadcast', *, scope='project', project='sdk', dry=False, target=None):
        body=json.dumps({'transcription':'Preserve these exact words','recordedAt':str(1770000010000+self.archive.status()['requests'])}).encode()
        r=self.archive.accept(body,'application/json',action,dry_run=dry)
        # Keep all fixtures explicit and independent of currently running agents.
        def planner(text,snapshot,endpoint,**kwargs):
            if endpoint=='broadcast':return broadcast_plan(text,scope,project)
            p=fixtures.fake_plan(text,snapshot,endpoint)
            if endpoint=='agent-msg' and target:p.update(target=target,project=target.split(':')[1])
            if endpoint in ('bd-file','bd-expand'):p['project']=project
            return p
        self.processor.planner=planner
        self.processor.route(self.archive.get(r['id']),self.processor.settings(self.archive.get(r['id'])))
        return r

    def test_global_and_project_dry_runs_announce_audience_without_sender(self):
        for scope in ('project','global'):
            r=self.admit(scope=scope,dry=True)
            self.drain()
            receipt=self.archive.receipt(r['id'])
            self.assertEqual(receipt['phase'],'dry_run')
            self.assertEqual(receipt['action']['scope'],scope)
            self.assertEqual(receipt['action']['attempts'],0)
        self.assertEqual(self.sender.calls,[])
        speech=' '.join(str(c) for c in self.voice.calls)
        self.assertIn('would broadcast globally to all projects',speech)
        self.assertIn('would broadcast to project sdk',speech)

    def test_recording_only_broadcast_preview_keeps_mai_and_zero_mutations(self):
        body,kind=fixtures.legacy_tests.multipart(text='')
        result=self.archive.accept(body,kind,'broadcast',dry_run=True)
        self.processor.planner=lambda text,*a,**k:broadcast_plan(text,'global')
        self.drain()
        self.assertEqual(len(self.transcriptions),1)
        self.assertEqual(self.archive.get(result['id'])['text'],'MAI words, not device words')
        self.assertEqual(self.archive.receipt(result['id'])['action']['scope'],'global')
        self.assertEqual(self.sender.calls,[])

    def test_failed_global_preview_does_not_block_any_live_message(self):
        preview=self.admit(scope='global',dry=True)
        self.archive.update(preview['id'],phase='blocked')
        msg=self.admit('agent-msg')
        self.drain()
        self.assertEqual(self.archive.get(msg['id'])['phase'],'stored')

    def test_explicit_broadcast_endpoint_admits_both_scope_choices(self):
        with self.http() as (url,_):
            status,r=self.post(url+'/hooks/global/pebble-broadcast?dry_run=true',b'{"transcription":"Broadcast globally"}')
            self.assertEqual(status,202)
            self.assertEqual(self.archive.get(r['id'])['endpoint_action'],'broadcast')
            self.assertTrue(r['dry_run'])
        self.drain()
        self.assertEqual(self.sender.calls,[])

    def test_uncertain_fanout_never_retries_and_does_not_hold_later_work(self):
        first=self.admit()
        self.archive.start_action(first['id'])
        self.archive.finish_action(first['id'],'indeterminate',error='lost_receipt')
        self.processor.confirm(self.archive.get(first['id']),self.processor.settings(self.archive.get(first['id'])))
        with self.assertRaisesRegex(intake.IntakeError,'authoritative_reconciliation'):
            self.archive.resolve(first['id'],'retry')
        held=self.admit('agent-msg')
        free=self.admit('agent-msg',target='node-b:website:site-worker')
        bead=self.admit('bd-file')
        preview=self.admit(scope='global',dry=True)
        self.drain()
        self.assertEqual(self.archive.get(held['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(free['id'])['phase'],'stored')
        self.assertEqual(self.archive.action(first['id'])['state'],'indeterminate')
        self.assertEqual(self.archive.action(first['id'])['attempts'],1)
        self.assertNotIn(first['id'],[r['input_id'] for r in self.sender.calls])
        self.assertEqual(self.archive.get(bead['id'])['phase'],'created')
        self.assertEqual(self.archive.get(preview['id'])['phase'],'dry_run')

    def test_failed_global_broadcast_does_not_hold_messages_broadcasts_or_beads(self):
        first=self.admit(scope='global')
        self.archive.start_action(first['id'])
        self.archive.finish_action(first['id'],'indeterminate',error='lost_receipt')
        self.processor.confirm(self.archive.get(first['id']),self.processor.settings(self.archive.get(first['id'])))
        msg=self.admit('agent-msg',target='node-b:website:site-worker')
        other_broadcast=self.admit(project='website')
        bead=self.admit('bd-file',project='website')
        self.drain()
        self.assertEqual(self.archive.get(msg['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(other_broadcast['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(bead['id'])['phase'],'created')
        self.assertEqual(self.archive.action(first['id'])['state'],'indeterminate')
        self.assertEqual(self.archive.action(first['id'])['attempts'],1)

    def test_failed_message_does_not_hold_broadcast_or_later_project_messages(self):
        first=self.admit('agent-msg')
        self.archive.start_action(first['id'])
        self.archive.finish_action(first['id'],'indeterminate',error='lost_receipt')
        self.processor.confirm(self.archive.get(first['id']),self.processor.settings(self.archive.get(first['id'])))
        b=self.admit()
        later=self.admit('agent-msg',target='node-b:sdk:other-worker')
        free=self.admit('agent-msg',target='node-b:website:site-worker')
        self.drain()
        self.assertEqual(self.archive.get(b['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(later['id'])['phase'],'stored')
        self.assertEqual(self.archive.get(free['id'])['phase'],'stored')
        self.assertEqual([r['input_id'] for r in self.sender.calls],[b['id'],later['id'],free['id']])

    def test_stored_and_partial_receipts_are_not_execution_claims(self):
        r=self.admit()
        def sender(recipe):
            response=broadcast_response(recipe)
            response['data']['delivery_receipt'].update(state='partially_delivered',delivered_recipients=1,failed_recipients=1,pending_recipients=1)
            return dispatch.receipt(recipe,response)
        self.processor.sender=sender
        self.drain()
        receipt=self.archive.receipt(r['id'])
        self.assertEqual(receipt['phase'],'blocked')
        self.assertEqual(receipt['error'],'broadcast_delivery_incomplete')
        self.assertEqual(receipt['action']['delivery']['failed_recipients'],1)
        self.assertTrue(any('some deliveries need attention' in str(c) for c in self.voice.calls))
