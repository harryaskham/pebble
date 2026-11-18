"""Deterministic ctx -> snapshot projection and subscription control tests."""
import contextlib
import copy
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import context_from_ctx as ctx
from jev_router import ACTIVE_STATES, validate_snapshot
from process_io import ProcessError, Result


def fixture(now=1000):
    values = {
        'cacophony.projects.a_skh_am-hash': {
            'name': 'a.skh.am', 'description': 'Website', 'description_checked_at': now-100,
            'recent_beads': [{'id': 'bd-one', 'title': 'A task', 'description': 'Details', 'status': 'open', 'priority': 2}],
        },
        'cacophony.agents.a_skh_am-hash.visiting-one': {
            'id': 'visiting-one', 'target': 'node-a:a.skh.am:visiting-one', 'node': 'node-a',
            'project': 'a.skh.am', 'name': 'Visible visitor', 'state': next(iter(sorted(ACTIVE_STATES))),
            'goal': 'Working', 'beads': ['bd-one'], 'inventory_source': 'local_live',
        },
        'cacophony.agents.a_skh_am-hash.stopped': {
            'id': 'stopped', 'target': 'node-a:a.skh.am:stopped', 'node': 'node-a',
            'project': 'a.skh.am', 'name': 'Stopped', 'state': 'stopped', 'beads': [],
        },
        'cacophony.nodes.node-a': {'name': 'node-a', 'liveness': 'alive'},
        'cacophony.messages.older': {'id': 'older', 'ts': '1970-01-01T00:15:00Z', 'body': 'Older', 'project': 'a.skh.am'},
        'cacophony.messages.newer': {'id': 'newer', 'ts': '1970-01-01T01:16:00+01:00', 'body': 'Newer', 'project': 'a.skh.am'},
    }
    sections = {name: {'observed_at': now-offset, 'count': sum(k.startswith('cacophony.'+name+'.') for k in values)}
                for name, offset in [('projects', 1), ('agents', 4), ('nodes', 3), ('messages', 2)]}
    values['cacophony.sync'] = {'status': 'ok', 'errors': {}, 'observed_at': now, 'sections': sections}
    return {'found': True, 'entries': [{'key': key, 'value': value, 'updated_at': '1960-01-01T00:00:00Z'}
                                     for key, value in values.items()], 'count': len(values), 'omitted': 0, 'head_rev': 10}


def record(data, key):
    return next(entry['value'] for entry in data['entries'] if entry['key'] == key)


class ProjectionTest(unittest.TestCase):
    def test_maps_existing_contract_and_uses_value_names_not_escaped_keys(self):
        data = fixture(); before = copy.deepcopy(data)
        result = ctx.project(data, now=1000)
        validate_snapshot(result, now=1000)
        self.assertEqual(result['version'], 1)
        self.assertEqual(result['created_at'], 996)
        self.assertEqual(result['projects'][0]['id'], 'a.skh.am')
        self.assertEqual(result['projects'][0]['description_at'], 900)
        self.assertEqual(result['projects'][0]['beads_at'], 999)
        self.assertEqual(result['projects'][0]['beads'][0]['id'], 'bd-one')
        self.assertEqual([a['target'] for a in result['agents']], ['node-a:a.skh.am:visiting-one'])
        self.assertEqual(result['agents'][0]['inventory_source'], 'local_live')
        self.assertEqual(result['agents'][0]['beads'], ['bd-one'])
        self.assertEqual(result['nodes'][0]['liveness'], 'alive')
        self.assertEqual([m['id'] for m in result['messages']], ['newer', 'older'])
        self.assertEqual(data, before)

    def test_bounds_preview_fields_and_keeps_all_candidates(self):
        data=fixture()
        project=record(data,'cacophony.projects.a_skh_am-hash')
        project['description']='x'*1000
        project['recent_beads']*=5
        record(data,'cacophony.messages.newer')['body']='x'*700
        result=ctx.project(data,now=1000,beads_limit=2,messages_limit=1)
        self.assertEqual(len(result['projects']),1)
        self.assertEqual(len(result['projects'][0]['description']),600)
        self.assertEqual(len(result['projects'][0]['beads']),2)
        self.assertEqual(len(result['messages']),1)
        self.assertEqual(len(result['messages'][0]['body']),500)
        self.assertTrue(result['messages'][0]['body_truncated'])

    def test_partial_stale_invalid_or_omitted_ctx_preserves_last_good_file(self):
        def status(data): record(data,'cacophony.sync')['status']='failed'
        def errors(data): record(data,'cacophony.sync')['errors']={'agents':'unavailable'}
        def stale(data): record(data,'cacophony.sync')['sections']['agents']['observed_at']=1
        def stale_global(data): record(data,'cacophony.sync')['observed_at']=1
        def partial(data): data['entries'].pop(0); data['count']-=1
        def omitted(data): data['omitted']=1
        def no_sync(data): data['entries']=[e for e in data['entries'] if e['key']!='cacophony.sync'];data['count']-=1
        def invalid(data): record(data,'cacophony.agents.a_skh_am-hash.visiting-one')['target']='wrong:project:target'
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'context.json'
            ctx.refresh(fixture(),output,now=1000,max_age=300)
            before=output.read_bytes(); stamp=output.stat().st_mtime_ns
            for change in (status,errors,stale,stale_global,partial,omitted,no_sync,invalid):
                with self.subTest(change=change.__name__):
                    data=fixture();change(data)
                    with self.assertRaises(ctx.ContextError):ctx.refresh(data,output,now=1000,max_age=300)
                    self.assertEqual(output.read_bytes(),before)
                    self.assertEqual(output.stat().st_mtime_ns,stamp)

    def test_managed_output_symlink_and_identical_snapshot_noop(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);target=root/'private/context.json';target.parent.mkdir()
            link=root/'context.json';link.symlink_to('private/context.json')
            result=ctx.refresh(fixture(),link,now=1000)
            self.assertTrue(result['written']);self.assertTrue(link.is_symlink())
            before=target.stat().st_mtime_ns
            again=ctx.refresh(fixture(),link,now=1010)
            self.assertFalse(again['written']);self.assertEqual(target.stat().st_mtime_ns,before)
            self.assertEqual(json.loads(link.read_text())['created_at'],996)

    def test_timestamps_are_utc_observations_and_not_projection_or_key_age(self):
        self.assertEqual(ctx.timestamp('1970-01-01T01:00:01+01:00'),1)
        for bad in (None,True,float('nan'),'1970-01-01T00:00:01','bad'):
            with self.assertRaises(ctx.ContextError):ctx.timestamp(bad)
        data=fixture();record(data,'cacophony.sync')['sections']['nodes']['observed_at']=1100
        with self.assertRaisesRegex(ctx.ContextError,'stale_nodes'):ctx.project(data,now=1000)

    def test_empty_healthy_sections_are_not_confused_with_partial_data(self):
        data=fixture();sync=record(data,'cacophony.sync')
        for value in sync['sections'].values():value['count']=0
        data['entries']=[{'key':'cacophony.sync','value':sync}];data['count']=1
        result=ctx.project(data,now=1000)
        for section in ctx.SECTIONS:self.assertEqual(result[section],[])


class ClientAndSubscriptionTest(unittest.TestCase):
    def test_filesystem_failure_does_not_claim_the_old_file_survived_a_write(self):
        with contextlib.redirect_stderr(io.StringIO()) as stderr:
            ctx.report_error(OSError('private-path-or-provider-text'))
        result=json.loads(stderr.getvalue())
        self.assertTrue(result['cache_write_unconfirmed'])
        self.assertNotIn('previous_snapshot_preserved',result)
        self.assertNotIn('private-path',stderr.getvalue())

    def test_only_native_get_and_watch_commands_no_filtering_or_secret_argv(self):
        calls=[]
        def runner(argv,**kwargs):
            calls.append((argv,kwargs))
            return Result(json.dumps({'status':'success','data':fixture()}).encode(),1)
        client=ctx.CtxReads('/fixture/ctx','/fixture/managed-config.yaml',runner=runner,env={'HOME':'/fixture'})
        data=client.get();client.wait(data['head_rev'])
        self.assertEqual(calls[0][0],['/fixture/ctx','--config','/fixture/managed-config.yaml','--json','get','cacophony','--format','ndjson'])
        self.assertEqual(calls[1][0],['/fixture/ctx','--config','/fixture/managed-config.yaml','--json','watch','cacophony.sync','--since','10','--once','--timeout','120s'])
        self.assertEqual(calls[0][1]['max_output'],ctx.MAX_READ_BYTES)
        self.assertNotIn('--max-age',calls[0][0]);self.assertNotIn('--max-bytes',calls[0][0])

    def test_bootstrap_then_revision_bound_watch_coalesces_newer_view(self):
        data=fixture();events=[]
        class Client:
            def get(self):events.append('get');return copy.deepcopy(data)
            def wait(self,since,timeout):
                events.append(('watch',since))
                if since==10:
                    data['head_rev']=12
                    record(data,'cacophony.projects.a_skh_am-hash')['description']='Latest after burst'
                    record(data,'cacophony.sync')['sections']['agents']['observed_at']=999
                else:raise KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            path=Path(directory)/'context.json'
            with self.assertRaises(KeyboardInterrupt):ctx.follow(Client(),path,now=1000)
            result=json.loads(path.read_text())
        self.assertEqual(events,['get',('watch',10),'get',('watch',12)])
        self.assertEqual(result['projects'][0]['description'],'Latest after burst')
        self.assertEqual(result['created_at'],997)

    def test_unavailable_bootstrap_retries_and_bad_sync_waits_for_event_without_erasing_cache(self):
        data=fixture();calls=[]
        class Client:
            def get(self):
                calls.append('get')
                if calls.count('get')==1:raise ProcessError('command_failed')
                current=copy.deepcopy(data)
                if calls.count('get')==2:record(current,'cacophony.sync')['status']='failed'
                return current
            def wait(self,since,timeout):
                calls.append(('watch',since))
                if calls.count('get')==2:data['head_rev']=11
                else:raise KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as errors:
            path=Path(directory)/'context.json';path.write_text('last good')
            def sleep(seconds):calls.append(('retry',seconds));self.assertEqual(path.read_text(),'last good')
            with self.assertRaises(KeyboardInterrupt):ctx.follow(Client(),path,now=1000,sleep=sleep)
            self.assertEqual(json.loads(path.read_text())['version'],1)
            self.assertIn('previous_snapshot_preserved',errors.getvalue())
        self.assertEqual(calls,['get',('retry',5),'get',('watch',10),'get',('watch',11)])

    def test_watch_expiry_reboots_snapshot_without_inflating_its_age(self):
        data=fixture();waits=[]
        class Client:
            def get(self):return data
            def wait(self,since,timeout):
                waits.append(since)
                if len(waits)==1:raise ProcessError('command_failed',7)
                raise KeyboardInterrupt()
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()) as logs:
            path=Path(directory)/'context.json'
            with self.assertRaises(KeyboardInterrupt):ctx.follow(Client(),path,now=1000)
            self.assertEqual(json.loads(path.read_text())['created_at'],996)
            self.assertFalse(json.loads(logs.getvalue().splitlines()[-1])['written'])
        self.assertEqual(waits,[10,10])


if __name__=='__main__':unittest.main()
