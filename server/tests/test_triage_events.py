"""Durable native JSON/SSE over real loopback HTTP, with isolated credentials."""
import contextlib
import json
import socket
import threading
import time
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

import test_command_pipeline as fixtures
import test_triage
import receiver
import triage_store


class EventsTest(unittest.TestCase):
    request = test_triage.TriageTest.request
    drain = fixtures.PipelineTest.drain

    def setUp(self):
        fixtures.PipelineTest.setUp(self)
        self.config['listen'].update(max_connections=8, request_timeout_secs=6)
        self.store = triage_store.Turns(self.archive)
        self.store.session('events'); self.store.session('other')

    def turn(self, key='one', session='events', **extra):
        return self.store.submit(session, {'client_turn_id': key, 'text': 'Fixture ' + key, 'mode': 'dry_run', **extra})

    def until(self, predicate):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if predicate(): return
            time.sleep(.01)
        self.fail('bounded event condition did not settle')

    @contextlib.contextmanager
    def http(self, worker=False):
        owner = receiver.Worker(self.archive, processor=self.processor)
        server = receiver.Server(self.archive, owner, ('127.0.0.1', 0), ingestor=self.archive.accept, routes={})
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        if worker: owner.thread.start()
        try:
            yield f'http://127.0.0.1:{server.server_port}', server
        finally:
            owner.stop.set(); owner.wake.set()
            server.shutdown(); server.server_close()
            thread.join(4)
            if owner.thread.is_alive(): owner.thread.join(5)
            self.assertFalse(thread.is_alive()); self.assertFalse(owner.thread.is_alive())
            self.assertEqual(server.session_waits, {})
            with self.assertRaises(OSError): socket.create_connection(server.server_address, timeout=.1)

    def frame(self, response):
        fields = {}
        while True:
            raw = response.readline()
            if not raw: return fields or None
            line = raw.decode().rstrip('\r\n')
            if not line:
                if fields: return fields
                continue
            if line.startswith(':'): continue
            key, _, value = line.partition(':')
            if key in ('event', 'id', 'data'): fields[key] = value.lstrip()

    def frames(self, response):
        result = []
        for _ in range(600):
            frame = self.frame(response)
            if frame is None: return result
            result.append(frame)
        self.fail('too many SSE frames')

    def burst(self, turn, count=270):
        for i in range(count):
            self.archive.update(turn['receipt']['id'], phase='ready' if i % 2 else 'routing')

    def error(self, url, status, **headers):
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.request(url, **headers)
        self.assertEqual(error.exception.code, status)
        result = json.load(error.exception)
        error.exception.close()
        return result

    def test_turn_json_preserved_and_sse_reconnect_uses_durable_sequences(self):
        turn = self.turn()
        with self.http() as (url, _):
            path = url + '/route/turns/' + turn['id'] + '/events'
            with self.request(path) as response:
                page = json.load(response)
            self.assertEqual(page['events'][0]['kind'], 'accepted')
            self.assertTrue(page['reset'])
            with self.request(path + '?seconds=3', Accept='text/event-stream', **{'Last-Event-ID': str(page['cursor'])}) as response:
                self.assertEqual(response.headers['Content-Type'], 'text/event-stream; charset=utf-8')
                self.drain()
                frames = self.frames(response)
            events = [json.loads(f['data']) for f in frames if f.get('event') == 'turn']
            expected = self.store.events(turn['id'], page['cursor'])['events']
            self.assertEqual(events, expected)
            self.assertTrue(all(e['turn_id'] == turn['id'] and e['session_id'] == 'events' for e in events))
            self.assertEqual([int(f['id']) for f in frames if f.get('event') == 'turn'], [e['seq'] for e in events])
            self.assertEqual(json.loads(frames[-1]['data'])['reason'], 'complete')
            with self.request(path, Accept='text/event-stream', **{'Last-Event-ID': str(events[-1]['seq'])}) as response:
                self.assertEqual([f['event'] for f in self.frames(response)], ['close'])
        self.assertEqual(self.sender.calls, [])

    def test_session_stream_observes_new_turns_and_never_other_session_history(self):
        secret = self.turn('other-private', session='other')
        with self.http(worker=True) as (url, _):
            with self.request(url + '/route/sessions/events/events?seconds=1', Accept='text/event-stream') as response:
                bootstrap = self.frame(response)
                self.assertEqual(bootstrap['event'], 'snapshot')
                self.assertEqual(json.loads(bootstrap['data'])['cursor'], 0)
                with self.request(url + '/route/sessions/events/turns', {'client_turn_id': 'streamed', 'text': 'A new fixture', 'mode': 'dry_run'}) as admitted:
                    turn = json.load(admitted)
                frames = self.frames(response)
            events = [json.loads(f['data']) for f in frames if f.get('event') == 'turn']
            self.assertTrue(any(e['kind'] == 'dry_run' for e in events))
            self.assertEqual({e['turn_id'] for e in events}, {turn['id']})
            self.assertNotIn(secret['id'], json.dumps(frames))
            self.assertEqual([e['seq'] for e in events], sorted({e['seq'] for e in events}))
            last = events[-1]['seq']
            with self.request(url + '/route/sessions/events/events', **{'Last-Event-ID': str(last)}) as response:
                self.assertEqual(json.load(response)['events'], [])
        self.assertEqual(self.sender.calls, [])

    def test_session_floor_detects_only_scoped_lost_events_and_reset_is_explicit(self):
        turn = self.turn(); other = self.turn(session='other')
        self.burst(other)
        page = self.store.session_events('events', turn['cursor'])
        self.assertEqual(page['events'], [])  # Global gaps are not session loss.
        self.assertEqual(page['retention_floor'], 0)
        self.burst(turn)
        for fn, identity, suffix in ((self.store.events, turn['id'], 'turn'), (self.store.session_events, 'events', 'session')):
            with self.assertRaises(triage_store.Error) as error: fn(identity, turn['cursor'])
            self.assertEqual(error.exception.code, 'cursor_expired_resync_' + suffix)
            initial = fn(identity, 0)
            self.assertTrue(initial['reset'])
            self.assertFalse(initial['history_complete'])
            self.assertEqual(initial['cursor'], initial['events'][-1]['seq'])
            self.assertLessEqual(len(initial['events']), 256)
        with self.http() as (url, _):
            value = self.error(url + '/route/sessions/events/events?after=' + str(turn['cursor']), 409)
            self.assertIn('resync_session', value['error']['code'])
            self.assertEqual(value['details']['resync'], '/route/sessions/events/events?after=0')

    def test_pruning_while_stream_open_emits_resync_not_silent_history_skip(self):
        turn = self.turn()
        # The stream deadline is min(seconds, request_timeout - 1). The burst's
        # trigger work can take several seconds on a loaded CI host.
        self.archive.config['listen']['request_timeout_secs'] = 12
        with self.http() as (url, _):
            with self.request(url + '/route/sessions/events/events?seconds=10', Accept='text/event-stream',
                              **{'Last-Event-ID': str(turn['cursor'])}) as response:
                # Commit the whole burst together: the reader cannot keep up
                # between updates and accidentally avoid the expiry scenario.
                with self.archive.db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    for i in range(300):
                        db.execute('UPDATE command_inputs SET phase=?,updated=? WHERE id=?',
                                   ('dry_ready' if i % 2 else 'dry_routing', time.time(), turn['receipt']['id']))
                frames = self.frames(response)
            self.assertEqual(frames[-1]['event'], 'resync')
            self.assertEqual(json.loads(frames[-1]['data'])['code'], 'cursor_expired_resync_session')
            self.assertNotIn('id', frames[-1])

    def test_page_byte_budget_and_bootstrap_snapshot_are_bounded_without_secret_settings(self):
        turn = self.turn()
        with self.archive.db() as db:
            data = json.dumps({'phase': 'dry_run', 'action': 'bd-file', 'title': 'x' * 3000})
            db.executemany('INSERT INTO triage_events(turn_id,at,kind,data) VALUES(?,?,?,?)',
                           [(turn['id'], time.time(), 'dry_run', data)] * 140)
            db.execute('INSERT INTO triage_events(turn_id,at,kind,data) VALUES(?,?,?,?)',
                       (turn['id'], time.time(), 'dry_run', json.dumps({'private': 'x' * 20000})))
        reset = self.store.session_events('events')
        self.assertFalse(reset['history_complete'])
        self.assertTrue(any(e.get('details_truncated') for e in reset['events']))
        self.assertLess(len(json.dumps(reset).encode()), 256 * 1024)
        self.assertNotIn(self.legacy.token, json.dumps(reset))
        self.assertNotIn('settings', json.dumps(reset))
        cursor, seen = turn['cursor'], []
        for _ in range(10):
            page = self.store.session_events('events', cursor)
            self.assertLess(len(json.dumps(page).encode()), 256 * 1024)
            seen += [e['seq'] for e in page['events']]
            cursor = page['cursor']
            if not page['has_more']: break
        self.assertEqual(len(seen), 141)
        self.assertEqual(len(set(seen)), 141)
        self.assertEqual(cursor, reset['cursor'])

    def test_bootstrap_latest_history_and_snapshot_omissions_are_declared(self):
        for i in range(130):
            turn = self.turn(str(i)); self.store.cancel(turn['id'])
        page = self.store.session_events('events')
        self.assertTrue(page['reset'])
        self.assertFalse(page['history_complete'])
        self.assertTrue(page['snapshot']['truncated'])
        self.assertEqual(len(page['snapshot']['turns']), 100)
        self.assertEqual(len(page['events']), 256)
        self.assertFalse(page['has_more'])  # Bootstrap advances, not a historical pagination promise.
        self.assertEqual(self.store.session_events('events', page['cursor'])['events'], [])

    def test_cursor_header_query_conflicts_and_malformed_values_fail_before_streaming(self):
        turn = self.turn()
        with self.http() as (url, _):
            base = url + '/route/turns/' + turn['id'] + '/events'
            for query in ('after=-1', 'after=1&after=2', 'after=', 'after=9223372036854775808', 'seconds=0', 'token=secret', 'after=1.0'):
                self.error(base + '?' + query, 400, Accept='text/event-stream')
            self.error(base + '?after=0', 400, **{'Last-Event-ID': str(turn['cursor'])})
            self.error(base, 400, **{'Last-Event-ID': '-1'})
            self.error(base + '?after=9999999', 409)
            self.error(url + '/route/sessions/missing/events', 404)
            with self.request(base + '?after=' + str(turn['cursor']), **{'Last-Event-ID': str(turn['cursor'])}) as response:
                self.assertEqual(json.load(response)['events'], [])

    def test_shared_global_and_session_wait_limits_keep_control_and_admission_available(self):
        self.store.session('third')
        with self.http() as (url, server):
            opened = []
            try:
                for _ in range(2):
                    response = self.request(url + '/route/sessions/events/events?seconds=2', Accept='text/event-stream')
                    opened.append(response); self.assertEqual(self.frame(response)['event'], 'snapshot')
                self.error(url + '/route/sessions/events/events', 429, Accept='text/event-stream')
                for _ in range(2):
                    response = self.request(url + '/route/sessions/other/events?seconds=2', Accept='text/event-stream')
                    opened.append(response); self.frame(response)
                self.assertEqual(sum(server.session_waits.values()), 4)
                self.error(url + '/route/sessions/third/events', 429, Accept='text/event-stream')
                with self.request(url + '/route/capabilities') as response:
                    caps = json.load(response)
                self.assertEqual(caps['wait_slots'], 4)
                self.assertEqual(caps['session_wait_slots'], 2)
                with self.request(url + '/route/sessions/third/turns', {'client_turn_id': 'control', 'text': 'control admission'}) as response:
                    turn = json.load(response)
                with self.request(url + '/route/turns/' + turn['id']) as response:
                    self.assertFalse(json.load(response)['done'])
                before = len(self.store.session_turns('third')['turns'])
                chat = {'model': 'triage-route-dry-run', 'messages': [{'role': 'user', 'content': 'Not admitted'}],
                        'triage': {'session_id': 'third'}}
                with self.assertRaises(urllib.error.HTTPError) as error:
                    self.request(url + '/route/v1/chat/completions', chat)
                self.assertEqual(error.exception.code, 429); error.exception.close()
                self.assertEqual(len(self.store.session_turns('third')['turns']), before)
            finally:
                for response in opened: response.close()
            self.until(lambda: not server.session_waits)
        self.assertEqual(self.sender.calls, [])

    def test_bearer_rotation_closes_existing_stream_without_new_state_disclosure(self):
        with self.http() as (url, server):
            with self.request(url + '/route/sessions/events/events?seconds=3', Accept='text/event-stream') as response:
                self.frame(response)
                (self.root / 'webhook-token').write_text('rotated-fixture-token')
                turn = self.turn('after-rotation')
                remainder = response.read().decode()
                self.assertNotIn(turn['id'], remainder)
                self.assertNotIn('event: turn', remainder)
            self.until(lambda: not server.session_waits)
            self.error(url + '/route/capabilities', 401)
            with self.request(url + '/route/capabilities', Authorization='Bearer rotated-fixture-token') as response:
                self.assertTrue(json.load(response)['native_sse'])

    def test_slow_socket_cannot_hold_worker_or_database_and_times_out_its_slot(self):
        turn = self.turn(); self.drain()
        with self.archive.db() as db:
            data = json.dumps({'phase': 'dry_run', 'action': 'bd-file', 'title': 'x' * 3000})
            db.executemany('INSERT INTO triage_events(turn_id,at,kind,data) VALUES(?,?,?,?)',
                           [(turn['id'], time.time(), 'dry_run', data)] * 200)
        setup = receiver.Handler.setup
        def tiny_send(handler):
            setup(handler)
            handler.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        with patch.object(receiver.Handler, 'setup', tiny_send), self.http(worker=True) as (url, server):
            slow = socket.socket()
            slow.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
            slow.settimeout(3)
            try:
                slow.connect(server.server_address)
                slow.sendall(('GET /route/sessions/events/events?seconds=2 HTTP/1.1\r\nHost: fixture\r\n'
                              'Accept: text/event-stream\r\nAuthorization: Bearer ' + self.legacy.token + '\r\n\r\n').encode())
                headers = b''
                while b'\r\n\r\n' not in headers: headers += slow.recv(1)
                self.assertIn(b'200 OK', headers)
                # Deliberately never consume the large snapshot body.
                started = time.monotonic()
                with self.request(url + '/route/sessions/other/turns', {'client_turn_id': 'during-slow', 'text': 'Unrelated fixture', 'mode': 'live'}) as response:
                    admitted = json.load(response)
                self.assertLess(time.monotonic() - started, 1)
                self.until(lambda: self.store.get(admitted['id'])['phase'] == 'created')
                self.until(lambda: not server.session_waits)
                self.assertIsNone(server.worker.fatal)
                self.assertEqual(len(self.sender.calls), 1)
            finally: slow.close()

    def test_parallel_capabilities_and_approval_keep_existing_native_capi_contract(self):
        preview = self.turn(); self.drain(); preview = self.store.get(preview['id'])
        body = {'client_turn_id': 'approve-once', 'operation': 'approve', 'reference_turn': preview['id'],
                'expected_revision': preview['proposal_revision'], 'mode': 'live', 'text': 'Exactly that.'}
        with self.http(worker=True) as (url, _):
            with self.request(url + '/route/capabilities') as response:
                caps = json.load(response)
            self.assertEqual(caps['stage_workers'], 4)
            self.assertEqual(caps['scheduling'], 'bounded_stage_workers')
            self.assertTrue(caps['preview_approval']); self.assertTrue(caps['cancel'])
            self.assertTrue(caps['clarification']); self.assertFalse(caps['agent_reply_wait'])
            chat = {'model': 'triage-route', 'messages': [{'role': 'user', 'content': body['text']}],
                    'triage': {'session_id': 'events', 'turn_id': body['client_turn_id'], 'purpose': 'user',
                               **{k: body[k] for k in ('operation', 'reference_turn', 'expected_revision')}}}
            with self.request(url + '/route/v1/chat/completions', chat) as response:
                result = json.load(response)
            self.assertIn('Created', result['choices'][0]['message']['content'])
            with self.request(url + '/route/sessions/events/turns', body) as response:
                repeated = json.load(response)
            self.assertEqual('chatcmpl-' + repeated['id'], result['id'])
            self.assertEqual(repeated['phase'], 'created')
        self.assertEqual(len(self.sender.calls), 1)
        from test_triage_workflows import assert_preserved_preview
        assert_preserved_preview(self, preview, self.store.get(preview['id']), repeated['id'])

    def test_worker_stop_closes_native_stream_and_preserves_pending_input(self):
        turn = self.turn()
        with self.http() as (url, server):
            with self.request(url + '/route/sessions/events/events?seconds=4', Accept='text/event-stream') as response:
                self.frame(response)
                server.worker.stop.set(); server.worker.wake.set()
                frames = self.frames(response)
            self.assertEqual(json.loads(frames[-1]['data'])['reason'], 'worker_stopped')
            self.assertEqual(self.store.get(turn['id'])['phase'], 'dry_ready')
            self.assertEqual(self.sender.calls, [])


if __name__ == '__main__': unittest.main()
