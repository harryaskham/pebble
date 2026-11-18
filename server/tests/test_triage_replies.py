"""REP-01..07: isolated real HTTP peers, disposable credentials, no live Caco."""
import contextlib
import copy
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import receiver
import triage_replies as replies

TARGET = 'fixture-node:fixture-project:fixture-agent'
TOKEN = 'fixture-reply-token'


def receipt():
    return {'id': 'a' * 32, 'phase': 'stored', 'dry_run': False,
            'action': {'state': 'stored', 'action': 'pebble', 'project': 'fixture-project',
                       'target': TARGET, 'message_id': 'msg-original', 'request_id': 'pebble-command-' + 'a' * 32}}


def message(number=1, **changes):
    body = 'Agent says the job is done; this is not execution proof.'
    result = {'id': 'msg-reply-' + str(number), 'project': 'fixture-project', 'scope': 'project',
              'sender': TARGET, 'target': 'fixture-node:fixture-project:operator', 'kind': 'dm',
              'body': body, 'body_len': len(body.encode()),
              'ts': (datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=number)).isoformat(),
              'reply_to': 'msg-original', 'sender_kind': 'agent', 'origin_principal': 'agent:fixture-agent',
              'operator_authenticated': False}
    result.update(changes)
    return result


def envelope(messages):
    return {'ok': True, 'data': {'messages': messages, 'count': len(messages)},
            'meta': {'request_id': 'fixture-http-request', 'node': 'fixture-node'}}


@contextlib.contextmanager
def peer(specs, *, tls_context=None):
    calls = []
    stopping = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def do_GET(self):
            index = len(calls)
            calls.append({'method': self.command, 'path': self.path, 'headers': dict(self.headers)})
            spec = specs[index] if index < len(specs) else {'status': 500}
            try:
                if callable(spec):
                    spec = spec(self)
                    if spec is None:
                        return
                if spec.get('delay'):
                    stopping.wait(spec['delay'])
                body = spec.get('body', json.dumps(spec.get('json', envelope([]))).encode())
                headers = {'Content-Type': 'application/json', 'Connection': 'close', **spec.get('headers', {})}
                if spec.get('length', True):
                    headers.setdefault('Content-Length', str(len(body)))
                self.send_response(spec.get('status', 200))
                for key, value in headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                self.close_connection = True

        do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_GET

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    if tls_context:
        server.socket = tls_context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01))
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}', calls, stopping
    finally:
        stopping.set()
        server.shutdown()
        server.server_close()
        thread.join(2)
        assert not thread.is_alive()
        with contextlib.suppress(ConnectionRefusedError), socket.create_connection(server.server_address, timeout=.1):
            raise AssertionError('fixture listener leaked')


class ReplyObserverTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='triage-reply-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.token = self.root / 'token'
        self.token.write_text(TOKEN)
        self.token.chmod(0o600)
        self.config = {'commands': {'caco': {'daemon_url': '', 'token_file': str(self.token), 'timeout_secs': 130}}}

    def observe_at(self, url, source=None, **kwargs):
        self.config['commands']['caco']['daemon_url'] = url
        return replies.observe(receipt() if source is None else source, self.config, **kwargs)

    def assert_unavailable(self, result, reason=None):
        self.assertEqual(result['status'], 'unavailable', result)
        self.assertEqual(result['replies'], [])
        self.assertFalse(result['window']['exhaustive'])
        if reason:
            self.assertEqual(result['reason'], reason)
        self.assertNotIn(TOKEN, json.dumps(result))
        self.assertNotIn(str(self.token), json.dumps(result))

    def test_real_http_exact_request_and_read_only_projection(self):
        source = receipt()
        for action in ('pebble', 'agent-msg'):
            source['action']['action'] = action
            original = copy.deepcopy(source)
            msg = message(unknown={'private': 'never-forward-this'})
            with peer([{'json': envelope([msg])}]) as (url, calls, _):
                # Current config target must not replace the persisted recipient.
                self.config['commands']['pebble_target'] = 'else:else:else'
                self.config['commands']['caco']['daemon_url'] = url + '/gateway'
                configured = copy.deepcopy(self.config)
                result = replies.observe(source, self.config)
                self.assertEqual(result['status'], 'correlated_reply')
                self.assertEqual(result['receipt']['message_id'], 'msg-original')
                self.assertEqual(result['receipt']['state'], 'stored')
                self.assertEqual(source, original)
                self.assertEqual(self.config, configured)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0]['method'], 'GET')
                request = urlsplit(calls[0]['path'])
                self.assertEqual(request.path, '/gateway/api/v1/projects/fixture-project/messages/chat')
                self.assertEqual(parse_qs(request.query), {'limit': ['20'], 'body_preview': ['4096']})
                self.assertEqual(calls[0]['headers']['Authorization'], 'Bearer ' + TOKEN)
                self.assertEqual(calls[0]['headers']['Accept-Encoding'], 'identity')
                reply = result['replies'][0]
                self.assertEqual(reply['reply_to'], source['action']['message_id'])
                self.assertEqual(reply['sender'], source['action']['target'])
                self.assertEqual(reply['provenance']['content_role'], 'agent_assertion')
                self.assertNotIn('unknown', reply)
                self.assertNotIn('completed', result)
                self.assertFalse(reply['body_truncated'])
                self.assertGreater(result['window']['response_bytes'], len(json.dumps(envelope([msg])).encode()))

    def test_ineligible_has_zero_secret_reads_and_zero_connections(self):
        cases = []
        for action in ('bd-file', 'bd-expand', 'broadcast', 'custom:hello'):
            item = receipt(); item['action']['action'] = action
            cases.append(item)
        for key, value in [('dry_run', True), ('phase', 'dry_run'), ('action', None)]:
            item = receipt(); item[key] = value; cases.append(item)
        for key, value in [('state', 'planned'), ('state', 'attempting'), ('state', 'indeterminate'),
                           ('state', 'created'), ('scope', 'global'), ('executed', False), ('dry_run', True)]:
            item = receipt(); item['action'][key] = value; cases.append(item)
        with peer([]) as (url, calls, _), patch.object(receiver, 'read_secret', side_effect=AssertionError('secret read')), \
                patch.object(replies, '_connect', side_effect=AssertionError('connection')):
            for item in cases:
                with self.subTest(item=item):
                    result = self.observe_at(url, item)
                    self.assertEqual(result['status'], 'not_applicable')
                    self.assertEqual(result['window']['requests'], 0)
            self.assertEqual(calls, [])

    def test_missing_identity_never_guessed_from_raw_or_request_id(self):
        cases = [None, [], {}, {'action': []}]
        for key, value in [('message_id', None), ('message_id', ''), ('message_id', 'msg-a/../b'),
                           ('project', None), ('project', '../else'), ('project', 'other-project'),
                           ('target', None), ('target', '*'), ('target', TARGET + ',other'),
                           ('target', TARGET + '\n'), ('dry_run', 'false')]:
            item = receipt(); item['action'][key] = value; cases.append(item)
        for key in ('dry_run', 'id'):
            item = receipt(); del item[key]; cases.append(item)
        item = receipt(); del item['action']['message_id']
        item['action']['raw'] = {'id': 'msg-original'}
        cases.append(item)
        with patch.object(receiver, 'read_secret', side_effect=AssertionError('secret read')), \
                patch.object(replies, '_connect', side_effect=AssertionError('connection')):
            for item in cases:
                result = replies.observe(item, self.config)
                self.assertIn(result['status'], ('unavailable', 'not_applicable'))
                self.assertEqual(result['window']['requests'], 0)

    def test_exact_correlation_rejects_cross_project_sender_reference_and_operator_spoof(self):
        variations = [{'project': 'other-project'}, {'project': '', 'scope': 'global', 'kind': 'broadcast'},
                      {'sender': 'fixture-agent'}, {'sender': TARGET.upper()}, {'sender': 'other:' + TARGET},
                      {'reply_to': None}, {'reply_to': 'msg-original-suffix'}, {'sender_kind': 'operator', 'operator_authenticated': True},
                      {'sender_kind': 'operator'}, {'sender_kind': 'relay'}, {'sender_kind': 'unknown'},
                      {'sender_kind': 'agent', 'operator_authenticated': True}, {'sender_kind': 'Agent'}]
        messages = [message(i, **value) for i, value in enumerate(variations)]
        with peer([{'json': envelope(messages)}]) as (url, _, _):
            result = self.observe_at(url)
        self.assertEqual(result['status'], 'not_observed_in_window')
        self.assertEqual(result['window']['candidate_count'], len(variations))
        self.assertEqual(result['window']['matched_count'], 0)
        self.assertFalse(result['window']['exhaustive'])

    def test_original_sender_or_reply_target_not_required_and_optional_origin_preserved(self):
        one = message(target=None)
        del one['origin_principal']
        two = message(2, target='any-operator-device', origin_principal='node:fixture-node')
        with peer([{'json': envelope([one, two])}]) as (url, _, _):
            result = self.observe_at(url)
        self.assertEqual(result['status'], 'correlated_reply')
        self.assertIsNone(result['replies'][0]['provenance']['origin_principal'])
        self.assertEqual(result['replies'][1]['provenance']['origin_principal'], 'node:fixture-node')

    def test_utf8_preview_truncation_preserved_without_full_body_fetch(self):
        body = '🌱' * 1024
        messages = [message(body=body, body_len=5000, body_truncated=True)]
        with peer([{'json': envelope(messages)}]) as (url, calls, _):
            result = self.observe_at(url)
        self.assertEqual(result['status'], 'correlated_reply')
        self.assertEqual(len(calls), 1)
        self.assertEqual(result['replies'][0]['body'], body)
        self.assertEqual(result['replies'][0]['body_len'], 5000)
        self.assertIs(result['replies'][0]['body_truncated'], True)

    def test_three_pages_limit_order_and_cursor_with_nanosecond_precision(self):
        pages = [[message(n) for n in range(start, start + 20)] for start in (40, 20, 0)]
        pages[-1][0]['ts'] = '2026-01-01T00:00:00.000000002Z'
        pages[-1][1]['ts'] = '2026-01-01T00:00:00.000000001Z'
        with peer([{'json': envelope(page)} for page in pages]) as (url, calls, _):
            result = self.observe_at(url)
        self.assertEqual(result['status'], 'correlated_reply')
        self.assertEqual(result['reason'], 'page_limit')
        self.assertEqual(result['window']['candidate_count'], 60)
        self.assertEqual(result['window']['matched_count'], 60)
        self.assertEqual(result['window']['pages'], 3)
        self.assertEqual(len(calls), 3)
        self.assertEqual([r['message_id'] for r in result['replies']], ['msg-reply-'+str(n) for n in range(52, 60)])
        self.assertEqual(result['window']['oldest_ts'], pages[-1][1]['ts'])
        self.assertEqual(result['window']['returned_count'], 8)
        self.assertTrue(result['window']['reply_list_truncated'])
        for i in (1, 2):
            self.assertEqual(parse_qs(urlsplit(calls[i]['path']).query)['before'], [pages[i - 1][0]['ts']])
        self.assertFalse(result['window']['exhaustive'])

    def test_unobserved_is_bounded_window_not_absence_even_empty_or_collision(self):
        for pages, reason in [([[]], 'empty_page'),
                              ([[message(reply_to=None)]], 'short_page'),
                              ([[message(n, reply_to=None, ts='2026-01-01T00:00:00Z') for n in range(20)], []], 'empty_page'),
                              ([[message(n, reply_to=None) for n in range(start, start + 20)] for start in (40, 20, 0)], 'page_limit')]:
            with self.subTest(reason=reason), peer([{'json': envelope(page)} for page in pages]) as (url, calls, _):
                result = self.observe_at(url)
                self.assertEqual(result['status'], 'not_observed_in_window')
                self.assertEqual(result['reason'], reason)
                self.assertFalse(result['window']['exhaustive'])
                self.assertEqual(len(calls), len(pages))

    def test_repeated_messages_and_nonadvancing_cursor_stop_truthfully(self):
        first = [message(n) for n in range(20)]
        for second, reason in [(first, 'repeated_message'),
                               ([message(100 + n) for n in range(20)], 'cursor_not_advanced')]:
            with peer([{'json': envelope(first)}, {'json': envelope(second)}]) as (url, calls, _):
                result = self.observe_at(url)
            self.assertEqual(result['reason'], reason)
            self.assertEqual(result['status'], 'correlated_reply')
            self.assertEqual(len(calls), 2)
            ids = [item['message_id'] for item in result['replies']]
            self.assertEqual(len(ids), len(set(ids)))
            self.assertFalse(result['window']['exhaustive'])

    def test_conflicting_message_identity_discards_partial_replies(self):
        first = [message(n) for n in range(20)]
        second = [message(0, body='different', body_len=9)]
        with peer([{'json': envelope(first)}, {'json': envelope(second)}]) as (url, _, _):
            self.assert_unavailable(self.observe_at(url), 'conflicting_message_identity')

    def test_malformed_messages_and_unbounded_metadata_fail_closed(self):
        variations = [{'id': None}, {'id': 'x'}, {'project': []}, {'sender': []}, {'target': {}},
                      {'scope': None}, {'kind': []}, {'sender_kind': None}, {'operator_authenticated': None},
                      {'operator_authenticated': 0}, {'reply_to': 42}, {'origin_principal': {}},
                      {'origin_principal': 'x' * 513}, {'origin_principal': 'bad\nmetadata'},
                      {'body': None}, {'body': 'x' * 4097, 'body_len': 4097}, {'body_len': True},
                      {'body_len': -1}, {'body_len': 2**64}, {'body_len': 9999}, {'body_truncated': 'true'},
                      {'body_truncated': True}, {'ts': 'yesterday'}, {'ts': '2026-02-31T00:00:00Z'},
                      {'ts': '2026-01-01T00:00:00'}, {'ts': '2026-01-01T00:00:00+00:99'}, {'body': '\ud800'}]
        for changes in variations:
            with self.subTest(changes=changes), peer([{'json': envelope([message(**changes)])}]) as (url, _, _):
                self.assert_unavailable(self.observe_at(url), 'invalid_response')

    def test_malformed_envelopes_fail_closed(self):
        cases = [b'not-json ' + TOKEN.encode(), b'\xff', b'[]', b'{"ok":true,"ok":false}',
                 json.dumps({'ok': True, 'data': {'messages': [], 'count': True}}).encode(),
                 json.dumps({'ok': False, 'error': {'message': TOKEN}}).encode(),
                 json.dumps(envelope([message(n) for n in range(21)])).encode(),
                 b'{"ok":true,"data":{"messages":[],"count":0},"bad":NaN}',
                 b'[' * 2000 + b']' * 2000]
        for body in cases:
            with self.subTest(body=body[:50]), peer([{'body': body}]) as (url, _, _):
                self.assert_unavailable(self.observe_at(url), 'invalid_response')

    def test_failure_on_later_page_never_claims_no_reply_or_returns_partial_content(self):
        for status in (403, 429, 500):
            with peer([{'json': envelope([message(n) for n in range(20)])},
                       {'status': status, 'body': TOKEN.encode()}]) as (url, calls, _):
                result = self.observe_at(url)
            self.assert_unavailable(result, 'http_unavailable')
            self.assertEqual(result['window']['pages'], 1)
            self.assertEqual(len(calls), 2)

    def test_redirects_never_followed_or_forward_credentials(self):
        for status in (301, 302, 303, 307, 308):
            with peer([]) as (other, forwarded, _), peer([{'status': status, 'headers': {'Location': other + '/stolen'}}]) as (url, calls, _):
                self.assert_unavailable(self.observe_at(url), 'redirect_refused')
                self.assertEqual(forwarded, [])
                self.assertEqual(len(calls), 1)
        with peer([lambda handler: {'status': 302, 'headers': {'Location': '/same-host'}}]) as (url, calls, _):
            self.assert_unavailable(self.observe_at(url), 'redirect_refused')
            self.assertEqual(len(calls), 1)

    def test_invalid_bases_refused_before_credential_read(self):
        urls = ['file:///etc/passwd', 'ftp://localhost', 'http://', 'http://user:pass@localhost',
                'http://@localhost', 'http://localhost?x=y', 'http://localhost?', 'http://localhost#',
                'http://localhost:0', 'http://localhost:65536', 'http://localhost:', 'http://localhost:abc',
                'http://localhost/../escape', 'http://localhost/%2e%2e', 'http://localhost//escape',
                'http://localhost\\evil', 'http://localhost\n', 'http://bad host', 'http://[invalid]']
        with patch.object(receiver, 'read_secret', side_effect=AssertionError('secret read')):
            for url in urls:
                with self.subTest(url=url):
                    self.assert_unavailable(self.observe_at(url), 'invalid_caco_config')

    def test_proxy_environment_does_not_receive_credentials(self):
        with peer([]) as (proxy, proxy_calls, _), peer([{'json': envelope([])}]) as (url, calls, _):
            with patch.dict(os.environ, {'HTTP_PROXY': proxy, 'HTTPS_PROXY': proxy, 'ALL_PROXY': proxy,
                                         'http_proxy': proxy, 'https_proxy': proxy, 'all_proxy': proxy, 'NO_PROXY': '', 'no_proxy': ''}):
                self.assertEqual(self.observe_at(url)['status'], 'not_observed_in_window')
            self.assertEqual(len(calls), 1)
            self.assertEqual(proxy_calls, [])

    def test_https_uses_verified_default_context_and_original_hostname(self):
        context = ssl.create_default_context()
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)
        # Test the adapter's use of the context without a live TLS daemon or
        # installing a private CA. Plain HTTP still crosses a real local socket.
        with peer([{'json': envelope([])}]) as (url, _, _), patch.object(replies.ssl, 'create_default_context') as create:
            create.return_value.wrap_socket.side_effect = lambda sock, **kwargs: sock
            result = self.observe_at(url.replace('http:', 'https:'))
            self.assertEqual(result['status'], 'not_observed_in_window')
            create.assert_called_once_with()
            self.assertEqual(create.return_value.wrap_socket.call_args.kwargs, {'server_hostname': '127.0.0.1'})

    @unittest.skipUnless(shutil.which('openssl'), 'openssl needed for disposable local TLS certificate')
    def test_real_https_verifies_certificate_and_hostname_before_authorization(self):
        certificate, key = self.root / 'fixture.crt', self.root / 'fixture.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-subj', '/CN=triage-reply-fixture', '-addext', 'subjectAltName=IP:127.0.0.1',
                        '-keyout', str(key), '-out', str(certificate)], check=True, timeout=10,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certificate, key)
        client_context = ssl.create_default_context(cafile=str(certificate))
        self.assertTrue(client_context.check_hostname)
        self.assertEqual(client_context.verify_mode, ssl.CERT_REQUIRED)
        with peer([{'json': envelope([message()])}], tls_context=server_context) as (url, calls, _):
            url = url.replace('http:', 'https:')
            self.assert_unavailable(self.observe_at(url), 'transport_unavailable')
            self.assertEqual(calls, [])  # No bearer header before authenticated TLS.
            with patch.object(replies.ssl, 'create_default_context', return_value=client_context):
                self.assert_unavailable(self.observe_at(url.replace('127.0.0.1', 'localhost')), 'transport_unavailable')
                self.assertEqual(calls, [])
                result = self.observe_at(url)
            self.assertEqual(result['status'], 'correlated_reply')
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['headers']['Authorization'], 'Bearer ' + TOKEN)

    def test_token_rotation_managed_multihop_symlinks_and_per_page_reread(self):
        link = self.root / 'managed'
        inner = self.root / 'indirect'
        inner.symlink_to(self.token.name)
        link.symlink_to(inner.name)
        self.config['commands']['caco']['token_file'] = str(link)

        def rotate(_):
            rotated = self.root / 'rotated'
            rotated.write_text('fixture-rotated-token')
            rotated.chmod(0o600)
            os.replace(rotated, self.token)
            return {'json': envelope([message(n) for n in range(20, 40)])}

        with peer([rotate, {'json': envelope([])}, {'json': envelope([])}]) as (url, calls, _):
            self.assertEqual(self.observe_at(url)['status'], 'correlated_reply')
            self.assertEqual(self.observe_at(url)['status'], 'not_observed_in_window')
        self.assertEqual([call['headers']['Authorization'] for call in calls],
                         ['Bearer ' + TOKEN, 'Bearer fixture-rotated-token', 'Bearer fixture-rotated-token'])
        self.assertTrue(link.is_symlink())
        self.assertTrue(inner.is_symlink())

    def test_unsafe_missing_cyclic_nonregular_and_header_injection_tokens(self):
        fifo = self.root / 'fifo'; os.mkfifo(fifo)
        cycle = self.root / 'cycle'; cycle.symlink_to(cycle.name)
        bad = self.root / 'bad'; bad.write_text('secret\r\nX-Evil: injected'); bad.chmod(0o600)
        empty = self.root / 'empty'; empty.touch(mode=0o600)
        long = self.root / 'long'; long.write_text('x' * 16385); long.chmod(0o600)
        self.token.chmod(0o644)
        with peer([]) as (url, calls, _):
            for path in (self.token, fifo, cycle, bad, empty, long, self.root, self.root / 'missing'):
                self.config['commands']['caco']['token_file'] = str(path)
                self.assert_unavailable(self.observe_at(url), 'credential_unavailable')
            self.assertEqual(calls, [])

    def test_response_budget_including_headers_and_unknown_length(self):
        for spec in [{'body': b'x' * (replies.MAX_RESPONSE_BYTES + 1)},
                     {'body': b'x' * (replies.MAX_RESPONSE_BYTES + 1), 'length': False},
                     {'headers': {'X-Large-' + str(n): 'x' * 60000 for n in range(5)}}]:
            with peer([spec]) as (url, _, _):
                result = self.observe_at(url)
            self.assert_unavailable(result, 'response_byte_limit')
            self.assertLessEqual(result['window']['response_bytes'], replies.MAX_RESPONSE_BYTES)

    def test_aggregate_budget_across_pages_not_per_page(self):
        pages = []
        for start in (40, 20, 0):
            page = envelope([message(n, body='x' * 4096, body_len=4096) for n in range(start, start + 20)])
            pages.append({'json': page})
        with peer(pages) as (url, calls, _):
            result = self.observe_at(url)
        self.assert_unavailable(result, 'response_byte_limit')
        self.assertEqual(len(calls), 3)
        self.assertEqual(result['window']['pages'], 2)
        self.assertLessEqual(result['window']['response_bytes'], replies.MAX_RESPONSE_BYTES)

    def test_encoded_incomplete_and_malformed_http_responses(self):
        specs = [{'headers': {'Content-Encoding': 'gzip'}}, {'headers': {'Content-Type': 'text/html'}},
                 {'headers': {'Content-Length': '9999'}}, {'headers': {'Content-Length': '-1'}},
                 {'headers': {'Transfer-Encoding': 'chunked'}}, {'headers': {'Transfer-Encoding': 'gzip'}, 'length': False}]
        for spec in specs:
            with peer([spec]) as (url, _, _):
                self.assert_unavailable(self.observe_at(url))

    def test_chunked_response_success_and_limit(self):
        body = json.dumps(envelope([message()])).encode()
        encoded = b'%x\r\n' % len(body) + body + b'\r\n0\r\n\r\n'
        with peer([{'body': encoded, 'length': False, 'headers': {'Transfer-Encoding': 'chunked'}}]) as (url, _, _):
            self.assertEqual(self.observe_at(url)['status'], 'correlated_reply')
        with peer([{'body': b'80000\r\n' + b'x' * (replies.MAX_RESPONSE_BYTES + 1),
                    'length': False, 'headers': {'Transfer-Encoding': 'chunked'}}]) as (url, _, _):
            self.assert_unavailable(self.observe_at(url), 'response_byte_limit')

    def test_header_and_body_trickles_obey_absolute_deadline(self):
        for header in (True, False):
            def trickle(handler, header=header):
                if not header:
                    handler.wfile.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n')
                for byte in (b'HTTP/1.1 200 OK\r\nX-Slow: ' if header else b'{"ok":true,'):
                    handler.wfile.write(bytes([byte]))
                    time.sleep(.04)

            with peer([trickle]) as (url, calls, _):
                start = time.monotonic()
                self.assert_unavailable(self.observe_at(url, timeout_secs=.15), 'deadline_exceeded')
                self.assertLess(time.monotonic() - start, .6)
                self.assertEqual(len(calls), 1)

    def test_deadline_is_aggregate_across_pages(self):
        pages = [{'delay': .09, 'json': envelope([message(n) for n in range(start, start + 20)])} for start in (40, 20, 0)]
        with peer(pages) as (url, calls, _):
            start = time.monotonic()
            result = self.observe_at(url, timeout_secs=.22)
            elapsed = time.monotonic() - start
        self.assert_unavailable(result, 'deadline_exceeded')
        self.assertEqual(len(calls), 3)
        self.assertEqual(result['window']['pages'], 2)
        self.assertLess(elapsed, .6)

    def test_slow_dns_deadline_cannot_later_send_http(self):
        released, finished = threading.Event(), threading.Event()
        real_resolve = socket.getaddrinfo
        with peer([]) as (url, calls, _):
            port = urlsplit(url).port

            def delayed(*args, **kwargs):
                try:
                    released.wait(1)
                    return real_resolve('127.0.0.1', port, type=socket.SOCK_STREAM)
                finally:
                    finished.set()

            try:
                with patch.object(replies.socket, 'getaddrinfo', side_effect=delayed):
                    start = time.monotonic()
                    result = self.observe_at(f'http://fixture.invalid:{port}', timeout_secs=.08)
                    self.assertLess(time.monotonic() - start, .4)
                    self.assert_unavailable(result, 'deadline_exceeded')
                    self.assertEqual(result['window']['requests'], 0)
            finally:
                released.set()
                self.assertTrue(finished.wait(1))
            self.assertEqual(calls, [])

    def test_timed_out_dns_cannot_accumulate_unbounded_resolver_threads(self):
        release = threading.Event()
        calls = []
        def stalled(*args, **kwargs):
            calls.append(args)
            release.wait(2)
            return []
        slots = threading.BoundedSemaphore(2)
        try:
            with patch.object(replies, '_DNS_SLOTS', slots), patch.object(replies.socket, 'getaddrinfo', side_effect=stalled):
                for _ in range(2):
                    with self.assertRaisesRegex(replies._Unavailable, 'deadline_exceeded'):
                        replies._addresses('fixture.invalid', 80, time.monotonic() + .03)
                for _ in range(8):
                    with self.assertRaisesRegex(replies._Unavailable, 'resolver_capacity'):
                        replies._addresses('fixture.invalid', 80, time.monotonic() + .03)
                self.assertEqual(len(calls), 2)
        finally:
            release.set()
        self.assertTrue(slots.acquire(timeout=1))
        self.assertTrue(slots.acquire(timeout=1))
        slots.release(); slots.release()

    def test_reply_list_is_bounded_separately_from_observed_window(self):
        rows = [message(i) for i in range(1, 13)]
        with peer([{'json': envelope(rows)}]) as (url, calls, _):
            result = self.observe_at(url)
        self.assertEqual(result['window']['matched_count'], 12)
        self.assertEqual(result['window']['returned_count'], 8)
        self.assertTrue(result['window']['reply_list_truncated'])
        self.assertEqual([r['message_id'] for r in result['replies']], ['msg-reply-'+str(i) for i in range(5, 13)])

    def test_valid_message_cannot_reflect_the_private_bearer(self):
        row = message(body=TOKEN, body_len=len(TOKEN))
        encoded = json.dumps(envelope([row])).replace(TOKEN, ''.join('\\u%04x' % ord(c) for c in TOKEN)).encode()
        with peer([{'body': encoded}]) as (url, calls, _):
            result = self.observe_at(url)
        self.assert_unavailable(result, 'credential_reflected')
        self.assertEqual(len(calls), 1)

    def test_changed_caco_authority_is_not_queried_with_current_credentials(self):
        frozen = copy.deepcopy(self.config)
        frozen['commands']['caco']['daemon_url'] = 'http://old.example'
        current = copy.deepcopy(frozen)
        current['commands']['caco']['daemon_url'] = 'http://new.example'
        with patch.object(receiver, 'read_secret', side_effect=AssertionError('credential read')):
            result = replies.observe(receipt(), frozen, current_config=current)
        self.assert_unavailable(result, 'caco_authority_changed')

    def test_timeout_cannot_be_increased_or_invalid(self):
        for timeout in (0, -1, 6, float('nan'), float('inf'), '5', True):
            with patch.object(receiver, 'read_secret', side_effect=AssertionError('secret read')):
                self.assert_unavailable(replies.observe(receipt(), self.config, timeout_secs=timeout), 'invalid_deadline')

    def test_real_archive_receipt_snapshot_and_action_unchanged(self):
        # Existing fixture uses deterministic sender/planner/voice, never CLI,
        # production data, model endpoints, or playback.
        import test_command_pipeline as fixtures
        fixture = fixtures.PipelineTest('runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        accepted = fixture.text('Hello fixture', action='agent-msg')
        fixture.drain()
        source = fixture.archive.receipt(accepted['id'])
        original = copy.deepcopy(source)
        calls_before = copy.deepcopy(fixture.sender.calls)
        action_before = fixture.archive.action(source['id'])
        matching = message(sender=source['action']['target'], project=source['action']['project'],
                           reply_to=source['action']['message_id'])
        with peer([{'json': envelope([matching])}]) as (url, _, _):
            result = self.observe_at(url, source)
        self.assertEqual(result['status'], 'correlated_reply')
        self.assertEqual(source, original)
        self.assertEqual(fixture.archive.receipt(source['id']), original)
        self.assertEqual(fixture.archive.action(source['id']), action_before)
        self.assertEqual(fixture.sender.calls, calls_before)


if __name__ == '__main__':
    unittest.main()
