"""Authenticated Triage native API and bounded OpenAI Chat Completions adapter."""
from __future__ import annotations
import hmac
import json
import sqlite3
import time
from urllib.parse import parse_qs, urlsplit
from triage_store import Error, digest, identifier
from change_signal import for_archive

MODELS = ('triage-route', 'triage-route-dry-run')
MAX_BODY = 1024 * 1024


def capabilities(server=None):
    workers = server.worker.stage_workers if server else 4
    return {'version': 1, 'models': list(MODELS), 'streaming': True,
            'native_turns': True, 'client_speech': True, 'receipt_first': True,
            'live_requires_identity': True, 'live_requires_user_purpose': True,
            'operations': ['submit', 'inspect', 'approve'], 'cancel': True,
            'clarification': True, 'clarification_slots': ['bead_project', 'bead_priority'],
            'reference_resolution': True, 'natural_preview_approval': True,
            'proposal_amendment': True, 'proposal_amendment_fields': ['priority'],
            'proposal_amendment_actions': ['bd-file'], 'derived_proposals': True,
            'reference_disambiguation': 'exact_server_turn_id', 'post_dispatch_amendment': False,
            'reference_candidates_max': 32, 'project_aliases': 'operator_configured', 'preview_approval': True,
            'approval_requires_revision': True, 'proposal_single_consumption': True, 'cancel_boundary': 'before_dispatch_attempt',
            'agent_reply_wait': False, 'reply_observation': True, 'reply_observation_max_seconds': 5, 'scheduling': 'bounded_stage_workers', 'stage_workers': workers,
            'server_speech_concurrency': 1, 'unknown_route_barrier': 'unfinished_only',
            'failed_before_effect_barrier': False, 'failed_input_barrier': False, 'caco_capability_checks': 'none',
            'native_sse': True, 'session_events': True, 'event_page_max_events': 256,
            'native_stream_max_seconds': 25, 'wait_slots': server.wait_limit if server else None,
            'session_wait_slots': server.session_wait_limit if server else 2,
            'max_body_bytes': MAX_BODY, 'max_text_bytes': 16384,
            'identity_scope': 'single_authenticated_operator', 'usage': 'not_available'}


def single(headers, name):
    values = headers.get_all(name, [])
    if len(values) > 1: raise Error('duplicate_' + name.lower().replace('-', '_'))
    return values[0] if values else None


def message_text(content):
    if isinstance(content, str): return content
    if isinstance(content, list) and len(content) <= 32:
        if any(not isinstance(p, dict) or p.get('type') != 'text' or not isinstance(p.get('text'), str) for p in content):
            raise Error('unsupported_content_type')
        return '\n'.join(p['text'] for p in content)
    if content is None: return ''
    raise Error('unsupported_content_type')


def parse_chat(body, headers):
    allowed = {'model', 'messages', 'stream', 'n', 'tools', 'tool_choice', 'parallel_tool_calls',
               'temperature', 'top_p', 'stream_options', 'store', 'user', 'triage', 'max_tokens', 'max_completion_tokens'}
    if not isinstance(body, dict) or set(body) - allowed: raise Error('unsupported_chat_parameter')
    model = body.get('model')
    if model not in MODELS: raise Error('model_not_found', 404)
    if type(body.get('n', 1)) is not int or body.get('n', 1) != 1: raise Error('only_one_choice_supported')
    budgets = [body[k] for k in ('max_tokens', 'max_completion_tokens') if k in body]
    if len(budgets) > 1 or any(type(n) is not int or not 1 <= n <= 8192 for n in budgets): raise Error('invalid_output_budget')
    if type(body.get('stream', False)) is not bool: raise Error('invalid_stream')
    if body.get('tool_choice', 'auto') not in ('auto', 'none'): raise Error('tool_execution_not_supported')
    for name, upper in (('temperature', 2), ('top_p', 1)):
        if name in body and (type(body[name]) not in (int, float) or not 0 <= body[name] <= upper): raise Error('invalid_sampling_parameter')
    if 'parallel_tool_calls' in body and type(body['parallel_tool_calls']) is not bool: raise Error('invalid_tool_parameter')
    if body.get('store', False) is not False: raise Error('provider_store_not_supported')
    if 'tools' in body and (not isinstance(body['tools'], list) or len(body['tools']) > 128): raise Error('invalid_tools')
    if 'stream_options' in body and body['stream_options'] not in ({}, {'include_usage': True}, {'include_usage': False}): raise Error('invalid_stream_options')
    messages = body.get('messages')
    if not isinstance(messages, list) or not 1 <= len(messages) <= 128: raise Error('invalid_messages')
    canonical, latest, index = [], None, None
    for i, message in enumerate(messages):
        if not isinstance(message, dict) or message.get('role') not in ('system', 'developer', 'user', 'assistant', 'tool'): raise Error('invalid_message')
        content = message_text(message.get('content'))
        if '\0' in content: raise Error('invalid_message')
        canonical.append({'role': message['role'], 'content': content})
        if message['role'] == 'user': latest, index = content, i
    if latest is None or not latest.strip(): raise Error('user_message_required')
    if len(latest.encode()) > 16384: raise Error('invalid_text')
    context = body.get('triage', {})
    if not isinstance(context, dict) or set(context) - {'session_id', 'turn_id', 'purpose', 'operation', 'reference_turn', 'expected_revision', 'route'}: raise Error('invalid_triage_options')
    def alias(values):
        supplied = [v for v in values if v is not None]
        if len(set(supplied)) > 1: raise Error('conflicting_identity_headers')
        return identifier(supplied[0]) if supplied else None
    session = alias([single(headers, 'X-Triage-Session-ID'), single(headers, 'X-Tri-Session-ID'),
                     single(headers, 'x-session-affinity'), single(headers, 'session_id'), context.get('session_id')])
    key = alias([single(headers, 'Idempotency-Key'), context.get('turn_id')])
    header_purpose = single(headers, 'X-Triage-Purpose')
    body_purpose = context.get('purpose')
    if header_purpose is not None and body_purpose is not None and header_purpose != body_purpose:
        raise Error('conflicting_request_purpose')
    purpose = header_purpose if header_purpose is not None else body_purpose
    if purpose not in (None, 'user', 'auxiliary'): raise Error('invalid_request_purpose')
    mode = 'live' if model == 'triage-route' else 'dry_run'
    legacy_mode = single(headers, 'X-Pebble-Dry-Run')
    if legacy_mode is not None and (legacy_mode not in ('true', 'false') or (legacy_mode == 'true') != (mode == 'dry_run')):
        raise Error('conflicting_dry_run_mode', 409)
    if mode == 'live' and purpose != 'auxiliary' and (not session or not key or purpose != 'user'):
        raise Error('live_requires_session_turn_id_and_user_purpose', 400)
    # A stateless first prompt cannot distinguish retry vs intentionally identical new session.
    # Compatibility mode deliberately reuses that identity; callers can provide a fresh session.
    session = session or 'compat-' + digest(canonical[:1])
    key = key or 'history-' + digest(canonical[:index+1])
    return {'model': model, 'stream': body.get('stream', False), 'purpose': purpose,
            'session': session, 'output_budget': budgets[0] if budgets else 8192, 'request': {'client_turn_id': key, 'text': latest, 'mode': mode,
            'operation': context.get('operation', 'submit'), 'route': context.get('route', 'route'),
            **{k: context[k] for k in ('reference_turn', 'expected_revision') if k in context}}}


def completion(model, turn, text, stream=False, finish=False):
    base = {'id': 'chatcmpl-' + turn['id'], 'object': 'chat.completion.chunk' if stream else 'chat.completion',
            'created': int(turn['created']), 'model': model}
    if stream:
        choice = {'index': 0, 'delta': {} if finish else {'content': text}, 'finish_reason': 'stop' if finish else None}
    else:
        choice = {'index': 0, 'message': {'role': 'assistant', 'content': text}, 'finish_reason': 'stop'}
    return {**base, 'choices': [choice]}


def read_body(handler):
    if handler.headers.get('Transfer-Encoding') or len(handler.headers.get_all('Content-Length', [])) != 1:
        raise Error('content_length_required', 411)
    try: length = int(handler.headers['Content-Length'])
    except ValueError: raise Error('invalid_content_length')
    if not 0 < length <= MAX_BODY: raise Error('body_too_large', 413)
    if handler.headers.get('Content-Type', '').split(';')[0].strip().lower() != 'application/json': raise Error('json_required', 415)
    raw = handler.rfile.read(length)
    if len(raw) != length: raise Error('incomplete_body')
    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result: raise Error('duplicate_json_key')
            result[key] = value
        return result
    try: return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(Error('invalid_json_number')))
    except (ValueError, UnicodeError, RecursionError): raise Error('malformed_json')


def still_authorized(handler):
    # Import lazily: receiver owns the credential parser and its managed symlink support.
    from receiver import read_secret
    try:
        return hmac.compare_digest(handler.headers['Authorization'].encode('latin1'), b'Bearer ' + read_secret(handler.server.archive.config['token_file']))
    except Exception: return False


def event_options(handler, query):
    values = parse_qs(query, keep_blank_values=True, strict_parsing=True, max_num_fields=2)
    if set(values) - {'after', 'seconds'} or any(len(v) != 1 for v in values.values()):
        raise Error('invalid_event_query')
    def number(value, maximum):
        if not isinstance(value, str) or not value.isascii() or not value.isdigit() or len(value) > 19:
            raise Error('invalid_cursor')
        result = int(value)
        if not 0 <= result <= maximum: raise Error('invalid_cursor')
        return result
    header = single(handler.headers, 'Last-Event-ID')
    after = number(values['after'][0], 9223372036854775807) if 'after' in values else None
    last = number(header, 9223372036854775807) if header is not None else None
    if after is not None and last is not None and after != last: raise Error('conflicting_event_cursor')
    seconds = number(values.get('seconds', ['25'])[0], 25)
    if seconds == 0: raise Error('invalid_event_duration')
    accept = single(handler.headers, 'Accept') or ''
    stream = any(part.split(';')[0].strip().lower() == 'text/event-stream' for part in accept.split(','))
    return after if after is not None else last or 0, seconds, stream


def native_events(handler, scope, identity, query):
    after, seconds, streaming = event_options(handler, query)
    store, worker = handler.server.triage, handler.server.worker
    page_fn = store.events if scope == 'turn' else store.session_events
    changes = for_archive(handler.server.archive)
    epoch = changes.version()  # before the read snapshot: no missed commit-to-wait race
    page = page_fn(identity, after)
    if not streaming:
        handler.reply(200, page)
        return
    session = page['session_id']
    if not handler.server.acquire_wait(session): raise Error('event_capacity_busy', 429)
    try:
        deadline = time.monotonic() + min(seconds, max(1, handler.server.archive.config['listen']['request_timeout_secs'] - 1))
        handler.triage_streaming = True
        handler.send_response(200)
        for key, value in {'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store',
                           'Connection': 'close', 'X-Accel-Buffering': 'no', 'X-Triage-Session-ID': session}.items():
            handler.send_header(key, value)
        handler.end_headers(); handler.close_connection = True
        def write(raw):
            # A blocked consumer owns only its HTTP slot, never a database read
            # transaction/worker claim. Bound each write and the whole stream.
            if not still_authorized(handler): raise Error('unauthorized', 401)
            handler.connection.settimeout(min(1, max(.05, deadline - time.monotonic())))
            handler.wfile.write(raw)
            handler.wfile.flush()
        def emit(kind, data, cursor=None):
            prefix = ('id: ' + str(cursor) + '\n') if cursor is not None else ''
            write((prefix + 'event: ' + kind + '\ndata: ' + json.dumps(data, separators=(',', ':')) + '\n\n').encode())
        cursor, reason = after, 'deadline'
        while time.monotonic() < deadline:
            if not still_authorized(handler): return
            if page['reset']:
                # One atomic bootstrap: recent history + bounded state at this
                # scoped high-water. No cursor rewind/replay of skipped history.
                emit('snapshot', page, page['cursor'])
            else:
                for event in page['events']:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('native_stream_deadline')
                    emit('turn', event, event['seq'])
            cursor = page['cursor']
            if worker.stop.is_set():
                reason = 'worker_stopped'
                break
            if scope == 'turn' and page['done'] and not page['has_more']:
                reason = 'complete'
                break
            if not page['has_more']:
                write(b': heartbeat\n\n')
                changes.wait(epoch, min(1, max(0, deadline - time.monotonic())), worker.stop)
            if not still_authorized(handler): return
            try:
                epoch = changes.version()
                page = page_fn(identity, cursor, initial=False)
            except Error as exc:
                emit('resync', {'code': exc.code, **(exc.details or {})})
                return
        emit('close', {'cursor': cursor, 'reason': reason})
    finally:
        handler.server.release_wait(session)


def wait_chat(handler, chat, turn):
    store, worker = handler.server.triage, handler.server.worker
    changes = for_archive(handler.server.archive)
    # Conservative UTF-8-byte ceiling (never more byte-level BPE tokens).
    # Presentation truncation must not cancel or reissue an admitted command.
    remaining, truncated = chat['output_budget'], False
    def bounded(text):
        nonlocal remaining, truncated
        raw = text.encode()
        truncated = truncated or len(raw) > remaining
        result = raw[:remaining].decode('utf-8', errors='ignore')
        remaining -= len(result.encode())
        return result
    limit = 90 if chat['stream'] else 30
    deadline = time.monotonic() + min(limit, max(1, handler.server.archive.config['listen']['request_timeout_secs'] - 2))
    if chat['stream']:
        handler.send_response(200)
        for key, value in {'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-store',
                           'Connection': 'close', 'X-Accel-Buffering': 'no', 'X-Triage-Turn-ID': turn['id'],
                           'X-Triage-Session-ID': turn['session_id']}.items(): handler.send_header(key, value)
        handler.end_headers(); handler.close_connection = True
        handler.triage_streaming = True
        def send(data):
            handler.connection.settimeout(min(1, max(.05, deadline - time.monotonic())))
            handler.wfile.write(b'data: ' + (data.encode() if isinstance(data, str) else json.dumps(data).encode()) + b'\n\n')
            handler.wfile.flush()
        first = completion(chat['model'], turn, '', True)
        first['choices'][0]['delta'] = {'role': 'assistant'}
        send(first)
        cursor, displayed = 0, set()
        while True:
            if not still_authorized(handler): return
            epoch = changes.version()
            turn = store.get(turn['id'])
            for event in turn['events']:
                if event['seq'] > cursor and event['text'] and event['text'] not in displayed:
                    send(completion(chat['model'], turn, bounded(event['text'] + '\n'), True))
                    displayed.add(event['text'])
                cursor = max(cursor, event['seq'])
            if turn['done'] or time.monotonic() >= deadline or worker.stop.is_set() or remaining == 0 or truncated: break
            handler.wfile.write(b': heartbeat\n\n'); handler.wfile.flush()
            changes.wait(epoch, min(1, max(0, deadline - time.monotonic())), worker.stop)
        text = store.response_text(turn) if turn['operation'] == 'inspect' else f"Turn {turn['id']}: " + (turn['phase'] if turn['done'] else 'pending; not complete') + '.'
        send(completion(chat['model'], turn, bounded(text), True))
        final = completion(chat['model'], turn, '', True, True)
        if truncated: final['choices'][0]['finish_reason'] = 'length'
        send(final); send('[DONE]')
    else:
        while not turn['done'] and time.monotonic() < deadline and not worker.stop.is_set():
            if not still_authorized(handler): raise Error('unauthorized', 401)
            epoch = changes.version()
            turn = store.get(turn['id'])
            if not turn['done']:
                changes.wait(epoch, min(1, max(0, deadline - time.monotonic())), worker.stop)
        result = completion(chat['model'], turn, bounded(store.response_text(turn)))
        if truncated: result['choices'][0]['finish_reason'] = 'length'
        handler.reply(200, result)


def handle(handler, method):
    """Returns true for owned routes. Caller authenticates before entering."""
    parsed = urlsplit(handler.path)
    path = parsed.path.rstrip('/')
    if path != '/route' and not path.startswith('/route/'): return False
    try:
        if handler.server.triage is None: raise Error('command_pipeline_required', 404)
        store = handler.server.triage
        if method == 'GET' and path == '/route/capabilities':
            if parsed.query: raise Error('unsupported_query')
            handler.reply(200, capabilities(handler.server))
        elif method == 'GET' and path == '/route/v1/models':
            if parsed.query: raise Error('unsupported_query')
            handler.reply(200, {'object': 'list', 'data': [{'id': m, 'object': 'model', 'created': 0, 'owned_by': 'triage'} for m in MODELS]})
        elif method == 'GET' and path == '/route/sessions':
            if parsed.query: raise Error('unsupported_query')
            handler.reply(200, store.sessions())
        elif method == 'GET' and path.startswith('/route/sessions/'):
            parts = path.split('/')
            if len(parts) == 5 and parts[4] == 'events':
                native_events(handler, 'session', parts[3], parsed.query)
            elif len(parts) == 4 and not parsed.query:
                handler.reply(200, store.session_turns(parts[3]))
            else: raise Error('not_found', 404)
        elif method == 'GET' and path.startswith('/route/turns/'):
            parts = path.split('/')
            if len(parts) == 4 and not parsed.query: handler.reply(200, store.get(parts[3]))
            elif len(parts) == 5 and parts[4] == 'events':
                native_events(handler, 'turn', parts[3], parsed.query)
            elif len(parts) == 5 and parts[4] == 'replies' and not parsed.query:
                turn = store.get(parts[3])
                if not handler.server.acquire_wait(turn['session_id']): raise Error('reply_observation_capacity_busy', 429)
                try:
                    observation = store.replies(parts[3])
                    if not still_authorized(handler): raise Error('unauthorized', 401)
                    handler.reply(200, observation)
                finally:
                    handler.server.release_wait(turn['session_id'])
            else: raise Error('not_found', 404)
        elif method == 'POST':
            if parsed.query: raise Error('unsupported_query')
            body = read_body(handler)
            if path == '/route/sessions':
                if not isinstance(body, dict) or set(body) != {'id'}: raise Error('invalid_session')
                handler.reply(200, store.session(body['id']))
            elif path.startswith('/route/sessions/') and path.endswith('/turns') and len(path.split('/')) == 5:
                if handler.server.worker.fatal: raise Error('worker_unavailable', 503)
                result = store.submit(path.split('/')[3], body)
                handler.server.worker.wake.set(); handler.reply(202, result)
            elif path.startswith('/route/turns/') and path.endswith('/cancel') and len(path.split('/')) == 5:
                if not isinstance(body, dict) or body: raise Error('invalid_cancel_request')
                # Cancellation remains available even when the worker is down.
                handler.reply(200, store.cancel(path.split('/')[3]))
            elif path == '/route/v1/chat/completions':
                chat = parse_chat(body, handler.headers)
                if chat['purpose'] == 'auxiliary':
                    dummy = {'id': 'aux-' + digest(body), 'created': time.time()}
                    # Auxiliary requests never enter sessions/turns or route user/history text.
                    text = 'Triage handles explicit routed commands. No command was submitted.'
                    truncated = len(text.encode()) > chat['output_budget']
                    text = text.encode()[:chat['output_budget']].decode('utf-8', errors='ignore')
                    if chat['stream']:
                        handler.send_response(200)
                        handler.send_header('Content-Type', 'text/event-stream; charset=utf-8')
                        handler.send_header('Cache-Control', 'no-store')
                        handler.send_header('Connection', 'close')
                        handler.end_headers(); handler.close_connection = True; handler.triage_streaming = True
                        final = completion(chat['model'], dummy, '', True, True)
                        if truncated: final['choices'][0]['finish_reason'] = 'length'
                        for data in (completion(chat['model'], dummy, text, True), final):
                            handler.wfile.write(b'data: ' + json.dumps(data).encode() + b'\n\n')
                        handler.wfile.write(b'data: [DONE]\n\n'); handler.wfile.flush()
                    else:
                        result = completion(chat['model'], dummy, text)
                        if truncated: result['choices'][0]['finish_reason'] = 'length'
                        handler.reply(200, result)
                else:
                    if handler.server.worker.fatal: raise Error('worker_unavailable', 503)
                    if not handler.server.acquire_wait(chat['session']): raise Error('completion_capacity_busy', 429)
                    try:
                        store.session(chat['session'])
                        turn = store.submit(chat['session'], chat['request'])
                        handler.server.worker.wake.set()
                        wait_chat(handler, chat, turn)
                    finally: handler.server.release_wait(chat['session'])
            else: raise Error('not_found', 404)
        else: raise Error('not_found', 404)
    except Error as exc:
        if getattr(handler, 'triage_streaming', False): return True
        handler.reply(exc.status, {'error': {'type': 'invalid_request_error' if exc.status < 500 else 'server_error', 'code': exc.code, 'message': exc.code},
                                   **({'details': exc.details} if exc.details is not None else {})})
    except (ValueError, TypeError, KeyError):
        if not getattr(handler, 'triage_streaming', False):
            handler.reply(400, {'error': {'type': 'invalid_request_error', 'code': 'invalid_request', 'message': 'Invalid request.'}})
    except (OSError, sqlite3.Error):
        if not getattr(handler, 'triage_streaming', False):
            handler.reply(503, {'error': {'type': 'server_error', 'code': 'storage_or_transport_unavailable', 'message': 'Acceptance may be unknown. Reuse the same turn identity; do not submit a new turn.'}})
    return True
