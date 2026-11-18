"""Explicit, bounded, read-only Caco reply observation. See reply-contract.md.

Only archive.receipt snapshots are accepted; this module never opens the archive
or promotes agent assertions into action completion/operator authority.
"""
from __future__ import annotations

import http.client
import io
import ipaddress
import json
import math
import queue
import re
import socket
import ssl
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit

PAGE_LIMIT = 20
BODY_PREVIEW_BYTES = 4096
MAX_PAGES = 3
MAX_RETURNED_REPLIES = 8
MAX_RESPONSE_BYTES = 256 * 1024
MAX_DEADLINE_SECS = 5.0
# getaddrinfo cannot be cancelled. Timed-out calls keep their slots until the
# resolver exits, preventing repeated observations from creating unbounded threads.
_DNS_SLOTS = threading.BoundedSemaphore(4)
_IDENTIFIER = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,199}\Z')
_MESSAGE_ID = re.compile(r'msg-[A-Za-z0-9_-]{1,196}\Z')
_RECEIPT_ID = re.compile(r'[a-f0-9]{32}\Z')
_TIMESTAMP = re.compile(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)\Z')


class _Unavailable(Exception):
    """Only fixed, local reason codes cross the public boundary."""


def _matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _text(value, limit, *, empty=False):
    return (isinstance(value, str) and (empty or bool(value))
            and len(value.encode('utf-8')) <= limit
            and not any(ord(char) < 32 or ord(char) == 127 for char in value))


def _identity(receipt):
    """Return (identity, status, reason) before touching configuration/secrets."""
    if not isinstance(receipt, dict):
        return None, 'unavailable', 'invalid_receipt'
    action = receipt.get('action')
    if receipt.get('dry_run') is True or receipt.get('phase') == 'dry_run':
        return None, 'not_applicable', 'dry_run'
    if action is None:
        return None, 'not_applicable', 'no_action'
    if not isinstance(action, dict):
        return None, 'unavailable', 'invalid_receipt'
    if action.get('action') not in ('pebble', 'agent-msg'):
        return None, 'not_applicable', 'unsupported_action'
    if action.get('dry_run') is True or action.get('executed') is False:
        return None, 'not_applicable', 'not_executed'
    if action.get('state') != 'stored':
        return None, 'not_applicable', 'action_not_stored'
    if action.get('scope', 'project') != 'project':
        return None, 'not_applicable', 'not_single_recipient'
    if (receipt.get('dry_run') is not False
            or not _matches(_RECEIPT_ID, receipt.get('id'))
            or ('dry_run' in action and action['dry_run'] is not False)):
        return None, 'unavailable', 'invalid_receipt'
    if not _matches(_MESSAGE_ID, action.get('message_id')):
        return None, 'unavailable', 'missing_or_invalid_message_id'
    project, target = action.get('project'), action.get('target')
    parts = target.split(':') if isinstance(target, str) else []
    if (not _matches(_IDENTIFIER, project) or len(parts) != 3
            or not all(_matches(_IDENTIFIER, part) for part in parts) or parts[1] != project):
        return None, 'unavailable', 'missing_or_invalid_recipient'
    identity = {'id': receipt['id'], **{key: action[key] for key in
                ('action', 'state', 'message_id', 'project', 'target')}}
    # Optional request ID is a reference only, never a source of message identity.
    if 'request_id' in action:
        if not _text(action['request_id'], 256):
            return None, 'unavailable', 'invalid_receipt'
        identity['request_id'] = action['request_id']
    return identity, None, None


def _base(config):
    try:
        caco = config['commands']['caco']
        value, token_file = caco['daemon_url'], caco['token_file']
        if (not _text(value, 4096) or any(char.isspace() for char in value)
                or not _text(token_file, 4096)):
            raise ValueError
        base = urlsplit(value)
        # Checking delimiters also rejects empty userinfo/query/fragment, which
        # urlsplit otherwise normalizes away. Never repair a malformed base.
        if (base.scheme not in ('http', 'https') or not base.hostname
                or any(char in value for char in ('?', '#', '\\')) or '@' in base.netloc
                or '%' in base.netloc or base.netloc.endswith(':')):
            raise ValueError
        port = base.port or (443 if base.scheme == 'https' else 80)
        if not 1 <= port <= 65535 or base.port == 0:
            raise ValueError
        host = base.hostname
        try:
            ipaddress.ip_address(host)
        except ValueError:
            host = host.encode('idna').decode('ascii')
            if (len(host) > 253 or not all(re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?', part)
                                         for part in host.rstrip('.').split('.'))):
                raise ValueError
        prefix = base.path.rstrip('/')
        if (prefix and (not prefix.startswith('/') or '//' in prefix
                or any(not _matches(_IDENTIFIER, part) for part in prefix[1:].split('/')))):
            raise ValueError
        return base.scheme, host, port, prefix, token_file
    except (KeyError, TypeError, ValueError, UnicodeError):
        raise _Unavailable('invalid_caco_config') from None


def _remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise _Unavailable('deadline_exceeded')
    return seconds


def _addresses(host, port, deadline):
    # Numeric hosts require no resolver thread. Platform getaddrinfo cannot be
    # cancelled; isolate only that operation, never credentials or HTTP work.
    try:
        ipaddress.ip_address(host)
    except ValueError:
        slots = _DNS_SLOTS
        if not slots.acquire(blocking=False):
            raise _Unavailable('resolver_capacity')
        result = queue.Queue(maxsize=1)

        def resolve():
            try:
                result.put(socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)[:8])
            except OSError:
                result.put(None)
            finally:
                slots.release()

        try:
            threading.Thread(target=resolve, name='triage-reply-dns', daemon=True).start()
        except Exception:
            slots.release()
            raise _Unavailable('resolver_unavailable') from None
        try:
            addresses = result.get(timeout=_remaining(deadline))
        except queue.Empty:
            raise _Unavailable('deadline_exceeded') from None
        if addresses is None:
            raise _Unavailable('transport_unavailable')
        return addresses[:8]
    return socket.getaddrinfo(host, port, type=socket.SOCK_STREAM, flags=socket.AI_NUMERICHOST)


class _DeadlineReader(io.RawIOBase):
    """Recompute the deadline at *every* socket read, including header trickles."""
    def __init__(self, raw, sock, deadline, window):
        self.raw, self.sock, self.deadline, self.window = raw, sock, deadline, window

    def readable(self):
        return True

    def readinto(self, buffer):
        self.sock.settimeout(_remaining(self.deadline))
        room = MAX_RESPONSE_BYTES - self.window['response_bytes']
        if room <= 0:
            raise _Unavailable('response_byte_limit')
        count = self.raw.readinto(memoryview(buffer)[:min(len(buffer), room)])
        self.window['response_bytes'] += count
        return count

    def close(self):
        try:
            self.raw.close()
        finally:
            super().close()


class _DeadlineSocket:
    def __init__(self, sock, deadline, window):
        self.sock, self.deadline, self.window = sock, deadline, window

    def makefile(self, mode):
        return io.BufferedReader(_DeadlineReader(self.sock.makefile(mode, buffering=0), self.sock, self.deadline, self.window))

    def sendall(self, data):
        self.sock.settimeout(_remaining(self.deadline))
        self.sock.sendall(data)

    def close(self):
        self.sock.close()


def _connect(scheme, host, port, deadline, window):
    context = ssl.create_default_context() if scheme == 'https' else None
    for family, kind, protocol, _, address in _addresses(host, port, deadline):
        sock = socket.socket(family, kind, protocol)
        try:
            sock.settimeout(_remaining(deadline))
            sock.connect(address)
            if context:
                sock.settimeout(_remaining(deadline))
                sock = context.wrap_socket(sock, server_hostname=host)
            return _DeadlineSocket(sock, deadline, window)
        except OSError:
            sock.close()
        except BaseException:
            sock.close()
            raise
    _remaining(deadline)
    raise _Unavailable('transport_unavailable')


def _page(base, project, before, deadline, window, secrets):
    scheme, host, port, prefix, token_file = base
    # Import lazily: receiver/intake/API imports may eventually import us.
    from receiver import Failure, read_secret
    try:
        token = read_secret(token_file)
        if not re.fullmatch(rb'[A-Za-z0-9._~+/-]+=*', token):
            raise ValueError
    except (Failure, OSError, ValueError, TypeError):
        raise _Unavailable('credential_unavailable') from None
    secrets.add(token.decode('ascii'))  # Private, bounded to this observation; never returned.
    query = {'limit': PAGE_LIMIT, 'body_preview': BODY_PREVIEW_BYTES}
    if before is not None:
        query['before'] = before
    path = prefix + '/api/v1/projects/' + quote(project, safe='') + '/messages/chat?' + urlencode(query)
    connection = http.client.HTTPConnection(host, port)
    try:
        connection.sock = _connect(scheme, host, port, deadline, window)
        start_bytes = window['response_bytes']
        window['requests'] += 1
        connection.request('GET', path, headers={'Authorization': 'Bearer ' + token.decode('ascii'),
                            'Accept': 'application/json', 'Accept-Encoding': 'identity', 'Connection': 'close'})
        del token
        with connection.getresponse() as response:
            if response.status != 200:
                raise _Unavailable('redirect_refused' if 300 <= response.status < 400 else 'http_unavailable')
            if response.getheader('Content-Encoding', 'identity').lower() != 'identity':
                raise _Unavailable('encoded_response_refused')
            if response.getheader('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
                raise _Unavailable('invalid_response')
            length = response.getheader('Content-Length')
            transfer = response.getheader('Transfer-Encoding')
            if transfer is not None and (transfer.lower() != 'chunked' or length is not None):
                raise _Unavailable('invalid_response')
            if length is not None:
                if not re.fullmatch(r'[0-9]{1,12}', length):
                    raise _Unavailable('invalid_response')
                if int(length) > MAX_RESPONSE_BYTES - start_bytes:
                    raise _Unavailable('response_byte_limit')
            data = bytearray()
            while True:
                _remaining(deadline)
                # The raw reader caps aggregate bytes, including HTTP headers
                # and chunk framing, even when Content-Length is absent/false.
                chunk = response.read1(8192)
                if not chunk:
                    break
                data.extend(chunk)
            if length is not None and len(data) != int(length):
                raise _Unavailable('invalid_response')
            _remaining(deadline)
            return data
    finally:
        connection.close()


def _timestamp(value):
    if not isinstance(value, str):
        raise _Unavailable('invalid_response')
    match = _TIMESTAMP.fullmatch(value)
    if not match or (match[3] != 'Z' and (int(match[3][1:3]) > 23 or int(match[3][4:6]) > 59)):
        raise _Unavailable('invalid_response')
    try:
        dt = datetime.fromisoformat(match[1] + match[3].replace('Z', '+00:00'))
        delta = dt - datetime(1970, 1, 1, tzinfo=timezone.utc)
        return (delta.days * 86400 + delta.seconds) * 10**9 + int((match[2] or '').ljust(9, '0'))
    except (ValueError, OverflowError):
        raise _Unavailable('invalid_response') from None


def _message(value):
    if not isinstance(value, dict):
        raise _Unavailable('invalid_response')
    if (not _matches(_MESSAGE_ID, value.get('id'))
            or not _text(value.get('project'), 200, empty=True)
            or not _text(value.get('scope'), 32) or not _text(value.get('sender'), 602)
            or not _text(value.get('kind'), 64) or not _text(value.get('sender_kind'), 32)
            or type(value.get('operator_authenticated')) is not bool
            or (value.get('target') is not None and not _text(value['target'], 602))
            or (value.get('reply_to') is not None and not _matches(_MESSAGE_ID, value['reply_to']))
            or (value.get('origin_principal') is not None and not _text(value['origin_principal'], 512))):
        raise _Unavailable('invalid_response')
    body, length, truncated = value.get('body'), value.get('body_len'), value.get('body_truncated', False)
    size = len(body.encode('utf-8')) if isinstance(body, str) else -1
    if (not 0 <= size <= BODY_PREVIEW_BYTES or type(length) is not int
            or not 0 <= length <= 2**63 - 1 or type(truncated) is not bool
            or (length != size if not truncated else length <= size)):
        raise _Unavailable('invalid_response')
    timestamp = _timestamp(value.get('ts'))
    message = {key: value.get(key) for key in ('project', 'scope', 'sender', 'target', 'kind', 'reply_to', 'ts')}
    message.update(message_id=value['id'], body=body, body_len=length, body_truncated=truncated,
                   provenance={'source': 'caco_project_chat', 'sender_kind': value['sender_kind'],
                               'origin_principal': value.get('origin_principal'),
                               'operator_authenticated': value['operator_authenticated'], 'content_role': 'agent_assertion'})
    return timestamp, message


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _Unavailable('invalid_response')
        result[key] = value
    return result


def _parse(data):
    def reject_constant(_):
        raise _Unavailable('invalid_response')
    value = json.loads(data.decode('utf-8'), object_pairs_hook=_unique_object, parse_constant=reject_constant)
    if not isinstance(value, dict) or value.get('ok') is not True or not isinstance(value.get('data'), dict):
        raise _Unavailable('invalid_response')
    messages, count = value['data'].get('messages'), value['data'].get('count')
    if not isinstance(messages, list) or type(count) is not int or count != len(messages) or count > PAGE_LIMIT:
        raise _Unavailable('invalid_response')
    return [_message(message) for message in messages]


def observe(receipt, config, *, timeout_secs=5.0, current_config=None):
    """Inspect one authoritative receipt; never mutate either argument or state.

    Deadline may only be lowered. Error outputs deliberately discard all remote
    strings and partial reply content. See reply-contract.md for caller duties.
    """
    window = {'exhaustive': False, 'page_limit': PAGE_LIMIT, 'max_pages': MAX_PAGES,
              'body_preview_bytes': BODY_PREVIEW_BYTES, 'max_response_bytes': MAX_RESPONSE_BYTES,
              'deadline_secs': MAX_DEADLINE_SECS, 'requests': 0, 'pages': 0, 'response_bytes': 0,
              'candidate_count': 0, 'matched_count': 0, 'returned_count': 0, 'reply_list_truncated': False,
              'max_returned_replies': MAX_RETURNED_REPLIES, 'oldest_ts': None, 'newest_ts': None,
              'before': None, 'stop_reason': None}
    result = {'version': 1, 'status': 'unavailable', 'reason': None, 'receipt': None, 'replies': [], 'window': window}

    def finish(status, reason):
        result.update(status=status, reason=reason)
        window['stop_reason'] = reason
        return result

    try:
        identity, status, reason = _identity(receipt)
        if status:
            return finish(status, reason)
        result['receipt'] = identity
        if (type(timeout_secs) not in (int, float) or not math.isfinite(timeout_secs)
                or not 0 < timeout_secs <= MAX_DEADLINE_SECS):
            return finish('unavailable', 'invalid_deadline')
        window['deadline_secs'] = timeout_secs
        deadline = time.monotonic() + timeout_secs
        base = _base(config)
        if current_config is not None:
            current = _base(current_config)
            if base[:4] != current[:4] or Path(base[4]).expanduser().absolute() != Path(current[4]).expanduser().absolute():
                return finish('unavailable', 'caco_authority_changed')
        seen, replies, before = {}, [], None
        secrets = set()
        oldest = newest = None
        stop = 'page_limit'
        for _ in range(MAX_PAGES):
            window['before'] = before
            messages = _parse(_page(base, identity['project'], before, deadline, window, secrets))
            _remaining(deadline)
            window['pages'] += 1
            repeated = False
            for timestamp, message in messages:
                encoded = json.dumps(message, ensure_ascii=False)
                if any(secret in encoded for secret in secrets):
                    raise _Unavailable('credential_reflected')
                mid = message['message_id']
                if mid in seen:
                    if seen[mid] != message:
                        raise _Unavailable('conflicting_message_identity')
                    repeated = True
                    continue
                seen[mid] = message
                window['candidate_count'] += 1
                if oldest is None or timestamp < oldest:
                    oldest, window['oldest_ts'] = timestamp, message['ts']
                if newest is None or timestamp > newest:
                    newest, window['newest_ts'] = timestamp, message['ts']
                if (message['reply_to'] == identity['message_id'] and message['sender'] == identity['target']
                        and message['project'] == identity['project'] and message['provenance']['sender_kind'] == 'agent'
                        and message['provenance']['operator_authenticated'] is False):
                    replies.append((timestamp, message))
            window['matched_count'] = len(replies)
            if not messages:
                stop = 'empty_page'
                break
            if repeated:
                stop = 'repeated_message'
                break
            earliest = min(messages, key=lambda item: (item[0], item[1]['message_id']))
            if before is not None and any(timestamp >= _timestamp(before) for timestamp, _ in messages):
                stop = 'cursor_not_advanced'
                break
            if len(messages) < PAGE_LIMIT:
                stop = 'short_page'
                break
            before = earliest[1]['ts']
        _remaining(deadline)
        ordered = sorted(replies, key=lambda item: (item[0], item[1]['message_id']))
        result['replies'] = [message for _, message in ordered[-MAX_RETURNED_REPLIES:]]
        window['returned_count'] = len(result['replies'])
        window['reply_list_truncated'] = len(ordered) > MAX_RETURNED_REPLIES
        return finish('correlated_reply' if replies else 'not_observed_in_window', stop)
    except _Unavailable as exc:
        return finish('unavailable', str(exc))
    except TimeoutError:
        return finish('unavailable', 'deadline_exceeded')
    except (OSError, http.client.HTTPException):
        return finish('unavailable', 'transport_unavailable')
    except (ValueError, TypeError, OverflowError, RecursionError):
        return finish('unavailable', 'invalid_response')
