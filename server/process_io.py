"""Bounded subprocess glue; payloads go through stdin, never shell interpolation."""
from __future__ import annotations

import contextlib
import os
import selectors
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass


class ProcessError(Exception):
    def __init__(self, code: str, exit_code: int | None = None):
        super().__init__(code)
        self.code, self.exit_code = code, exit_code


@dataclass(frozen=True)
class Result:
    stdout: bytes
    elapsed_ms: float


def run(argv: list[str], payload: bytes = b'', *, timeout: float = 20,
        max_output: int = 262144, env: dict | None = None) -> Result:
    if not argv or any(not isinstance(arg, str) or '\0' in arg for arg in argv):
        raise ProcessError('invalid_command')
    if not 0 < timeout <= 600 or not 0 <= max_output <= 4 * 1024 * 1024:
        raise ProcessError('invalid_process_bound')
    started = time.monotonic()
    with tempfile.TemporaryFile() as source:
        source.write(payload)
        source.seek(0)
        try:
            proc = subprocess.Popen(argv, stdin=source, stdout=subprocess.PIPE,
                                    stderr=subprocess.DEVNULL, env=env, start_new_session=True)
        except OSError as exc:
            raise ProcessError('command_unavailable') from exc
        chunks, count, settled = [], 0, False
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(proc.stdout, selectors.EVENT_READ)
                while selector.get_map():
                    remaining = timeout - (time.monotonic() - started)
                    if remaining <= 0:
                        raise ProcessError('command_timeout')
                    for key, _ in selector.select(min(remaining, 0.25)):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:
                            selector.unregister(key.fileobj)
                            continue
                        count += len(data)
                        if count > max_output:
                            raise ProcessError('command_output_limit')
                        chunks.append(data)
            try:
                code = proc.wait(timeout=max(.01, timeout - (time.monotonic() - started)))
            except subprocess.TimeoutExpired as exc:
                raise ProcessError('command_timeout') from exc
            settled = True
            if code:
                raise ProcessError('command_failed', code)
            return Result(b''.join(chunks), round((time.monotonic() - started) * 1000, 2))
        finally:
            if not settled:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
            proc.stdout.close()
