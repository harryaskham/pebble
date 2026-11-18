// Bounded one-shot transport bridge for the optional Agent Utils MAI adapter and webhook forwarding.
// Config/audio/payload FILE PATHS only on argv; credentials never leave headers.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const MAX_RESPONSE = 1024 * 1024;
const expand = p => p.startsWith('~/') ? path.join(os.homedir(), p.slice(2)) : p;
class Failure extends Error {
  constructor(code, retryable = false) { super(code); this.code = code; this.retryable = retryable; }
}
function secret(filename) {
  const fd = fs.openSync(expand(filename), fs.constants.O_RDONLY | fs.constants.O_NONBLOCK);
  try {
    const st = fs.fstatSync(fd);
    if (!st.isFile() || st.uid !== process.getuid() || (st.mode & 0o077) || st.size > 16384) throw new Failure('unsafe_credential_file');
    const buffer = Buffer.alloc(16385);
    const n = fs.readSync(fd, buffer, 0, buffer.length, 0);
    const value = buffer.subarray(0, n).toString('utf8').trim();
    if (!value || n > 16384) throw new Failure('invalid_credential_file');
    return value;
  } finally { fs.closeSync(fd); }
}
function safeUrl(value, mai = false) {
  const url = new URL(value);
  if (url.username || url.password || url.hash || url.search) throw new Failure('invalid_endpoint');
  if (mai && url.protocol !== 'https:' && !(url.protocol === 'http:' && url.hostname === '127.0.0.1')) throw new Failure('mai_requires_https');
  if (!['http:', 'https:'].includes(url.protocol)) throw new Failure('invalid_endpoint');
  return url.toString().replace(/\/$/, '');
}

export async function bridge(operation, config, inputPath) {
  let responseValue = null, status = null, timedOut = false;
  const secrets = [];
  const started = Date.now();
  const controller = new AbortController();
  const timeout = 1000 * (operation === 'transcribe' ? config.transcription.timeout_secs : config.forward.timeout_secs);
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeout);
  // Hold the upstream timeout open through the entire bounded response body.
  const boundedFetch = async (url, init) => {
    // The shared Entra helper owns token acquisition. Retain its outgoing
    // credential only in memory for redacting a provider that echoes headers.
    const headers = new Headers(init.headers);
    const authorization = headers.get('authorization');
    if (authorization) {
      secrets.push(authorization, authorization.replace(/^Bearer\s+/i, '').split('#').at(-1));
    }
    const signal = init.signal ? AbortSignal.any([init.signal, controller.signal]) : controller.signal;
    const response = await fetch(url, { ...init, signal, redirect: 'error' });
    status = response.status;
    const reader = response.body?.getReader();
    let length = 0;
    const chunks = [];
    if (reader) {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          length += value.byteLength;
          if (length > MAX_RESPONSE) { await reader.cancel(); throw new Failure('response_too_large'); }
          chunks.push(Buffer.from(value));
        }
      } finally { reader.releaseLock(); }
    }
    let body = Buffer.concat(chunks).toString('utf8');
    for (const value of secrets) body = body.replaceAll(value, '[redacted]');
    try { responseValue = JSON.parse(body); } catch { responseValue = { non_json: body.slice(0, 4096) }; }
    // Preserve the adapter's real parsing/error behavior, with secrets redacted.
    return new Response(body || null, { status, headers: response.headers });
  };
  try {
    if (operation === 'transcribe') {
      const endpoint = safeUrl(secret(config.transcription.endpoint_file), true);
      const auth = process.env.PI_STT_AZURE_AUTH ?? process.env.AZURE_SPEECH_AUTH ?? 'key';
      const apiKey = auth === 'entra' ? undefined : secret(config.transcription.key_file);
      if (apiKey) secrets.push(apiKey);
      const wav = fs.readFileSync(inputPath);
      if (wav.length > 48000 * (config.transcription.max_audio_secs + 1) + 4096) throw new Failure('wav_too_large');
      const modulePath = path.join(expand(config.transcription.agent_utils_dir), 'extensions/lib/realtime-stt-batch.js');
      const { transcribeMaiAudioDirect } = await import(pathToFileURL(modulePath).href);
      const text = await transcribeMaiAudioDirect({ wav, model: 'mai-transcribe-2', deployment: config.transcription.deployment,
        endpoint, apiKey, auth, language: config.transcription.language, timeoutMs: timeout,
        signal: controller.signal, fetchImpl: boundedFetch });
      if (typeof responseValue?.text !== 'string') throw new Failure('invalid_mai_response');
      if (!text?.trim() || Buffer.byteLength(text, 'utf8') > 16384) throw new Failure('empty_or_oversized_transcript');
      return { ok: true, text, model: 'mai-transcribe-2', response: responseValue, elapsed_ms: Date.now() - started };
    }
    if (operation === 'forward') {
      const url = safeUrl(config.forward.url);
      const headers = { 'Content-Type': 'application/json' };
      if (config.forward.auth !== 'none') {
        const token = secret(config.forward.auth === 'bearer-file' ? config.forward.token_file : config.token_file);
        secrets.push(token);
        headers.Authorization = `Bearer ${token}`;
      }
      const body = fs.readFileSync(inputPath);
      if (body.length > 32768) throw new Failure('forward_payload_too_large');
      const response = await boundedFetch(url, { method: 'POST', body, headers });
      if (!response.ok) throw new Failure(`forward_http_${status}`, status === 408 || status === 429 || status >= 500);
      if (config.forward.receipt === 'http-2xx') return { ok: true, response: responseValue, elapsed_ms: Date.now() - started };
      if (responseValue?.ok !== true || responseValue?.exit_code !== 0) throw new Failure('forward_receipt_invalid');
      let receipt;
      try { receipt = JSON.parse(responseValue.stdout_tail); } catch { throw new Failure('forward_receipt_invalid'); }
      if (!/^[a-f0-9]{32}$/.test(receipt?.id) || !['queued', 'delivered', 'blocked', 'indeterminate', 'skipped'].includes(receipt?.state)) throw new Failure('forward_receipt_invalid');
      return { ok: true, response: responseValue, first_capture_id: receipt.id, first_phase: receipt.state, elapsed_ms: Date.now() - started };
    }
    throw new Failure('unknown_operation');
  } catch (error) {
    let code = error instanceof Failure ? error.code : timedOut ? 'request_timeout' :
      status && status >= 400 ? `mai_http_${status}` : ['ENOENT', 'ERR_MODULE_NOT_FOUND'].includes(error?.code) ? 'credential_or_adapter_missing' : 'transport_or_adapter_error';
    const retryable = error instanceof Failure ? error.retryable : timedOut || status === 408 || status === 429 || status >= 500 || (!status && !['ENOENT', 'ERR_MODULE_NOT_FOUND'].includes(error?.code));
    return { ok: false, error: { code, retryable }, response: responseValue, elapsed_ms: Date.now() - started };
  } finally { clearTimeout(timer); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [operation, configPath, inputPath] = process.argv.slice(2);
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    process.stdout.write(JSON.stringify(await bridge(operation, config, inputPath)) + '\n');
  } catch {
    process.stdout.write(JSON.stringify({ ok: false, error: { code: 'adapter_configuration_error', retryable: false } }) + '\n');
  }
}
