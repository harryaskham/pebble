import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { mkdir, open, opendir, realpath, stat } from "node:fs/promises";
import { isAbsolute, join } from "node:path";

export const PROVIDER = "triage";
export const DRY = "triage-route-dry-run";
export const LIVE = "triage-route";
export const LIMITS = Object.freeze({ text: 16384, file: 131072, turns: 10000, response: 262144 });
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const ID = /^[A-Za-z0-9_.:-]{1,128}$/;

export class TrgError extends Error {}
const fail = (message) => { throw new TrgError(message); };

// O_NONBLOCK prevents a configured FIFO from hanging before fstat. Symlinks are
// intentionally followed; permissions apply to the resolved, opened target.
export async function readBounded(path, max, privateFile = false) {
  let fd;
  try {
    fd = await open(path, constants.O_RDONLY | constants.O_NONBLOCK);
    const info = await fd.stat();
    if (!info.isFile() || info.size > max) fail("Triage file is not a bounded regular file.");
    if (privateFile && ((info.mode & 0o077) || ![0, process.getuid?.()].includes(info.uid))) {
      fail("Triage credential/journal target must be owner-private.");
    }
    const bytes = Buffer.alloc(max + 1);
    let size = 0;
    while (size < bytes.length) {
      const { bytesRead } = await fd.read(bytes, size, bytes.length - size, size);
      if (!bytesRead) break;
      size += bytesRead;
    }
    if (size > max) fail("Triage file is too large.");
    return bytes.subarray(0, size).toString("utf8");
  } catch (error) {
    if (error instanceof TrgError) throw error;
    fail("Cannot read Triage file; check configured path and permissions.");
  } finally {
    await fd?.close();
  }
}

export function validateConfig(value) {
  const keys = ["baseUrl", "tokenFile", "tokenEnv", "stateDir", "allowLive", "timeoutMs", "maxTokens"];
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).some(k => !keys.includes(k))) {
    fail("Invalid Triage config fields.");
  }
  let url;
  try { url = new URL(value.baseUrl); } catch { fail("Invalid Triage baseUrl."); }
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) {
    fail("Triage baseUrl must be an HTTP(S) API base URL without credentials/query/fragment.");
  }
  if (Boolean(value.tokenFile) === Boolean(value.tokenEnv)) fail("Choose exactly one of tokenFile or tokenEnv.");
  if (value.tokenFile && (typeof value.tokenFile !== "string" || !isAbsolute(value.tokenFile))) fail("tokenFile must be absolute.");
  if (value.tokenEnv && (typeof value.tokenEnv !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value.tokenEnv))) fail("tokenEnv must be an environment variable name, not a token.");
  if (typeof value.stateDir !== "string" || !isAbsolute(value.stateDir)) fail("stateDir must be absolute.");
  if (value.allowLive !== undefined && typeof value.allowLive !== "boolean") fail("allowLive must be boolean.");
  const timeoutMs = value.timeoutMs ?? 95000;
  const maxTokens = value.maxTokens ?? 4096;
  if (!Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 95000) fail("timeoutMs must be 100..95000.");
  if (!Number.isInteger(maxTokens) || maxTokens < 1 || maxTokens > 8192) fail("maxTokens must be 1..8192.");
  return Object.freeze({ ...value, baseUrl: url.href.replace(/\/$/, ""), allowLive: value.allowLive ?? false, timeoutMs, maxTokens });
}

export async function loadConfig(path) {
  if (!path || !isAbsolute(path)) fail("Set TRG_PI_CONFIG to an absolute config-file path.");
  let config;
  try { config = JSON.parse(await readBounded(path, 16384)); }
  catch (error) {
    if (error instanceof TrgError) throw error;
    fail("Triage config is not valid JSON.");
  }
  return validateConfig(config);
}

export async function credential(config) {
  const token = config.tokenFile
    ? (await readBounded(config.tokenFile, 8192, true)).trim()
    : process.env[config.tokenEnv];
  if (typeof token !== "string" || !/^[\x21-\x7e]{1,8192}$/.test(token)) fail("Triage credential is missing or invalid.");
  return token;
}

export function textOf(message) {
  const value = message?.content;
  if (typeof value === "string") return value;
  if (!Array.isArray(value) || value.some(part => part.type !== "text" || typeof part.text !== "string")) return undefined;
  return value.map(part => part.text).join("\n");
}
export function validateText(text) {
  if (typeof text !== "string" || !text.trim() || text.includes("\0") || /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(text) || Buffer.byteLength(text) > LIMITS.text) fail("Triage requires 1..16384 UTF-8 bytes of valid Unicode text, without images or NUL.");
  return text;
}

export class Journal {
  constructor(config) { this.config = config; }
  async directory() {
    await mkdir(this.config.stateDir, { recursive: true, mode: 0o700 });
    const dir = await realpath(this.config.stateDir);
    const info = await stat(dir);
    if (!info.isDirectory() || (info.mode & 0o077) || info.uid !== process.getuid?.()) fail("Triage stateDir target must be owned by this user with mode 0700.");
    return dir;
  }
  async create(sessionId, model, text) {
    if (!ID.test(sessionId)) fail("Invalid Pi session identity.");
    if (![DRY, LIVE].includes(model) || (model === LIVE && !this.config.allowLive)) fail("Triage live mode is not enabled.");
    validateText(text);
    const dir = await this.directory();
    let count = 0;
    const entries = await opendir(dir);
    for await (const _entry of entries) {
      if (++count >= LIMITS.turns) fail("Triage journal quota reached; archive settled records explicitly.");
    }
    const record = Object.freeze({ version: 1, turnId: randomUUID(), sessionId, model, text, baseUrl: this.config.baseUrl });
    const fd = await open(join(dir, `${record.turnId}.json`), "wx", 0o600);
    try { await fd.writeFile(JSON.stringify(record) + "\n"); await fd.sync(); }
    finally { await fd.close(); }
    const directory = await open(dir, "r");
    try { await directory.sync(); } finally { await directory.close(); }
    return record;
  }
  async read(turnId) {
    if (!UUID.test(turnId)) fail("Use /trg-retry <turn UUID> from the journal; never retype an ambiguous command.");
    let record;
    try { record = JSON.parse(await readBounded(join(await this.directory(), `${turnId}.json`), LIMITS.file, true)); }
    catch (error) {
      if (error instanceof TrgError) throw error;
      fail("Invalid Triage journal record.");
    }
    if (!record || Object.keys(record).some(key => !["version", "turnId", "sessionId", "model", "text", "baseUrl"].includes(key)) || record.version !== 1 || record.turnId !== turnId || typeof record.sessionId !== "string" || !ID.test(record.sessionId) || record.baseUrl !== this.config.baseUrl || ![DRY, LIVE].includes(record.model)) fail("Triage journal identity or endpoint mismatch.");
    if (record.model === LIVE && !this.config.allowLive) fail("Triage live mode is not enabled.");
    validateText(record.text);
    return Object.freeze(record);
  }
}

export function payload(record, maxTokens) {
  return {
    model: record.model,
    messages: [{ role: "user", content: record.text }],
    stream: true, n: 1, max_tokens: maxTokens, tool_choice: "none",
    triage: { session_id: record.sessionId, turn_id: record.turnId, purpose: "user", operation: "submit" },
  };
}

// The adapter may retry this function, but cannot change the identity or body.
// No ambient fetch hooks, redirects, model overrides or provider headers are used.
export function guardedFetch(config, record, signal) {
  const endpoint = `${config.baseUrl}/chat/completions`;
  return async (input, init) => {
    const requestedUrl = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (requestedUrl !== endpoint) fail("Triage endpoint override refused.");
    const response = await fetch(endpoint, {
      method: "POST", redirect: "error",
      signal: AbortSignal.any([signal, ...(init?.signal ? [init.signal] : [])]),
      headers: {
        "Content-Type": "application/json", Accept: "text/event-stream",
        Authorization: `Bearer ${await credential(config)}`,
        "X-Triage-Session-ID": record.sessionId,
        "Idempotency-Key": record.turnId,
        "X-Triage-Purpose": "user",
      },
      body: JSON.stringify(payload(record, config.maxTokens)),
    });
    if (!response.body) fail("Triage returned an empty HTTP response.");
    let bytes = 0;
    const body = response.body.pipeThrough(new TransformStream({
      transform(chunk, controller) {
        bytes += chunk.byteLength;
        if (bytes > LIMITS.response) throw new TrgError("Triage response exceeded byte limit.");
        controller.enqueue(chunk);
      },
    }));
    return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
  };
}

export function outputMessage(model) {
  return {
    role: "assistant", api: "openai-completions", provider: PROVIDER, model: model.id,
    content: [], stopReason: "pending", timestamp: Date.now(),
    // Pi requires a Usage object. Zero means unavailable here, NOT free/zero work.
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  };
}
