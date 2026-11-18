import assert from "node:assert/strict";
import { test } from "node:test";
import { chmod, mkdtemp, mkdir, rm, writeFile, symlink, readdir, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:http";
import { once } from "node:events";
import { randomUUID } from "node:crypto";
import { DRY, LIVE, Journal, validateConfig, credential, guardedFetch, payload } from "./core.mjs";
import { installConnector } from "./connector.mjs";

export async function fixture(t, extra = {}) {
  const dir = await mkdtemp(join(tmpdir(), "trg-pi-unit-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const tokenFile = join(dir, "token");
  await writeFile(tokenFile, "fake-test-token\n", { mode: 0o600 });
  return { dir, config: validateConfig({ baseUrl: "http://127.0.0.1:1/route/v1", tokenFile, stateDir: join(dir, "turns"), ...extra }) };
}

// Minimal stream shape, not a reimplementation of Pi's HTTP adapter. The real
// installed adapter/extension loader is exercised by the separate process suite.
function eventStream() {
  const queue = [];
  let ended = false, wake;
  return {
    push(value) { queue.push(value); wake?.(); }, end() { ended = true; wake?.(); },
    async *[Symbol.asyncIterator]() {
      while (!ended || queue.length) {
        if (queue.length) yield queue.shift();
        else await new Promise(resolve => { wake = resolve; });
      }
    },
  };
}
async function harness(t, extra = {}, sdkEvents) {
  const { config, dir } = await fixture(t, extra);
  const handlers = new Map(), commands = new Map(), requests = [], entries = [];
  let provider;
  const ctx = {
    model: { provider: "triage", id: DRY }, hasUI: false, isIdle: () => true,
    sessionManager: { getSessionId: () => "pi-test-session" }, waitForIdle: async () => {},
  };
  const pi = {
    registerProvider(_name, definition) { provider = definition; },
    on(name, handler) { handlers.set(name, handler); },
    registerCommand(name, definition) { commands.set(name, definition); },
    appendEntry(type, data) { entries.push({ type, data }); },
    sendUserMessage(text) { pi.sent = text; }, sendMessage() {},
  };
  installConnector(pi, {
    createAssistantMessageEventStream: eventStream,
    openAICompletionsApi: () => ({ streamSimple(model, context, options) {
      requests.push({ model, context, options });
      const stream = eventStream();
      for (const event of sdkEvents ?? [{ type: "done", reason: "stop", message: {} }]) stream.push(event);
      stream.end(); return stream;
    } }),
  }, config);
  const emit = (name, event = {}) => handlers.get(name)?.(event, ctx);
  const submit = async (text, source = "interactive") => {
    const input = await emit("input", { text, source });
    if (input?.action === "handled") return;
    if (input?.action === "transform") text = input.text;
    await emit("before_agent_start", { prompt: text });
    const message = { role: "user", content: text, timestamp: 123 };
    await emit("message_end", { message });
    return message;
  };
  const context = async messages => (await emit("context", { messages }))?.messages ?? messages;
  const invoke = async messages => Array.fromAsync(provider.streamSimple(ctx.model, { messages }, { sessionId: "pi-test-session" }));
  return { config, dir, pi, ctx, provider, emit, submit, context, invoke, requests, commands, entries };
}

test("config defaults dry; rejects secrets in URL, literal-token config and invalid bounds", async t => {
  const { config } = await fixture(t);
  assert.equal(config.allowLive, false);
  for (const change of [{ baseUrl: "https://secret@host/route/v1" }, { token: "secret" }, { timeoutMs: 0 }, { maxTokens: 8193 }, { allowLive: "false" }]) {
    assert.throws(() => validateConfig({ ...config, ...change }));
  }
  assert.equal(validateConfig({ ...config, baseUrl: "https://example.test/route/v1/" }).baseUrl, "https://example.test/route/v1");
  assert.equal(validateConfig({ ...config, baseUrl: "https://proxy.test/custom/v1" }).baseUrl, "https://proxy.test/custom/v1");
});

test("journal fsyncs random identities before first response; repeat is distinct; restart replays exactly", async t => {
  const { config } = await fixture(t);
  const journal = new Journal(config);
  const a = await journal.create("session", DRY, "same"), b = await journal.create("session", DRY, "same");
  assert.notEqual(a.turnId, b.turnId);
  assert.equal((await stat(join(config.stateDir, `${a.turnId}.json`))).mode & 0o777, 0o600);
  assert.deepEqual(await new Journal(config).read(a.turnId), a);
  await assert.rejects(() => journal.read("../../bad"));
  await assert.rejects(() => journal.create("session", LIVE, "no"));
  await assert.rejects(() => journal.create("session", DRY, "\ud800"));
  await assert.rejects(() => journal.create("session", DRY, "x".repeat(16385)));
  await assert.rejects(() => new Journal({ ...config, baseUrl: "http://other/route/v1" }).read(a.turnId));
});

test("managed config/token/state symlinks accepted; secret target permissions enforced; env name supported", async t => {
  const { config, dir } = await fixture(t);
  const link = join(dir, "token-link"); await symlink("token", link);
  assert.equal(await credential({ ...config, tokenFile: link }), "fake-test-token");
  await writeFile(config.tokenFile, "rotated", { mode: 0o600 });
  assert.equal(await credential({ ...config, tokenFile: link }), "rotated");
  const target = join(dir, "real-state"); await mkdir(target, { mode: 0o700 });
  await symlink("real-state", config.stateDir);
  await new Journal(config).create("s", DRY, "hello");
  assert.equal((await readdir(target)).length, 1);
  const envName = `TRG_TEST_TOKEN_${randomUUID().replaceAll("-", "")}`;
  process.env[envName] = "test-env-token";
  t.after(() => { delete process.env[envName]; });
  assert.equal(await credential(validateConfig({ ...config, tokenFile: undefined, tokenEnv: envName })), "test-env-token");
  const bad = join(dir, "public-token"); await writeFile(bad, "not-private", { mode: 0o600 }); await chmod(bad, 0o644);
  await assert.rejects(() => credential({ ...config, tokenFile: bad }));
});

test("only actual input + normal context capability may dispatch; direct summaries and replayed objects blocked", async t => {
  const h = await harness(t);
  const user = await h.submit("repeat");
  assert.equal((await readdir(h.config.stateDir)).length, 1); // before provider called
  assert.equal((await h.invoke([user]))[0].type, "error"); // no context authorization
  const authorized = await h.context([user, { role: "custom", content: "background instructions" }]);
  await h.invoke(authorized);
  assert.equal(h.requests.length, 1);
  assert.equal(h.requests[0].context.messages.length, 1);
  assert.deepEqual((await h.requests[0].options.onPayload()).triage, {
    session_id: "pi-test-session", turn_id: h.entries[0].data.turnId, purpose: "user", operation: "submit",
  });
  assert.equal((await h.invoke(authorized))[0].type, "error"); // consumed capability
  await h.invoke([{ role: "user", content: "summarize repeat", timestamp: 123 }]);
  assert.equal(h.requests.length, 1);
  await h.invoke(await h.context([user])); // Pi logical retry
  assert.equal(h.requests.length, 2);
  assert.deepEqual(await h.requests[0].options.onPayload(), await h.requests[1].options.onPayload());
});

test("Pi 1.3 system transcript messages neither authorize nor block the one capability-bearing utterance", async t => {
  const h = await harness(t);
  const system = { role: "system", content: "", sections: { base: "coding agent prompt" }, timestamp: 1 };
  const user = await h.submit("route this");
  const [authorized] = await h.context([user]);
  // Pi 1.3 re-adds system-prompt/tool messages after context(); dispatch proceeds,
  // and the wire payload is still only the journaled utterance.
  await h.invoke([system, authorized]);
  assert.equal(h.requests.length, 1);
  assert.deepEqual(h.requests[0].context.messages.map(message => message.role), ["user"]);
  assert.equal(h.requests[0].context.messages[0].content, "route this");
  // System messages carry no authority: unminted or extra user messages stay blocked.
  assert.equal((await h.invoke([system, { role: "user", content: "route this", timestamp: 123 }]))[0].type, "error");
  const again = (await h.emit("agent_settled"), await h.submit("second"));
  const [minted] = await h.context([again]);
  assert.equal((await h.invoke([system, minted, { role: "user", content: "extra", timestamp: 2 }]))[0].type, "error");
  assert.equal((await h.invoke([system]))[0].type, "error");
  assert.equal(h.requests.length, 1);
});

test("identical actual submissions get different UUIDs, retry command retains identity across connector restart", async t => {
  const h = await harness(t);
  const user = await h.submit("same"); await h.invoke(await h.context([user]));
  await h.emit("agent_settled");
  const second = await h.submit("same"); await h.invoke(await h.context([second]));
  const firstBody = await h.requests[0].options.onPayload(), secondBody = await h.requests[1].options.onPayload();
  assert.notEqual(firstBody.triage.turn_id, secondBody.triage.turn_id);
  await h.emit("session_start");
  const retry = await h.submit(`/trg-retry ${firstBody.triage.turn_id}`); await h.invoke(await h.context([retry]));
  assert.deepEqual(await h.requests[2].options.onPayload(), firstBody);
});

test("compaction/tree summary, extension injection, input transformation, queueing and stale sessions fail closed", async t => {
  const h = await harness(t);
  assert.deepEqual(await h.emit("session_before_compact"), { cancel: true });
  assert.deepEqual(await h.emit("session_before_tree", { preparation: { userWantsSummary: true } }), { cancel: true });
  assert.equal(await h.emit("session_before_tree", { preparation: { userWantsSummary: false } }), undefined);
  assert.equal(await h.submit("background", "extension"), undefined);
  const user = await h.submit("first");
  h.ctx.isIdle = () => false;
  assert.equal((await h.emit("input", { text: "queue", source: "interactive", streamingBehavior: "followUp" })).action, "handled");
  h.ctx.isIdle = () => true;
  await h.emit("agent_settled");
  await h.invoke(await h.context([user])); assert.equal(h.requests.length, 0);
  await h.emit("input", { text: "/template", source: "interactive" });
  await h.emit("before_agent_start", { prompt: "expanded instructions" });
  const transformed = { role: "user", content: "expanded instructions", timestamp: 123 };
  await h.emit("message_end", { message: transformed });
  await h.invoke(await h.context([transformed])); assert.equal(h.requests.length, 0);
  await h.emit("agent_settled");
  const good = await h.submit("good");
  await h.emit("session_shutdown");
  await h.invoke(await h.context([good])); assert.equal(h.requests.length, 0);
});

test("live is opt-in; model switch is untouched; unrelated provider events ignored", async t => {
  const h = await harness(t, { allowLive: true });
  assert.deepEqual(h.provider.models.map(m => m.id), [DRY, LIVE]);
  h.ctx.model.id = LIVE;
  const user = await h.submit("live fixture only"); await h.invoke(await h.context([user]));
  assert.equal((await h.requests[0].options.onPayload()).model, LIVE);
  h.ctx.model = { provider: "elsewhere", id: "unrelated" };
  assert.equal(await h.emit("session_before_compact"), undefined);
  assert.equal(await h.emit("input", { source: "interactive", text: "hello" }), undefined);
});

test("bounded real HTTP transport sends exact identity; redirects and oversized responses are refused", { timeout: 5000 }, async t => {
  let received, mode = "normal", redirectTargetHit = false;
  const server = createServer(async (req, res) => {
    if (req.url === "/other") { redirectTargetHit = true; res.end(); return; }
    let body = ""; for await (const chunk of req) body += chunk;
    received = { body: JSON.parse(body), headers: req.headers };
    if (mode === "redirect") { res.writeHead(307, { Location: "/other" }); res.end(); }
    else if (mode === "large") { res.end("x".repeat(262145)); }
    else if (mode === "stall") { /* cancelled by caller deadline */ }
    else { res.end("data: [DONE]\n\n"); }
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const { config } = await fixture(t, { baseUrl: `http://127.0.0.1:${server.address().port}/route/v1` });
  const record = await new Journal(config).create("s", DRY, "é 🙂");
  const run = guardedFetch(config, record, AbortSignal.timeout(3000));
  const url = config.baseUrl + "/chat/completions";
  await (await run(url)).text();
  assert.deepEqual(received.body, payload(record, config.maxTokens));
  assert.equal(received.headers["x-triage-purpose"], "user");
  assert.equal(received.headers["idempotency-key"], record.turnId);
  mode = "redirect"; await assert.rejects(() => run(url)); assert.equal(redirectTargetHit, false);
  mode = "large"; await assert.rejects(async () => (await run(url)).text());
  await assert.rejects(() => run("http://other/route/v1/chat/completions"));
  mode = "stall";
  await assert.rejects(() => guardedFetch(config, record, AbortSignal.timeout(100))(url));
});

test("concurrent input preflights do not overwrite the admitted command", async t => {
  const h = await harness(t);
  const first = h.emit("input", { text: "first", source: "rpc" });
  const other = await h.emit("input", { text: "first", source: "rpc" });
  assert.equal(other.action, "handled");
  assert.equal((await first).action, "continue");
  assert.equal((await readdir(h.config.stateDir)).length, 1);
});

test("unexpected tools, provider errors and incomplete streams fail closed with redacted error", async t => {
  for (const events of [[{ type: "toolcall_start" }], [{ type: "error", error: { errorMessage: "SECRET upstream body" } }], []]) {
    const h = await harness(t, {}, events);
    const user = await h.submit("safe mock");
    const output = await h.invoke(await h.context([user]));
    assert.equal(output.at(-1).type, "error");
    assert.match(output.at(-1).error.errorMessage, /trg-retry/);
    assert.doesNotMatch(JSON.stringify(output), /SECRET|toolcall/);
  }
});
