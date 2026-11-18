import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { validateConfig } from "./core.mjs";
import { createVoiceCapture, INPUT_EVENT, VOICE_ACTIONS as A } from "./voice.mjs";
import { makeSpeechHarness, fakeSpeech, uiHelpers, waitFor } from "./test_triage_pi_speech_helpers.mjs";

async function setup(t) {
  const dir = await mkdtemp(join(tmpdir(), "trg-voice-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const config = validateConfig({ baseUrl: "http://127.0.0.1:1/custom/v1", tokenEnv: "TRG_FAKE_UNUSED_TOKEN", stateDir: join(dir, "journal") });
  const h = makeSpeechHarness(config);
  t.after(() => h.emit("session_shutdown"));
  return { h, config, files: () => readdir(config.stateDir).catch(() => []) };
}

test("voice is opt-in; only a correlated final after the local finish key enters the durable user path", async t => {
  const { h, config, files } = await setup(t);
  const peer = fakeSpeech(h);
  assert.equal(peer.captures, 0);
  assert.equal((await h.prompt("/trg-voice", "extension")).action, "handled");
  assert.equal(peer.captures, 0);
  const pending = h.prompt("/trg-voice");
  await waitFor(() => peer.captures === 1);
  peer.send(A.submit, "unsolicited before finish");
  peer.send(A.update, "a partial");
  assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []);
  assert.equal(h.editor, "");
  h.key("\r");
  peer.send(A.submit, "wrong source", { source: "extension" });
  peer.send(A.submit, "wrong nonce", { sessionId: "someone-else" });
  assert.equal(h.requests.length, 0);
  peer.send(A.submit, "final spoken command");
  peer.send(A.submit, "duplicate must not route");
  await pending;
  assert.equal(peer.commits, 1); assert.equal(peer.genericReleases, 0);
  assert.equal(h.requests.length, 1);
  const body = await h.requests[0].options.onPayload();
  assert.equal(body.messages[0].content, "final spoken command");
  const record = JSON.parse(await readFile(join(config.stateDir, `${body.triage.turn_id}.json`), "utf8"));
  assert.equal(record.text, body.messages[0].content);
  assert.equal(body.triage.purpose, "user");
  assert.equal(h.genericSends.length, 0); assert.equal(h.terminal.size, 0);
  assert.equal(h.listenerCount(INPUT_EVENT), 1); // only fake AgentUtils peer remains
});

test("identical spoken commands are separate user turns, never keys derived from text", async t => {
  const { h } = await setup(t); const peer = fakeSpeech(h);
  for (let i = 1; i <= 2; i++) {
    const pending = h.prompt("/trg-voice"); await waitFor(() => peer.captures === i);
    h.key(" "); peer.send(A.submit, "same speech"); await pending;
  }
  const a = await h.requests[0].options.onPayload(), b = await h.requests[1].options.onPayload();
  assert.notEqual(a.triage.turn_id, b.triage.turn_id);
  assert.equal(a.triage.session_id, b.triage.session_id);
});

test("Esc, Ctrl-C and late callbacks cancel without touching the editor or journal", async t => {
  const { h, files } = await setup(t); const peer = fakeSpeech(h);
  for (const [i, key] of ["\x1b", "\x03"].entries()) {
    const pending = h.prompt("/trg-voice"); await waitFor(() => peer.captures === i + 1);
    peer.send(A.update, "uncommitted");
    h.key("\x1b[200~unterminated paste");
    h.key(key);
    assert.equal((await pending).action, "handled");
    peer.send(A.submit, "too late");
  }
  assert.equal(peer.cancels, 2); assert.equal(peer.genericReleases, 0);
  assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []); assert.equal(h.editor, "");
  assert.equal(h.terminal.size, 0);
});

test("model/session change during capture or final transcription revokes the nonce", async t => {
  for (const event of ["model_select", "session_tree", "session_shutdown"]) {
    const { h, files } = await setup(t); const peer = fakeSpeech(h);
    const pending = h.prompt("/trg-voice"); await waitFor(() => peer.captures === 1);
    h.key("\r");
    await h.emit(event);
    peer.send(A.submit, "stale model transcript");
    assert.equal((await pending).action, "handled");
    assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []);
    assert.equal(h.terminal.size, 0); assert.equal(peer.cancels, 1);
  }
});

test("draft-first never routes until a separate actual Enter and preserves editor edits", async t => {
  const { h, files } = await setup(t); const peer = fakeSpeech(h);
  const pending = h.prompt("/trg-voice draft"); await waitFor(() => peer.captures === 1);
  h.key("\r"); peer.send(A.submit, "draft words"); await pending;
  assert.equal(h.editor, "draft words"); assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []);
  await h.prompt("edited draft words");
  assert.equal((await h.requests[0].options.onPayload()).messages[0].content, "edited draft words");
});

test("draft-first will not overwrite an editor changed by another owner", async t => {
  const { h, files } = await setup(t); const peer = fakeSpeech(h);
  const pending = h.prompt("/trg-voice draft"); await waitFor(() => peer.captures === 1);
  h.editor = "other draft";
  h.key("\r"); peer.send(A.submit, "voice draft"); await pending;
  assert.equal(h.editor, "other draft"); assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []);
});

test("paste and release events do not commit; unknown keys cannot leak to generic PTT", async t => {
  const { h } = await setup(t); const peer = fakeSpeech(h);
  const pending = h.prompt("/trg-voice"); await waitFor(() => peer.captures === 1);
  for (const key of ["\x1b[200~pasted ", "\r", "\x1b[201~", "\r", "\x1b[13:3u", "x", "\x00"]) h.key(key);
  assert.equal(peer.commits, 0); assert.equal(peer.genericReleases, 0);
  h.key("\r"); peer.send(A.submit, "intentional final"); await pending;
  assert.equal(peer.commits, 1); assert.equal(h.requests.length, 1);
});

test("active realtime or pending model restore is refused before requesting the microphone", async t => {
  for (const snapshot of [{ previousModel: { provider: "other", id: "old" } }, { state: { connection: "connected" } }]) {
    const { h } = await setup(t); const peer = fakeSpeech(h, { snapshot });
    assert.equal((await h.prompt("/trg-voice")).action, "handled");
    assert.equal(peer.captures, 0); assert.equal(h.modelChanges.length, 0);
  }
});

test("missing bus controls, non-TUI and bad arguments cannot start voice capture", async t => {
  const { h } = await setup(t);
  assert.equal((await h.prompt("/trg-voice")).action, "handled");
  const peer = fakeSpeech(h);
  h.ctx.mode = "rpc";
  assert.equal((await h.prompt("/trg-voice", "rpc")).action, "handled");
  h.ctx.mode = "tui";
  assert.equal((await h.prompt("/trg-voice unknown")).action, "handled");
  assert.equal(peer.captures, 0);
});

test("capture/finalize deadlines remove listeners, cancel ownership and discard late results", async t => {
  for (const commit of [false, true]) {
    const { h } = await setup(t);
    const timers = new Map(); let next = 0;
    const voice = createVoiceCapture(h.pi, uiHelpers, {
      setTimer(fn, ms) { const id = ++next; timers.set(id, { fn, ms }); return id; },
      clearTimer(id) { timers.delete(id); },
    });
    const peer = fakeSpeech(h);
    const pending = voice.capture(h.ctx, () => true);
    await waitFor(() => peer.captures === 1);
    if (commit) h.key("\r");
    assert.equal(timers.size, 1);
    const timer = [...timers.values()][0]; assert.equal(timer.ms, commit ? 15000 : 60000);
    timer.fn();
    assert.equal((await pending).status, "error");
    peer.send(A.submit, "late");
    assert.equal(timers.size, 0); assert.equal(h.terminal.size, 0);
    assert.equal(h.listenerCount(INPUT_EVENT), 1); assert.equal(peer.cancels, 1);
    voice.dispose();
  }
});

test("bounded transcript rendering and invalid final text never authorize a turn", async t => {
  const { h, files } = await setup(t); const peer = fakeSpeech(h);
  const pending = h.prompt("/trg-voice"); await waitFor(() => peer.captures === 1);
  peer.send(A.update, "\x1b]52;c;clipboard\x07hello");
  for (const width of [0, 1, 20, 80]) {
    const lines = h.component.render(width);
    assert.ok(lines.every(line => line.length <= width));
    assert.doesNotMatch(lines.join(""), /[\x00-\x1f\x7f-\x9f]/);
  }
  h.key("\r"); peer.send(A.submit, "");
  assert.equal((await pending).action, "handled");
  assert.equal(h.requests.length, 0); assert.deepEqual(await files(), []);
});
