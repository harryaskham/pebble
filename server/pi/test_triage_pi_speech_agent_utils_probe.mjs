// Executed only by the isolated subprocess test; never import into operator Pi.
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { validateConfig } from "./core.mjs";
import { INPUT_EVENT, VOICE_ACTIONS as A } from "./voice.mjs";
import { makeSpeechHarness, waitFor } from "./test_triage_pi_speech_helpers.mjs";

globalThis.fetch = async () => { throw new Error("NETWORK FORBIDDEN in speech probe"); };
const source = join(process.env.TRG_TEST_AGENT_UTILS, "extensions/realtime-agent.js");
const { default: realtime, __setLocalVadHooksForTest, setRealtimeWebSocketConstructor } = await import(pathToFileURL(source));
const actualActions = await import(pathToFileURL(join(process.env.TRG_TEST_AGENT_UTILS, "extensions/lib/input-actions.js")));
assert.equal(actualActions.INPUT_ACTION_EVENT, INPUT_EVENT);
assert.deepEqual(Object.values(A).sort(), Object.entries(actualActions.INPUT_ACTIONS).filter(([key]) => key.startsWith("FREEFORM_")).map(([, value]) => value).sort());
setRealtimeWebSocketConstructor(class { constructor() { throw new Error("WEBSOCKET FORBIDDEN in speech probe"); } });
const captures = [];
let transcript = async () => "real handler fake transcription";
__setLocalVadHooksForTest({
  capture() {
    const capture = new EventEmitter(); capture.stdout = new EventEmitter(); capture.stderr = new EventEmitter();
    capture.kill = () => { capture.killed = true; };
    captures.push(capture); return capture;
  },
  // Like a real provider, honour the request signal: shutdown aborts owned STT
  // requests and waits for them to settle.
  transcribe: (_buffer, { signal } = {}) => new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    signal?.addEventListener("abort", () => reject(signal.reason), { once: true });
    Promise.resolve(transcript()).then(resolve, reject);
  }),
});
const config = validateConfig({ baseUrl: "http://127.0.0.1:1/custom/v1", tokenEnv: "TRG_NEVER_READ", stateDir: join(process.env.HOME, "turns") });
const h = makeSpeechHarness(config);
realtime(h.pi); // actual factory; our controller subscribed to realtime:controls first
const pcm = (ms, speech) => {
  const n = Math.round(ms / 1000 * 24000), bytes = Buffer.alloc(n * 2);
  if (speech) for (let i = 0; i < n; i++) bytes.writeInt16LE(8000, i * 2);
  return bytes;
};
const feed = capture => {
  capture.stdout.emit("data", pcm(500, true));
  for (let i = 0; i < 12; i++) capture.stdout.emit("data", pcm(100, false));
};
try {
  await h.emit("session_start", { reason: "startup" });
  assert.equal(captures.length, 0);
  const pending = h.prompt("/trg-voice");
  await waitFor(() => captures.length === 1);
  feed(captures[0]);
  await waitFor(() => h.events.some(({ event }) => event.action === A.update && event.text === "real handler fake transcription"));
  assert.equal(h.requests.length, 0); assert.equal(h.editor, "");
  h.key("\r"); // real listener order: our raw guard precedes AgentUtils hold-release
  await pending;
  assert.equal(captures[0].killed, true);
  assert.equal(h.requests.length, 1);
  assert.equal((await h.requests[0].options.onPayload()).messages[0].content, "real handler fake transcription");
  assert.equal(h.genericSends.length, 0);
  assert.equal(h.modelChanges.length, 0);

  const canceled = h.prompt("/trg-voice");
  await waitFor(() => captures.length === 2);
  feed(captures[1]);
  h.key("\x1b"); await canceled;
  assert.equal(captures[1].killed, true);
  assert.equal(h.editor, "", "Esc did not reach AgentUtils finalizeHeldToEditor");
  assert.equal(h.requests.length, 1);

  let resolveTranscript;
  transcript = () => new Promise(resolve => { resolveTranscript = resolve; });
  const stale = h.prompt("/trg-voice");
  await waitFor(() => captures.length === 3);
  feed(captures[2]);
  await waitFor(() => resolveTranscript);
  h.key("\r");
  await h.emit("model_select");
  resolveTranscript("late transcript after model change");
  await stale;
  await new Promise(resolve => setTimeout(resolve, 25));
  assert.equal(captures[2].killed, true);
  assert.equal(h.requests.length, 1); assert.equal(h.genericSends.length, 0);
  assert.equal(h.modelChanges.length, 0);
  console.log(JSON.stringify({ actualAgentUtils: true, fakeCaptures: captures.length, routed: h.requests.length, genericSends: 0, modelChanges: 0 }));
} finally {
  await h.emit("session_shutdown");
  __setLocalVadHooksForTest({});
  assert.ok(captures.every(capture => capture.killed));
  assert.equal(h.terminal.size, 0);
}
