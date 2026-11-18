import assert from "node:assert/strict";
import { test } from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = process.env.TRG_TEST_AGENT_UTILS;
test("actual AgentUtils PTT bus handler with fake PCM/STT, isolated HOME and no network/audio", {
  timeout: 25000, skip: !root && "Set TRG_TEST_AGENT_UTILS to a reviewed AgentUtils checkout",
}, async t => {
  const dir = await mkdtemp(join(tmpdir(), "trg-real-stt-handler-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const env = {
    PATH: process.env.PATH, HOME: dir, TMPDIR: dir,
    XDG_CONFIG_HOME: dir, XDG_STATE_HOME: dir, XDG_DATA_HOME: dir, XDG_CACHE_HOME: dir,
    PI_CODING_AGENT_DIR: dir, PI_RT_DISABLE_AUDIO: "1", PI_RT_DIRECT_AZURE: "0",
    PI_OFFLINE: "1", PI_TELEMETRY: "0", DISABLE_PI_CACO: "1",
    TRG_TEST_AGENT_UTILS: root,
  };
  const probe = join(dirname(fileURLToPath(import.meta.url)), "test_triage_pi_speech_agent_utils_probe.mjs");
  const { stdout, stderr } = await promisify(execFile)(process.execPath, [probe], {
    cwd: dir, env, timeout: 20000, killSignal: "SIGKILL", maxBuffer: 65536,
  });
  assert.equal(stderr, "");
  assert.deepEqual(JSON.parse(stdout), { actualAgentUtils: true, fakeCaptures: 3, routed: 1, genericSends: 0, modelChanges: 0 });
});
