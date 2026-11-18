import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { once } from "node:events";
import { spawn } from "node:child_process";

const here = dirname(fileURLToPath(import.meta.url));
const binary = process.env.TRG_TEST_PI ?? (process.env.PI_PACKAGE_DIR && join(process.env.PI_PACKAGE_DIR, "pi"));

function run(binary, args, cwd, env, expectedCode = 0) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "", expired = false;
    const timer = setTimeout(() => { expired = true; child.kill("SIGKILL"); }, 20000);
    child.stdout.on("data", chunk => { stdout += chunk; if (stdout.length > 262144) child.kill("SIGKILL"); });
    child.stderr.on("data", chunk => { stderr += chunk; if (stderr.length > 262144) child.kill("SIGKILL"); });
    child.on("error", error => { clearTimeout(timer); reject(error); });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      if (expired || signal || code !== expectedCode || /Extension error|Failed to load extension/.test(stderr)) reject(new Error(`Pi failed: code=${code} signal=${signal}\n${stdout.slice(-3000)}\n${stderr.slice(-3000)}`));
      else resolve({ stdout, stderr });
    });
  });
}

test("installed Pi --print: dry/live fake CAPI, persisted retry, repeated user turns and summaries", { timeout: 90000, skip: !binary && "Set PI_PACKAGE_DIR or TRG_TEST_PI to Pi 1.3.0" }, async t => {
  const root = await mkdtemp(join(tmpdir(), "trg-pi-installed-"));
  const home = join(root, "home"), agent = join(root, "agent"), cwd = join(root, "work");
  const stateDir = join(root, "turns"), sessions = join(root, "sessions");
  for (const dir of [home, agent, cwd, sessions]) await mkdir(dir, { mode: 0o700 });
  t.after(() => rm(root, { recursive: true, force: true }));
  // No autoloaded packages, keys, hooks, context, shell startup, telemetry or updates.
  await writeFile(join(agent, "settings.json"), JSON.stringify({
    enableInstallTelemetry: false, retry: { enabled: false },
    compaction: { enabled: false, reserveTokens: 1024, keepRecentTokens: 1 },
  }));
  const tokenFile = join(root, "token");
  await writeFile(tokenFile, "isolated-fake-capi-token", { mode: 0o600 });
  const requests = [], violations = [];
  let failFirst = true;
  const server = createServer(async (req, res) => {
    try {
      assert.equal(req.url, "/route/v1/chat/completions");
      assert.equal(req.headers.authorization, "Bearer isolated-fake-capi-token");
      let raw = ""; for await (const chunk of req) { raw += chunk; assert.ok(raw.length < 65536); }
      const body = JSON.parse(raw);
      assert.equal(body.triage.purpose, "user");
      assert.equal(body.triage.turn_id, req.headers["idempotency-key"]);
      assert.equal(body.triage.session_id, req.headers["x-triage-session-id"]);
      assert.equal(body.tool_choice, "none");
      assert.equal(body.messages.length, 1);
      assert.deepEqual(Object.keys(body).sort(), ["model", "messages", "stream", "n", "max_tokens", "tool_choice", "triage"].sort());
      // Durability before even first server admission (not after the assistant).
      const durable = JSON.parse(await readFile(join(stateDir, `${body.triage.turn_id}.json`), "utf8"));
      assert.equal(durable.text, body.messages[0].content);
      requests.push(body);
      if (failFirst) {
        failFirst = false;
        res.writeHead(503, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: { message: "fixture temporary unavailable", type: "server_error" } }));
        return;
      }
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta, finish_reason = null) => JSON.stringify({
        id: "chatcmpl-fixture", object: "chat.completion.chunk", created: 1, model: body.model,
        choices: [{ index: 0, delta, finish_reason }],
      });
      res.write(`data: ${chunk({ role: "assistant" })}\n\n`);
      res.write(`data: ${chunk({ content: "Fixture receipt: é 🙂. No real action.\n" })}\n\n`);
      res.end(`data: ${chunk({}, "stop")}\n\ndata: [DONE]\n\n`);
    } catch (error) { violations.push(String(error)); res.writeHead(500); res.end("fixture rejected request"); }
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  const configPath = join(root, "config.json");
  await writeFile(configPath, JSON.stringify({
    baseUrl: `http://127.0.0.1:${server.address().port}/route/v1`, tokenFile, stateDir,
    allowLive: true, timeoutMs: 5000,
  }));
  const env = {
    PATH: process.env.PATH, HOME: home, TMPDIR: root, TERM: "dumb", NO_COLOR: "1",
    PI_CODING_AGENT_DIR: agent, PI_CONFIG_DIR: agent, PI_AGENT_CONFIG_DIR: agent,
    PI_CODING_AGENT_SESSION_DIR: sessions,
    PI_PACKAGE_DIR: process.env.PI_PACKAGE_DIR ?? dirname(binary),
    PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0",
    XDG_CONFIG_HOME: join(root, "config"), XDG_DATA_HOME: join(root, "data"),
    XDG_STATE_HOME: join(root, "state"), XDG_CACHE_HOME: join(root, "cache"),
    TRG_PI_CONFIG: configPath,
  };
  const flags = ["--offline", "--no-approve", "--no-extensions", "-e", join(here, "index.ts"),
    "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-tools",
    "--provider", "triage", "--model", "triage-route-dry-run", "--session-dir", sessions, "--print"];
  const version = await run(binary, ["--version"], cwd, env);
  assert.match(version.stdout, /\b1\.3\.0\b/, "Revalidate the connector against the installed Pi API before changing this pin.");
  const first = await run(binary, [...flags, "repeat actual user text"], cwd, env);
  assert.deepEqual(violations, []);
  assert.match(first.stdout, /Fixture receipt: é 🙂/);
  assert.equal(requests.length, 2); // SDK retries one 503 with the SAME UUID/body
  assert.deepEqual(requests[0], requests[1]);
  const firstId = requests[0].triage.turn_id;
  const sessionFile = join(sessions, (await readdir(sessions)).find(name => name.endsWith(".jsonl")));
  const sessionRows = (await readFile(sessionFile, "utf8")).trim().split("\n").map(line => JSON.parse(line));
  assert.ok(sessionRows.some(row => row.customType === "trg-turn" && row.data.turnId === firstId));
  assert.ok(sessionRows.some(row => row.message?.role === "assistant" && row.message.usage.totalTokens === 0));

  await run(binary, [...flags, "--session", sessionFile, "repeat actual user text"], cwd, env);
  assert.equal(requests.length, 3);
  assert.notEqual(requests[2].triage.turn_id, firstId);
  assert.equal(requests[2].triage.session_id, requests[0].triage.session_id);

  const retried = await run(binary, [...flags, "--session", sessionFile, `/trg-retry ${firstId}`], cwd, env);
  assert.equal(requests.length, 4, JSON.stringify(retried));
  assert.deepEqual(requests[3], requests[0]);

  // A separate, explicitly loaded TEST fixture tries a direct background provider
  // call and the real Pi compaction path. It never runs in operator config.
  const probePath = join(root, "probe.ts"), evidencePath = join(root, "probe-result.json");
  await writeFile(probePath, `
import { writeFile } from "node:fs/promises";
export default function (pi) {
  pi.registerCommand("fixture-summary", {
    handler: async (_args, ctx) => {
      const provider = ctx.modelRegistry.getProvider("triage");
      const events = [];
      for await (const event of provider.streamSimple(ctx.model, {
        messages: [{ role: "user", content: "repeat actual user text", timestamp: 123 }]
      }, { sessionId: ctx.sessionManager.getSessionId() })) events.push(event.type);
      const compact = await new Promise(resolve => ctx.compact({
        onComplete: () => resolve("unexpected-complete"),
        onError: () => resolve("blocked")
      }));
      await writeFile(${JSON.stringify(evidencePath)}, JSON.stringify({ events, compact }));
    }
  });
  pi.registerCommand("fixture-background", {
    handler: async () => { pi.sendUserMessage("background: do not dispatch"); }
  });
}
`);
  const summary = await run(binary, [...flags, "-e", probePath, "--session", sessionFile, "/fixture-summary"], cwd, env, 1);
  assert.match(summary.stderr, /Triage cannot compact/);
  const probe = JSON.parse(await readFile(evidencePath, "utf8"));
  assert.deepEqual(probe.events, ["error"]);
  assert.equal(probe.compact, "blocked");
  assert.equal(requests.length, 4);
  const background = await run(binary, [...flags, "-e", probePath, "--session", sessionFile, "/fixture-background"], cwd, env, 1);
  assert.match(background.stderr, /blocked extension-generated input/);
  assert.equal(requests.length, 4);

  const liveFlags = flags.map(arg => arg === "triage-route-dry-run" ? "triage-route" : arg);
  await run(binary, [...liveFlags, "fake server live-model contract only"], cwd, env);
  assert.equal(requests.length, 5);
  assert.equal(requests[4].model, "triage-route");
  assert.deepEqual(violations, []);
  assert.equal((await readdir(stateDir)).filter(name => name.endsWith(".json")).length, 3);
  t.diagnostic("Pi 1.3.0: 7 isolated processes; 5 fake HTTP requests; 3 distinct user submissions; no real credentials/actions.");
});
