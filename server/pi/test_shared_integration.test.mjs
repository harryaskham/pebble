import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, mkdir, readFile, readdir, rm, writeFile, copyFile, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { once } from "node:events";
import { spawn } from "node:child_process";

const here = dirname(fileURLToPath(import.meta.url));
const shared = new URL("../../examples/pi/", import.meta.url);
const binary = process.env.TRG_TEST_PI ?? (process.env.PI_PACKAGE_DIR && join(process.env.PI_PACKAGE_DIR, "pi"));

function run(args, cwd, env, expectedCode = 0) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, { cwd, env, stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "", stderr = "", expired = false;
    const timer = setTimeout(() => { expired = true; child.kill("SIGKILL"); }, 20000);
    child.stdout.on("data", chunk => { stdout += chunk; if (stdout.length > 262144) child.kill("SIGKILL"); });
    child.stderr.on("data", chunk => { stderr += chunk; if (stderr.length > 262144) child.kill("SIGKILL"); });
    child.on("error", error => { clearTimeout(timer); reject(error); });
    child.on("close", (code, signal) => {
      clearTimeout(timer);
      if (expired || signal || code !== expectedCode || /Extension error|Failed to load extension|Failed to load models/.test(stderr)) {
        reject(new Error(`Pi failed: code=${code} signal=${signal}\n${stdout.slice(-3000)}\n${stderr.slice(-3000)}`));
      } else resolve({ stdout, stderr });
    });
  });
}

test("shared models.json + autoloaded connector: actual Pi catalog, dry/live guard, no-extension fail-closed", {
  timeout: 120000, skip: !binary && "Set TRG_TEST_PI or PI_PACKAGE_DIR to Pi 1.3.0",
}, async t => {
  const root = await mkdtemp(join(tmpdir(), "trg-shared-pi-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const home = join(root, "home"), agent = join(home, ".pi/agent"), cwd = join(root, "work");
  const configDir = agent;
  const connectorDir = join(agent, "pebble-triage");
  const stateDir = join(home, ".local/state/pebble/pi");
  for (const dir of [agent, cwd, configDir, connectorDir]) await mkdir(dir, { recursive: true, mode: 0o700 });
  for (const name of ["index.ts", "core.mjs", "connector.mjs", "voice.mjs", "shared-config.mjs"]) {
    await copyFile(join(here, name), join(connectorDir, name));
  }
  // Preserve the actual relative package path, model overrides, defaults and
  // compaction budget; omit unrelated extensions/providers and their I/O.
  const settings = JSON.parse(await readFile(new URL("settings.json", shared), "utf8"));
  Object.assign(settings, {
    packages: settings.packages.slice(0, 1), extensions: [], skills: [], prompts: [], themes: [], theme: "dark",
    enableInstallTelemetry: false, retry: { enabled: false },
  });
  await writeFile(join(configDir, "settings.json"), JSON.stringify(settings));
  const provider = JSON.parse(await readFile(new URL("models.json", shared), "utf8")).providers.triage;
  const preferences = JSON.parse(await readFile(new URL("triage.json", shared), "utf8"));
  const requests = [], violations = [];
  const server = createServer(async (req, res) => {
    try {
      assert.equal(req.url, "/route/v1/chat/completions");
      assert.equal(req.headers.authorization, "Bearer fixture-only-token");
      let raw = "";
      for await (const chunk of req) { raw += chunk; assert.ok(raw.length < 65536); }
      const body = JSON.parse(raw);
      assert.equal(body.triage.purpose, "user");
      assert.equal(body.triage.turn_id, req.headers["idempotency-key"]);
      assert.equal(body.triage.session_id, req.headers["x-triage-session-id"]);
      assert.equal(body.messages.length, 1);
      assert.equal(body.tool_choice, "none");
      assert.equal(body.max_tokens, 4096);
      assert.equal(body.tools, undefined);
      const durable = JSON.parse(await readFile(join(stateDir, `${body.triage.turn_id}.json`), "utf8"));
      assert.equal(durable.text, body.messages[0].content);
      assert.equal(durable.model, body.model);
      requests.push(body);
      const chunk = (delta, finish_reason = null) => JSON.stringify({
        id: "chatcmpl-shared", object: "chat.completion.chunk", created: 1, model: body.model,
        choices: [{ index: 0, delta, finish_reason }],
      });
      res.writeHead(200, { "Content-Type": "text/event-stream" });
      res.end(`data: ${chunk({ role: "assistant", content: "Shared fixture receipt. No real action." })}\n\ndata: ${chunk({}, "stop")}\n\ndata: [DONE]\n\n`);
    } catch (error) { violations.push(String(error)); res.writeHead(500); res.end("fixture rejected request"); }
  });
  server.listen(0, "127.0.0.1"); await once(server, "listening");
  t.after(() => { server.closeAllConnections(); return new Promise(resolve => server.close(resolve)); });
  provider.baseUrl = `http://127.0.0.1:${server.address().port}/route/v1`;
  await writeFile(join(configDir, "models.json"), JSON.stringify({ providers: { triage: provider } }));
  await writeFile(join(configDir, "triage.json"), JSON.stringify(preferences));
  const env = {
    PATH: process.env.PATH, HOME: home, TMPDIR: root, TERM: "dumb", NO_COLOR: "1",
    PI_CODING_AGENT_DIR: agent, PI_CONFIG_DIR: agent, PI_AGENT_CONFIG_DIR: agent,
    PI_PACKAGE_DIR: process.env.PI_PACKAGE_DIR ?? dirname(binary),
    PI_OFFLINE: "1", PI_SKIP_VERSION_CHECK: "1", PI_TELEMETRY: "0",
    XDG_CONFIG_HOME: join(root, "config"), XDG_DATA_HOME: join(root, "data"),
    XDG_STATE_HOME: join(root, "state"), XDG_CACHE_HOME: join(root, "cache"),
  };
  const common = ["--offline", "--no-approve", "--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--no-tools"];
  const selected = ["--provider", "triage", "--model", "triage-route-dry-run", "--print"];
  const listing = await run([...common, "--list-models", "triage"], cwd, env);
  assert.match(listing.stdout, /triage-route-dry-run/);
  assert.match(listing.stdout, /triage-route\s/);
  await assert.rejects(readdir(stateDir), { code: "ENOENT" });
  assert.equal(requests.length, 0); // no credential file exists yet
  const absent = await run([...common, "--no-extensions", "--list-models", "triage"], cwd, env);
  assert.doesNotMatch(absent.stdout, /triage-route/);
  assert.equal(requests.length, 0);

  const evidence = join(root, "probe.json"), probe = join(root, "probe.ts");
  await writeFile(probe, `
import { writeFile } from "node:fs/promises";
export default function (pi) {
  pi.registerCommand("fixture-probe", { handler: async (_args, ctx) => {
    const provider = ctx.modelRegistry.getProvider("triage"), events = [];
    for await (const event of provider.streamSimple(ctx.model, {
      messages: [{ role: "user", content: "unclassified background", timestamp: 1 }]
    }, { sessionId: ctx.sessionManager.getSessionId() })) events.push(event.type);
    await writeFile(${JSON.stringify(evidence)}, JSON.stringify({
      api: ctx.model.api, contextWindow: ctx.model.contextWindow,
      liveName: ctx.modelRegistry.find("triage", "triage-route")?.name,
      scoped: ctx.scopedModels.map(item => item.model.id), events,
      commands: pi.getCommands().filter(item => item.name.startsWith("trg-status")).map(item => item.name)
    }));
  }});
  pi.registerCommand("fixture-background", { handler: async () => { pi.sendUserMessage("not operator input"); }});
  pi.registerCommand("fixture-stale-endpoint", { handler: async (_args, ctx) => {
    await pi.setModel({ ...ctx.model, baseUrl: ctx.model.baseUrl + "/changed" });
    pi.sendUserMessage("must not reach either endpoint");
  }});
}
`);
  await run([...common, ...selected, "-e", probe, "/fixture-probe"], cwd, env);
  const result = JSON.parse(await readFile(evidence, "utf8"));
  assert.equal(result.api, "triage-chat-completions");
  assert.equal(result.contextWindow, provider.modelOverrides["triage-route-dry-run"].contextWindow);
  assert.equal(result.liveName, "Triage (LIVE dispatch)");
  // Cycling follows the shared enabledModels (the dry run may be pruned); live never cycles.
  assert.equal(result.scoped.includes("triage-route-dry-run"), settings.enabledModels.includes("triage/triage-route-dry-run"));
  assert.ok(!result.scoped.includes("triage-route"));
  assert.deepEqual(result.events, ["error"]);
  assert.deepEqual(result.commands, ["trg-status"]);
  assert.equal(requests.length, 0);
  const background = await run([...common, ...selected, "-e", probe, "/fixture-background"], cwd, env, 1);
  assert.match(background.stderr, /blocked extension-generated input/);
  const stale = await run([...common, ...selected, "-e", probe, "/fixture-stale-endpoint"], cwd, env, 1);
  assert.match(stale.stderr, /Triage endpoint\/API changed/);
  assert.equal(requests.length, 0);

  const tokenFile = join(home, ".config/pebble/ingress.token");
  await mkdir(dirname(tokenFile), { recursive: true, mode: 0o700 });
  await writeFile(tokenFile, "fixture-only-token", { mode: 0o600 });
  const dry = await run([...common, ...selected, "shared dry fixture"], cwd, env);
  assert.match(dry.stdout, /Shared fixture receipt/);
  const live = await run([...common, ...selected.map(value => value === "triage-route-dry-run" ? "triage-route" : value), "shared live fixture"], cwd, env);
  assert.match(live.stdout, /Shared fixture receipt/);
  assert.deepEqual(requests.map(body => body.model), ["triage-route-dry-run", "triage-route"]);
  assert.equal((await readdir(stateDir)).length, 2);
  assert.deepEqual(violations, []);

  // Old explicit -e invocations refer to the same entry point: no duplicate
  // connector, commands, voice handlers or durable turn identities.
  await run([...common, ...selected, "-e", join(connectorDir, "index.ts"), "-e", probe, "/fixture-probe"], cwd, env);
  assert.deepEqual(JSON.parse(await readFile(evidence, "utf8")).commands, ["trg-status"]);
  const override = join(root, "dry-only.json");
  await writeFile(override, JSON.stringify({ baseUrl: provider.baseUrl, tokenFile, stateDir, allowLive: false }));
  const overridden = await run([...common, "--list-models", "triage"], cwd, { ...env, TRG_PI_CONFIG: override });
  assert.match(overridden.stdout, /triage-route-dry-run/);
  assert.doesNotMatch(overridden.stdout, /triage-route\s/);
  assert.equal(requests.length, 2);
  t.diagnostic("Autoload + symlinked shared settings/models verified; 2 loopback fixture requests, no real tokens/effects.");
});
