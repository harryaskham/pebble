import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { defaultSharedDirectory, loadSharedConfig, sharedModelMatches } from "./shared-config.mjs";

const shared = new URL("../../examples/pi/", import.meta.url);

test("shared provider uses guarded model overrides, preserves defaults and keeps live out of cycling", async () => {
  const settings = JSON.parse(await readFile(new URL("settings.json", shared), "utf8"));
  const models = JSON.parse(await readFile(new URL("models.json", shared), "utf8"));
  assert.equal(settings.packages[0], "./pebble-triage/index.ts");
  assert.ok(!settings.enabledModels.includes("triage/triage-route"));
  assert.equal(models.providers.triage.models, undefined);
  assert.equal(models.providers.triage.api, "triage-chat-completions");
  assert.equal(models.providers.triage.apiKey, "trg-transport-resolves-credential");
  assert.deepEqual(Object.keys(models.providers.triage.modelOverrides).sort(), ["triage-route", "triage-route-dry-run"]);
  const config = await loadSharedConfig(shared, "/fixture/home");
  assert.equal(config.baseUrl, "http://127.0.0.1:11301/route/v1");
  assert.equal(config.tokenFile, "/fixture/home/.config/pebble/ingress.token");
  assert.equal(config.stateDir, "/fixture/home/.local/state/pebble/pi");
  assert.equal(config.allowLive, false); // examples start in dry-run-only mode
  assert.ok(Object.isFrozen(config));
  for (const model of Object.values(models.providers.triage.modelOverrides)) {
    assert.ok(model.contextWindow > settings.compaction.reserveTokens + settings.compaction.keepRecentTokens);
  }
});

test("shared loader validates bounded config without reading credentials or creating state", async t => {
  const root = await mkdtemp(join(tmpdir(), "trg-shared-config-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const directory = pathToFileURL(root + "/");
  const provider = { api: "triage-chat-completions", baseUrl: "http://127.0.0.1:1234/route/v1/" };
  const preferences = { tokenEnv: "FIXTURE_TOKEN", stateDir: "~/turns", allowLive: false };
  const put = async (p = provider, c = preferences) => {
    await writeFile(join(root, "models.json"), JSON.stringify({ providers: { triage: p } }));
    await writeFile(join(root, "triage.json"), JSON.stringify(c));
  };
  await put();
  const config = await loadSharedConfig(directory, root);
  assert.equal(config.allowLive, false);
  assert.equal(config.baseUrl, "http://127.0.0.1:1234/route/v1");
  assert.equal(config.stateDir, join(root, "turns"));
  assert.ok(sharedModelMatches({ api: provider.api, baseUrl: provider.baseUrl }, config));
  assert.ok(!sharedModelMatches({ api: "openai-completions", baseUrl: provider.baseUrl }, config));
  assert.ok(!sharedModelMatches({ api: provider.api, baseUrl: "http://127.0.0.1:4321/route/v1" }, config));
  assert.ok(!sharedModelMatches({}, config));
  await put({ ...provider, api: "openai-completions" });
  await assert.rejects(loadSharedConfig(directory), /guarded API/);
  await put({ ...provider, models: [{ id: "triage-route" }] });
  await assert.rejects(loadSharedConfig(directory), /modelOverrides/);
  await put(provider, { ...preferences, baseUrl: "http://other.invalid" });
  await assert.rejects(loadSharedConfig(directory), /only in models.json/);
  await put(provider, { ...preferences, allowLive: "false" });
  await assert.rejects(loadSharedConfig(directory), /allowLive must be boolean/);
  await put();
  await writeFile(join(root, "triage.json"), "not-json-sensitive-content");
  await assert.rejects(loadSharedConfig(directory), error => /not valid JSON/.test(error.message) && !error.message.includes("sensitive"));
});


test("default shared directory follows explicit Triage and Pi agent configuration", () => {
  assert.equal(defaultSharedDirectory({ TRG_PI_SHARED_CONFIG_DIR: "/fixture/triage" }, "/home").href, "file:///fixture/triage/");
  assert.equal(defaultSharedDirectory({ PI_CODING_AGENT_DIR: "/fixture/agent" }, "/home").href, "file:///fixture/agent/");
  assert.equal(defaultSharedDirectory({}, "/fixture/home").href, "file:///fixture/home/.pi/agent/");
});
