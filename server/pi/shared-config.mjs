import { homedir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { readBounded, TrgError, validateConfig } from "./core.mjs";

export function defaultSharedDirectory(env = process.env, home = homedir()) {
  const directory = env.TRG_PI_SHARED_CONFIG_DIR ?? env.PI_CODING_AGENT_DIR ?? join(home, ".pi/agent");
  return pathToFileURL(directory.endsWith("/") ? directory : directory + "/");
}

async function readJson(file, limit) {
  try { return JSON.parse(await readBounded(fileURLToPath(file), limit)); }
  catch (error) {
    if (error instanceof TrgError) throw error;
    throw new TrgError("Shared Triage configuration is not valid JSON.");
  }
}

// Read only non-secret configuration at load time. Credentials are still read
// by guardedFetch, and no journal directory is created until actual user input.
export async function loadSharedConfig(directory = defaultSharedDirectory(), home = homedir()) {
  const models = await readJson(new URL("models.json", directory), 131072);
  const provider = models?.providers?.triage;
  if (provider?.api !== "triage-chat-completions" || provider.models !== undefined) {
    throw new TrgError("Shared Triage requires its guarded API and modelOverrides, not standalone models.");
  }
  const preferences = await readJson(new URL("triage.json", directory), 16384);
  if (!preferences || typeof preferences !== "object" || Array.isArray(preferences) || "baseUrl" in preferences) {
    throw new TrgError("Configure the shared Triage baseUrl only in models.json.");
  }
  const expandHome = value => typeof value === "string" && value.startsWith("~/")
    ? join(home, value.slice(2)) : value;
  return validateConfig({
    ...preferences, baseUrl: provider.baseUrl,
    tokenFile: expandHome(preferences.tokenFile), stateDir: expandHome(preferences.stateDir),
  });
}

// models.json refreshes on opening /model, but extension configuration changes
// require /reload. Never silently send to the previous endpoint during that gap.
export function sharedModelMatches(model, config) {
  try {
    return model.api === "triage-chat-completions" &&
      new URL(model.baseUrl).href.replace(/\/$/, "") === config.baseUrl;
  } catch { return false; }
}
