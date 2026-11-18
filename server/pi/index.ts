import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, openAICompletionsApi } from "@earendil-works/pi-ai";
import { isKeyRelease, matchesKey, truncateToWidth } from "@earendil-works/pi-tui";
import { installConnector } from "./connector.mjs";
import { loadConfig, PROVIDER } from "./core.mjs";
import { loadSharedConfig, sharedModelMatches } from "./shared-config.mjs";

export default async function (pi: ExtensionAPI) {
  // Explicit isolated profiles retain their existing strict validation and mode.
  const shared = process.env.TRG_PI_CONFIG === undefined;
  const config = shared ? await loadSharedConfig() : await loadConfig(process.env.TRG_PI_CONFIG);
  if (shared) {
    pi.on("input", (_event, ctx) => {
      if (ctx.model?.provider !== PROVIDER || sharedModelMatches(ctx.model, config)) return;
      const message = "Triage endpoint/API changed. Run /reload before submitting; nothing was sent.";
      if (ctx.hasUI) ctx.ui.notify(message, "warning");
      else if (["print", "json"].includes(ctx.mode)) {
        process.stderr.write(`[Triage] ${message}\n`);
        process.exitCode = 1;
      }
      return { action: "handled" };
    });
  }
  installConnector(pi, { createAssistantMessageEventStream, openAICompletionsApi, isKeyRelease, matchesKey, truncateToWidth }, config);
}
