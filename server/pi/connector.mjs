import {
  DRY, LIVE, PROVIDER, Journal, TrgError, guardedFetch, outputMessage, payload, textOf, validateText,
} from "./core.mjs";
import { createVoiceCapture } from "./voice.mjs";

const COMPAT = Object.freeze({
  supportsStore: false, supportsDeveloperRole: false, supportsReasoningEffort: false,
  supportsUsageInStreaming: false, supportsFinishReason: true, supportsStrictMode: false,
  sendSessionAffinityHeaders: false, maxTokensField: "max_tokens",
});
const selected = ctx => ctx.model?.provider === PROVIDER;

/** SDK dependencies are injected so the state machine can be tested without a global Pi runtime. */
export function installConnector(pi, { openAICompletionsApi, createAssistantMessageEventStream, ...voiceUi }, config) {
  const journal = new Journal(config);
  const voice = createVoiceCapture(pi, voiceUi);
  const adapter = openAICompletionsApi();
  let pending, awaiting, active, last;
  let busy = false;
  let generation = 0;
  // Only context() can mint one of these. Nothing serializable is an authority.
  // Pi 1.3.0 convertToLlm preserves ordinary UserMessage object identity. If a
  // future release clones it, authorization fails closed (the integration test fails).
  let capabilities = new WeakMap();
  const counts = { submissions: 0, requests: 0, blocked: 0 };
  const reset = () => {
    generation++;
    voice.cancel("Voice canceled: model or session lifecycle changed");
    pending = awaiting = active = undefined; busy = false; capabilities = new WeakMap();
  };
  const notify = (ctx, message, level = "warning") => {
    if (ctx.hasUI) ctx.ui.notify(message, level);
    else if (["print", "json"].includes(ctx.mode)) {
      // A handled input otherwise exits successfully (and may print an old reply).
      process.stderr.write(`[Triage] ${message}\n`);
      process.exitCode = 1;
    }
  };

  function streamSimple(model, context, options = {}) {
    const stream = createAssistantMessageEventStream();
    // Pi 1.3 carries the system prompt and tool-set changes as role:"system"
    // transcript messages and adds them back after context(). Triage never sends
    // them (the wire payload is rebuilt from the journal record), so the one
    // capability-bearing utterance is counted among the other messages only.
    const conversation = context.messages.filter(message => message.role !== "system");
    const message = conversation.length === 1 ? conversation[0] : undefined;
    const claim = message && capabilities.get(message);
    if (message) capabilities.delete(message); // one provider invocation per context capability
    const record = claim?.record;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), config.timeoutMs);
    const signal = AbortSignal.any([controller.signal, ...(options.signal ? [options.signal] : [])]);
    (async () => {
      try {
        if (!claim || claim.piSessionId !== options.sessionId || record.model !== model.id ||
            (record.model === LIVE && !config.allowLive) || textOf(message) !== record.text) {
          counts.blocked++;
          throw new TrgError("Triage blocked an auxiliary or unclassified request; no command was submitted.");
        }
        counts.requests++;
        // Already fsynced in input(), before agent processing/network admission.
        const inner = adapter.streamSimple({
          ...model, api: "openai-completions", baseUrl: config.baseUrl,
          compat: COMPAT, samplingParams: undefined, headers: undefined,
        }, { messages: [{ role: "user", content: record.text, timestamp: message.timestamp }] }, {
          apiKey: "trg-transport-resolves-credential", signal,
          cacheRetention: "none", maxTokens: config.maxTokens,
          timeoutMs: config.timeoutMs, maxRetries: 1, maxRetryDelayMs: 2000,
          // Final wire payload is intentionally independent of unrelated model hooks.
          onPayload: () => payload(record, config.maxTokens),
          fetch: guardedFetch(config, record, signal),
        });
        let finished = false;
        for await (const event of inner) {
          if (event.type.startsWith("toolcall") || event.type === "thinking_delta" ||
              event.type === "error" ||
              (event.type === "done" && !["stop", "length"].includes(event.reason))) {
            throw new Error("Unexpected or failed provider stream");
          }
          if (event.type === "done") finished = true;
          stream.push(event);
        }
        if (!finished) throw new Error("Incomplete provider stream");
      } catch (error) {
        const result = outputMessage(model);
        result.stopReason = options.signal?.aborted ? "aborted" : "error";
        result.errorMessage = !claim && error instanceof TrgError ? error.message :
          `Triage request failed; acceptance may be unknown. Reuse /trg-retry ${record?.turnId ?? "<turn UUID>"}; do not retype the command.`;
        stream.push({ type: "error", reason: result.stopReason, error: result });
      } finally {
        clearTimeout(timeout);
        controller.abort();
        stream.end();
      }
    })();
    return stream;
  }

  pi.registerProvider(PROVIDER, {
    name: "Triage", baseUrl: config.baseUrl,
    // Non-secret availability sentinel. Actual auth is exclusively file/env indirection
    // in guardedFetch, reread for each retry (no /login or shell command evaluation).
    apiKey: "trg-transport-resolves-credential",
    api: "triage-chat-completions", streamSimple,
    models: [DRY, ...(config.allowLive ? [LIVE] : [])].map(id => ({
      id, name: id === LIVE ? "Triage (live dispatch)" : "Triage (dry run)",
      reasoning: false, input: ["text"], maxTokens: config.maxTokens,
      contextWindow: 32768, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      compat: COMPAT,
    })),
  });

  pi.on("session_start", reset);
  pi.on("session_shutdown", () => { reset(); voice.dispose(); });
  pi.on("model_select", reset);
  pi.on("agent_settled", reset);

  pi.on("input", async (event, ctx) => {
    if (!selected(ctx)) return;
    // Queued messages have no stable admission callback linking input to a user
    // entry in this SDK. Reject, don't guess by text or overwrite a running claim.
    if (busy || !ctx.isIdle() || event.streamingBehavior) {
      notify(ctx, "Triage does not queue commands. Wait for this turn to settle, then submit again.");
      return { action: "handled" };
    }
    busy = true; // also covers asynchronous preflight before Pi marks itself busy
    active = pending = awaiting = undefined;
    const epoch = generation;
    const sessionId = ctx.sessionManager.getSessionId(), modelId = ctx.model.id;
    const isCurrent = () => {
      try { return generation === epoch && selected(ctx) && ctx.model.id === modelId && ctx.sessionManager.getSessionId() === sessionId; }
      catch { return false; }
    };
    try {
      if (event.images?.length) throw new TrgError("Triage is text-only; transcribe audio in the Pi client first.");
      validateText(event.text);
      if (!["interactive", "rpc"].includes(event.source)) {
        throw new TrgError("Triage blocked extension-generated input. Use editor submission or an explicit user RPC prompt.");
      }
      // Both directives remain inside actual USER input. No sendUserMessage()
      // exception or global "voice is active" exemption for extension input.
      let text = event.text;
      const voiceInput = /^\/trg-voice(?:\s|$)/.test(text);
      if (voiceInput) {
        const args = text.slice("/trg-voice".length).trim();
        if (args && args !== "draft") throw new TrgError("Use /trg-voice or /trg-voice draft.");
        const result = await voice.capture(ctx, isCurrent, { draft: args === "draft" });
        if (!isCurrent()) return { action: "handled" };
        if (result.status !== "submitted") {
          busy = false;
          notify(ctx, result.reason, result.status === "error" ? "warning" : "info");
          return { action: "handled" };
        }
        text = result.text;
        if (args === "draft") {
          if (ctx.ui.getEditorText()) throw new TrgError("Editor changed during voice capture; transcript was not inserted or routed.");
          ctx.ui.setEditorText(text);
          busy = false;
          return { action: "handled" }; // the next actual Enter authorizes routing
        }
      }
      // Intercept as input, not registerCommand + fire-and-forget sendUserMessage:
      // print mode can shut down while that asynchronous message is still starting.
      const retry = !voiceInput && /^\/trg-retry(?:\s|$)/.test(text);
      const record = retry
        ? await journal.read(text.slice("/trg-retry".length).trim())
        : await journal.create(sessionId, modelId, text);
      // Session/model may change while capture or disk persistence is pending.
      if (!isCurrent()) return { action: "handled" };
      if (record.model !== modelId) throw new TrgError("Select the original Triage model before retrying; dry/live replay cannot change mode.");
      pending = { record, piSessionId: sessionId };
      if (!retry) counts.submissions++;
      last = record;
      return retry || voiceInput ? { action: "transform", text: record.text } : { action: "continue" };
    } catch (error) {
      if (!isCurrent()) return { action: "handled" };
      pending = undefined;
      busy = false;
      counts.blocked++;
      notify(ctx, error instanceof TrgError ? error.message : "Triage could not persist the turn; nothing was submitted.");
      return { action: "handled" };
    }
  });

  pi.on("before_agent_start", (event, ctx) => {
    if (!selected(ctx)) return;
    // Fail closed on template/skill/input transforms; route the actual human text,
    // not generated instructions. Another handler may run after us; message_end
    // and context independently verify what will be sent.
    awaiting = pending && pending.record.text === event.prompt ? pending : undefined;
    pending = undefined;
    if (!awaiting) notify(ctx, "Triage input was transformed or unclassified; dispatch is blocked.");
  });
  pi.on("message_end", (event, ctx) => {
    if (!selected(ctx) || event.message.role !== "user") return;
    active = awaiting && textOf(event.message) === awaiting.record.text
      ? { ...awaiting, timestamp: event.message.timestamp } : undefined;
    awaiting = undefined;
    if (active) {
      pi.appendEntry("trg-turn", { version: 1, turnId: active.record.turnId, sessionId: active.record.sessionId });
    }
  });
  pi.on("context", (event, ctx) => {
    if (!selected(ctx)) return;
    const user = event.messages.findLast(message => message.role === "user");
    if (!active || !user || user.timestamp !== active.timestamp || textOf(user) !== active.record.text) return;
    const onlyUser = { role: "user", content: active.record.text, timestamp: user.timestamp };
    capabilities.set(onlyUser, active);
    // Triage routes one utterance, not a coding transcript. Omit system prompt,
    // summaries, tool definitions/results, and injected follow-up messages.
    return { messages: [onlyUser] };
  });

  pi.on("session_before_compact", (_event, ctx) => {
    if (!selected(ctx)) return;
    notify(ctx, "Triage cannot compact. No command was submitted; use a separate summarization model/session.");
    return { cancel: true };
  });
  pi.on("session_before_tree", (event, ctx) => {
    if (selected(ctx) && event.preparation.userWantsSummary) {
      notify(ctx, "Triage cannot summarize branches. Navigate without a summary instead.");
      return { cancel: true };
    }
  });
  pi.on("session_tree", reset);

  pi.registerCommand("trg-status", {
    description: "Show the last Triage client turn identity and bounded counters (no prompt or credential)",
    handler: async (_args, ctx) => {
      const status = { version: 1, ...counts, turnId: last?.turnId, sessionId: last?.sessionId, model: last?.model };
      pi.sendMessage({ customType: "trg-status", content: JSON.stringify(status), display: true }, { triggerTurn: false });
    },
  });
}
