import assert from "node:assert/strict";
import { installConnector } from "./connector.mjs";
import { DRY, outputMessage } from "./core.mjs";
import { CONTROLS_EVENT, INPUT_EVENT, VOICE_ACTIONS as A } from "./voice.mjs";

export function streamQueue() {
  const queue = []; let ended = false, wake;
  return {
    push(event) { queue.push(event); wake?.(); }, end() { ended = true; wake?.(); },
    async *[Symbol.asyncIterator]() {
      while (!ended || queue.length) {
        if (queue.length) yield queue.shift();
        else await new Promise(resolve => { wake = resolve; });
      }
    },
  };
}
export const uiHelpers = {
  matchesKey: (data, key) => ({ enter: ["\r", "\n"], escape: ["\x1b"], "ctrl+c": ["\x03"] }[key] ?? []).includes(data),
  isKeyRelease: data => /:3u$/.test(data),
  truncateToWidth: (text, width) => [...text].slice(0, width).join(""),
};
export function makeSpeechHarness(config) {
  const hooks = new Map(), bus = new Map(), terminal = new Set(), providers = new Map(), commands = new Map();
  const h = { requests: [], events: [], notifications: [], editor: "", modelChanges: [], entryData: [], genericSends: [], terminal, components: [], widgets: new Map(), renders: 0 };
  const ctx = h.ctx = {
    mode: "tui", hasUI: true, model: { provider: "triage", id: DRY }, isIdle: () => true,
    sessionManager: { getSessionId: () => "speech-test-session", getBranch: () => [], getEntries: () => [], getLeafId: () => "leaf" },
    modelRegistry: { find: () => undefined },
    ui: {
      custom: async factory => new Promise(resolve => {
        h.component = factory({ requestRender: () => { h.renders++; } }, { fg: (_role, text) => text }, {}, () => { h.component = null; resolve(null); });
        h.components.push(h.component);
      }),
      onTerminalInput: handler => { terminal.add(handler); return () => terminal.delete(handler); },
      notify: message => h.notifications.push(message),
      getEditorText: () => h.editor,
      setEditorText: text => { h.editor = text; },
      setStatus() {},
      setWidget: (name, value) => { if (value === undefined) h.widgets.delete(name); else h.widgets.set(name, value); },
    },
  };
  const pi = h.pi = {
    on: (name, fn) => { if (!hooks.has(name)) hooks.set(name, []); hooks.get(name).push(fn); },
    events: {
      on(name, fn) { if (!bus.has(name)) bus.set(name, new Set()); bus.get(name).add(fn); return () => bus.get(name).delete(fn); },
      off: (name, fn) => bus.get(name)?.delete(fn),
      emit(name, event) { h.events.push({ name, event }); for (const fn of [...bus.get(name) ?? []]) fn(event); },
    },
    registerProvider: (name, provider) => providers.set(name, provider),
    unregisterProvider: name => providers.delete(name),
    registerCommand: (name, cmd) => commands.set(name, cmd),
    registerMessageRenderer() {}, registerTool() {},
    appendEntry: (_type, data) => h.entryData.push(data),
    sendMessage() {}, sendUserMessage: text => h.genericSends.push(text),
    setModel: async model => { h.modelChanges.push(model); ctx.model = model; return true; },
  };
  installConnector(pi, {
    ...uiHelpers, createAssistantMessageEventStream: streamQueue,
    openAICompletionsApi: () => ({ streamSimple(model, context, options) {
      h.requests.push({ model, context, options });
      const stream = streamQueue(), message = outputMessage(model);
      message.stopReason = "stop";
      stream.push({ type: "done", reason: "stop", message }); stream.end(); return stream;
    } }),
  }, config);
  h.emit = async (name, event = {}) => {
    let result;
    for (const fn of hooks.get(name) ?? []) {
      result = await fn(event, ctx);
      if (name === "input" && result?.action === "handled") return result;
      if (name === "input" && result?.action === "transform") event = { ...event, text: result.text };
      if (name === "context" && result?.messages) event = { ...event, messages: result.messages };
    }
    return name === "context" ? event : result;
  };
  h.key = data => {
    for (const fn of terminal) if (fn(data)?.consume) return true;
    h.component?.handleInput(data); return false;
  };
  h.prompt = async (text, source = "interactive") => {
    const result = await h.emit("input", { source, text });
    if (result?.action === "handled") return result;
    if (result?.action === "transform") text = result.text;
    await h.emit("before_agent_start", { prompt: text });
    const message = { role: "user", content: text, timestamp: Date.now() };
    await h.emit("message_end", { message });
    const context = await h.emit("context", { messages: structuredClone([message]) });
    const events = await Array.fromAsync(providers.get("triage").streamSimple(ctx.model, context, { sessionId: ctx.sessionManager.getSessionId() }));
    await h.emit("agent_settled");
    return events;
  };
  h.listenerCount = name => bus.get(name)?.size ?? 0;
  return h;
}

// Deterministic bus peer; real AgentUtils handler tests are a separate process.
export function fakeSpeech(h, options = {}) {
  let nonce, offRelease;
  const peer = { captures: 0, commits: 0, cancels: 0, genericReleases: 0, nonce: () => nonce };
  h.pi.events.emit(CONTROLS_EVENT, { snapshot: () => ({ previousModel: null, state: { connection: "off", micMode: null }, ...options.snapshot }) });
  h.pi.events.on(INPUT_EVENT, event => {
    if (event.action === A.enter) {
      nonce = event.sessionId; peer.captures++;
      offRelease = h.ctx.ui.onTerminalInput(() => { peer.genericReleases++; return { consume: true }; });
      options.enter?.(event);
    } else if (event.sessionId === nonce && event.action === A.cancel) {
      peer.cancels++; offRelease?.();
    } else if (event.sessionId === nonce && event.action === A.commit) {
      peer.commits++; offRelease?.(); options.commit?.(event);
    }
  });
  peer.send = (action, text, changes = {}) => h.pi.events.emit(INPUT_EVENT, { action, sessionId: nonce, source: "ptt", text, ...changes });
  return peer;
}
export async function waitFor(predicate, limitMs = 2000) {
  const end = Date.now() + limitMs;
  while (!predicate() && Date.now() < end) await new Promise(resolve => setTimeout(resolve, 2));
  assert.ok(predicate(), "bounded wait expired");
}
