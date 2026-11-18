import { randomUUID } from "node:crypto";
import { LIVE, TrgError, validateText } from "./core.mjs";

// AgentUtils' existing extensions/lib/input-actions.js protocol. Keep these
// literal wire names here; do not import/initialize a second realtime extension.
export const INPUT_EVENT = "agent-utils:input-action";
export const CONTROLS_EVENT = "realtime:controls";
export const VOICE_ACTIONS = Object.freeze({
  enter: "freeform-enter", update: "freeform-update", submit: "freeform-submit",
  cancel: "freeform-cancel", commit: "freeform-ptt-commit",
});
export const VOICE_LIMITS = Object.freeze({ captureMs: 60000, finalizeMs: 15000 });

/** One command-owned capture. The bus supplies text, never routing authority. */
export function createVoiceCapture(pi, ui = {}, timers = {}) {
  const setTimer = timers.setTimer ?? setTimeout, clearTimer = timers.clearTimer ?? clearTimeout;
  let controls, active;
  // AgentUtils emits this once from its factory: load Triage BEFORE realtime-agent.
  const offControls = pi.events?.on(CONTROLS_EVENT, value => { controls = value; });

  const cancel = (reason = "Voice capture canceled") => active?.finish({ status: "cancelled", reason });
  const dispose = () => { cancel(); if (typeof offControls === "function") offControls(); controls = undefined; };

  async function capture(ctx, isCurrent, { draft = false } = {}) {
    if (active) throw new TrgError("Triage voice capture is already active.");
    if (ctx.mode !== "tui" || !ctx.ui?.custom || !ctx.ui?.onTerminalInput || !ui.matchesKey || !ui.truncateToWidth) {
      throw new TrgError("/trg-voice requires the interactive Pi terminal UI.");
    }
    if (!controls?.snapshot || !pi.events?.on || !pi.events?.emit) {
      throw new TrgError("Load Triage before AgentUtils realtime-agent, then use /trg-voice.");
    }
    let state;
    try { state = controls.snapshot(); } // read only; never call enable/disable/setModel
    catch { throw new TrgError("AgentUtils status unavailable; no recording started."); }
    if (state?.previousModel || state?.state?.connection !== "off" || state?.state?.micMode) {
      throw new TrgError("Stop AgentUtils realtime first, then select Triage and use /trg-voice.");
    }
    if (!isCurrent()) return { status: "cancelled", reason: "Voice target changed" };

    return new Promise(resolve => {
      const record = {
        nonce: randomUUID(), phase: "listening", text: "", finished: false,
        entered: false, pasted: false, suppressEnter: false,
      };
      active = record;
      const emit = action => pi.events.emit(INPUT_EVENT, { ...action, sessionId: record.nonce });
      const current = () => {
        try { return !record.finished && active === record && isCurrent(); } catch { return false; }
      };
      const finish = result => {
        if (record.finished) return;
        record.finished = true;
        if (active === record) active = undefined;
        clearTimer(record.timer);
        // Invalidate the upstream generation BEFORE restoring editor focus. During
        // final transcription the bus cannot abort STT, but capture has stopped
        // and all late results are ignored. No fallback to generic sendUserMessage.
        if (record.entered) {
          try { emit({ action: VOICE_ACTIONS.cancel, source: "trg-voice", reason: "owner-closed" }); } catch {}
        }
        for (const cleanup of [record.offInput, record.offTerminal, () => record.done?.(null)]) {
          try { if (typeof cleanup === "function") cleanup(); } catch {}
        }
        resolve(result);
      };
      record.finish = finish;
      const deadline = (ms, reason) => {
        clearTimer(record.timer);
        record.timer = setTimer(() => finish({ status: "error", reason }), ms);
      };
      deadline(VOICE_LIMITS.captureMs, "Voice capture timed out; nothing submitted");

      const input = event => {
        if (!current()) { finish({ status: "cancelled", reason: "Voice target changed" }); return; }
        if (event?.sessionId !== record.nonce || event?.source !== "ptt") return;
        if (event.action === VOICE_ACTIONS.cancel) {
          finish({ status: "cancelled", reason: "No voice transcript; nothing submitted" });
        } else if (event.action === VOICE_ACTIONS.update) {
          // Updates are display-only, never a fallback final utterance.
          if (typeof event.text !== "string" || Buffer.byteLength(event.text) > 16384) {
            finish({ status: "error", reason: "Voice transcript exceeded the text limit" }); return;
          }
          record.text = event.text;
          record.render?.();
        } else if (event.action === VOICE_ACTIONS.submit && record.phase === "finishing") {
          try {
            const text = validateText(event.text).trim();
            finish({ status: "submitted", text, nonce: record.nonce });
          } catch {
            finish({ status: "error", reason: "Invalid voice transcript; nothing submitted" });
          }
        }
      };
      const key = data => {
        if (!current()) { finish({ status: "cancelled", reason: "Voice target changed" }); return; }
        if (ui.isKeyRelease?.(data)) return;
        // Cancellation remains available even after an unterminated paste.
        if (ui.matchesKey(data, "escape") || ui.matchesKey(data, "ctrl+c")) {
          finish({ status: "cancelled", reason: "Voice capture canceled" }); return;
        }
        // Like interactive_choice: pasted bytes and the following Enter cannot
        // commit a microphone recording or leak into the underlying editor.
        if (record.pasted) {
          if (data.includes("\x1b[201~")) { record.pasted = false; record.suppressEnter = true; }
          return;
        }
        if (data.includes("\x1b[200~")) {
          record.pasted = !data.includes("\x1b[201~"); record.suppressEnter = true; return;
        }
        const enter = ui.matchesKey(data, "enter");
        if (record.suppressEnter && enter) { record.suppressEnter = false; return; }
        if (record.phase === "listening" && (enter || data === " ")) {
          record.phase = "finishing"; // only this LOCAL key grants final-result authority
          deadline(VOICE_LIMITS.finalizeMs, "Voice transcription timed out; nothing submitted");
          record.render?.();
          try { emit({ action: VOICE_ACTIONS.commit, source: "keyboard" }); }
          catch { finish({ status: "error", reason: "Voice commit unavailable; nothing submitted" }); }
        }
      };

      void ctx.ui.custom((tui, theme, _keybindings, done) => {
        if (!current()) { finish({ status: "cancelled", reason: "Voice target changed" }); done(null); return { render: () => [], invalidate() {}, handleInput() {} }; }
        record.done = done;
        record.render = () => tui.requestRender();
        record.offInput = pi.events.on(INPUT_EVENT, input);
        // Pi's raw listeners run BEFORE focused custom components. Register this
        // BEFORE FREEFORM_ENTER so AgentUtils' later hold-release listener cannot
        // consume Enter/Esc or run finalizeHeldToEditor/generic STT submission.
        record.offTerminal = ctx.ui.onTerminalInput(data => { key(data); return { consume: true }; });
        if (typeof record.offInput !== "function" || typeof record.offTerminal !== "function") {
          finish({ status: "error", reason: "Voice input ownership is unavailable" });
        } else {
          record.entered = true;
          emit({ action: VOICE_ACTIONS.enter, mode: "ptt", source: "trg-voice" });
        }
        return {
          render(width) {
            if (width < 1) return [];
            const mode = draft ? "draft" : ctx.model?.id === LIVE ? "LIVE" : "dry run";
            const title = `Triage voice · ${record.phase === "finishing" ? "Transcribing" : "Speak"} · ${mode}`;
            const hint = record.phase === "finishing" || width < 24 ? "Esc cancel" : width < 60
              ? `Esc cancel · Enter ${draft ? "draft" : "route"}`
              : `Esc cancel · Enter/Space ${draft ? "finish to editor" : "finish and route"}`;
            // No ANSI/OSC/control sequences from transcripts enter terminal output.
            const preview = record.text.replace(/[\x00-\x1f\x7f-\x9f]/g, " ").slice(-500);
            return [theme.fg("accent", title), preview || "…", theme.fg("muted", hint)]
              .map(line => ui.truncateToWidth(line, width));
          },
          invalidate() {},
          handleInput: key,
        };
      }, { overlay: true, overlayOptions: { anchor: "bottom-left", col: 0, width: "100%", maxHeight: "100%", margin: 0 } })
        .then(() => { if (!record.finished) finish({ status: "cancelled", reason: "Voice dialog closed" }); })
        .catch(() => finish({ status: "error", reason: "Voice UI unavailable; nothing submitted" }));
    });
  }
  return { capture, cancel, dispose, isActive: () => Boolean(active) };
}
