# Optional integrations

The core server needs only Python, FFmpeg, Node.js for the bridge, a transcription
provider and a webhook or custom actions. The integrations below are optional.

## Agent Utils

Agent Utils provides two optional adapters:

- MAI transcription through `extensions/lib/realtime-stt-batch.js`
- Azure speech synthesis through `extensions/lib/tts.js`

Point `transcription.agent_utils_dir` or `commands.speech.agent_utils_dir` at an
Agent Utils checkout or Nix store path. Pebble does not import Agent Utils unless one
of those providers is selected.

The Pi Triage connector can also integrate with Agent Utils push-to-talk events when
both extensions are loaded.

## Cacophony actions

Enable `services.pebble.commands.caco.enable` to expose these built-in actions:

| Action | Effect |
| --- | --- |
| `pebble` | Send the request to one configured assistant target |
| `bd-file` | Create one bead in a selected project |
| `bd-expand` | Expand a request into planned beads |
| `agent-msg` | Send text to one active agent |
| `broadcast` | Send text to one project or every project |

The adapter calls the configured `caco` CLI with structured argv and stdin. It
requires `project`, `target`, a Caco daemon URL and a Caco credential file. Caco
results are parsed as receipts. A missing or invalid receipt is indeterminate and is
not retried as a fresh action.

## Routing context

Caco actions need a project and agent inventory:

- `context.source = "ctx"` follows a ctx key and writes a bounded local snapshot.
- `context.source = "caco"` polls the Caco API on a timer.
- `context.source = "none"` is for custom-action-only Jev routes.

## Tailscale Funnel

`services.pebble.funnel.enable` adds only Pebble paths to an existing HTTPS Funnel.
It does not reset Funnel, replace its root handler or overwrite another backend's
path. Command-mode paths are derived from configured routes, action prefixes and
status endpoints unless `funnel.paths` is set explicitly.

## Pi Triage connector

`server/pi/index.ts` adds a guarded `triage` provider to Pi. It reads `models.json`
and `triage.json` from:

1. `TRG_PI_SHARED_CONFIG_DIR`
2. `PI_CODING_AGENT_DIR`
3. `~/.pi/agent`

See [`examples/pi`](../examples/pi). The example starts with live dispatch disabled.

## Terminal clients

`pebble-route` sends text, a recording or a local Pulse capture to a Pebble endpoint.
Set `PEBBLE_ROUTE_URL` and `PEBBLE_ROUTE_TOKEN_FILE` for your deployment.

`trg`, the Triage terminal client, uses the `/route` API. It is distributed
separately.
