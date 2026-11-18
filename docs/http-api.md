# HTTP API

All endpoints require exactly one `Authorization: Bearer TOKEN` header. The token
comes from `token_file`. Tokens in query strings are not supported.

## Inputs

Pebble accepts:

- `application/json` text: `{ "transcription": "...", "recordedAt": "...", "client": "..." }`
- multipart text with the same fields
- multipart audio in an `audio` part, with optional device transcription

Supported audio types are `audio/wav`, `audio/x-wav`, `audio/mp4`, `audio/m4a` and
`audio/x-m4a`. Remote audio URLs and base64 audio are rejected.

`recordedAt` is a millisecond timestamp string or integer. Webhook mode requires it
for both audio and text so retries deduplicate. Command mode can also use an
`Idempotency-Key` header.

A connection probe sends `{"test": true}`. It authenticates and reports
capabilities, but archives nothing and runs no transcription, Jev call, action or
speech.

## Webhook mode

When command routing is disabled, `POST /` and
`POST /hooks/global/pebble-audio` accept audio or text. Audio is transcribed and
forwarded. Text skips transcription and is forwarded unchanged.

Responses:

- `202` with `phase: queued` for audio or `phase: ready` for text
- `200` for a connection probe
- `400` for invalid input
- `401` for authentication failure
- `503` when storage, the token or the worker is unavailable

## Command mode

Each configured route accepts audio or text. A route can run Jev triage or a fixed
action. Custom actions also have these fixed paths:

```text
/pebble/action/NAME
/hooks/global/pebble/action/NAME
```

Use `X-Pebble-Dry-Run: true` or `?dry_run=true` for a full preview with no action
command. Changing the dry-run mode or route profile for an existing idempotency key
returns `409`.

The response is an admission receipt, not proof the action completed:

```json
{
  "id": "32 hex characters",
  "phase": "ready",
  "duplicate": false,
  "dry_run": false,
  "archived": true
}
```

## Status

| Method | Path | Meaning |
| --- | --- | --- |
| `GET` | `/health` | Private receiver health and capabilities |
| `GET` | `/receipts/ID` | Receipt for one command-mode request |
| `GET` | `/pebble/status` | Bounded status snapshot |
| `GET` | `/pebble/events` | Server-sent status snapshots |

Status endpoints never call Jev, run actions, read secrets or repeat speech.

## Triage API

The `/route` API supports interactive clients such as `trg` and the Pi connector.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/route/capabilities` | Implemented feature inventory |
| `POST` | `/route/sessions` | Create or reuse a client session |
| `GET` | `/route/sessions/ID` | Recent turns in a session |
| `POST` | `/route/sessions/ID/turns` | Submit a dry-run, live, inspect or approval turn |
| `GET` | `/route/turns/ID` | Durable turn receipt and events |
| `GET` | `/route/turns/ID/events?after=N` | JSON events or native SSE |
| `POST` | `/route/turns/ID/cancel` | Cancel a turn before dispatch |
| `GET` | `/route/v1/models` | Chat-compatible model list |
| `POST` | `/route/v1/chat/completions` | Chat-compatible Triage interface |

Dry runs return proposal revisions. A live approval must name the exact preview turn
and revision. A changed or consumed preview cannot be approved.
