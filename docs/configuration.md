# Configuration

The Home Manager module writes a version-2 JSON file and starts the receiver as a
systemd user service. You can also run `pebble-server serve --config FILE`
directly.

Version-1 files are still accepted. They are upgraded in memory to the version-2
shape, and old stored requests are upgraded when they are retried.

## Receiver

| Field | Meaning |
| --- | --- |
| `listen` | Loopback host, port, request timeout and connection limit. The host must be `127.0.0.1`. |
| `token_file` | Owner-private bearer token accepted by every HTTP endpoint. |
| `state_dir` | Private durable archive. Keep it out of synchronised or public folders. |
| `max_body_bytes`, `max_pending`, `max_archive_bytes`, `min_free_bytes` | Admission and storage limits. |
| `retry_delays_secs` | Retry delays for retryable transcription or webhook failures. |
| `ffmpeg`, `node` | Runtime executables. The Nix module fills these in. |
| `chime` | Optional immediate upload sound. `server` may be empty to use the local Pulse default. |

## Transcription

Every audio recording is converted to 24 kHz mono WAV before transcription.

### Command provider

```json
{
  "provider": "command",
  "model": "whisper-1",
  "command": ["pebble-transcribe"],
  "output": "json",
  "language": "en",
  "timeout_secs": 60,
  "max_audio_secs": 120
}
```

The command receives WAV bytes on stdin. With `output: "text"`, stdout is the
transcript. With `output: "json"`, stdout must be an object with a non-empty
`text` field. The service environment is inherited, including any variables loaded
from `providerSecretFiles`.

A non-zero exit or timeout is retryable. Invalid output or an empty transcript
blocks the input for inspection rather than falling back to device text.

### Agent Utils MAI provider

```json
{
  "provider": "agent-utils-mai",
  "model": "mai-transcribe-2",
  "agent_utils_dir": "/path/to/agent-utils",
  "endpoint_file": "/run/user/1000/secrets/mai-endpoint",
  "key_file": "/run/user/1000/secrets/mai-key",
  "deployment": "my-mai-transcribe-2",
  "language": "en",
  "timeout_secs": 60,
  "max_audio_secs": 120
}
```

This calls `transcribeMaiAudioDirect` from Agent Utils. Set
`AZURE_SPEECH_AUTH=entra` and the Agent Utils broker variables to use Entra rather
than the key file.

## Webhook forwarding

When `commands.enabled` is false, Pebble forwards each transcript or typed text
request to one webhook:

```json
{
  "url": "https://hooks.example.invalid/pebble",
  "timeout_secs": 30,
  "auth": "bearer-file",
  "token_file": "~/.config/pebble/webhook.token",
  "receipt": "http-2xx"
}
```

`auth` is one of:

- `none`: send no `Authorization` header.
- `bearer-ingress`: reuse Pebble's ingress token.
- `bearer-file`: read a separate owner-private token file.

`receipt` is one of:

- `http-2xx`: any 2xx response is success.
- `caco-hook`: require Cacophony's webhook result envelope and a queued, delivered,
  blocked, indeterminate or skipped receipt.

The JSON body is:

```json
{
  "transcription": "text",
  "recordedAt": "1767268800000",
  "client": "phone",
  "audio_capture_id": "32 hex characters",
  "transcript_source": "whisper-1"
}
```

Typed text uses `transcript_source: "client-text"`.

## Command routing

Set `commands.enabled` to route through Jev and actions instead of forwarding.

| Field | Meaning |
| --- | --- |
| `routes` | Map from HTTP path to `{ "action": ..., "profile": ... }`. |
| `jev` | Default Jev executable, provider, model and timeouts. |
| `jev_profiles` | Named overrides of provider, model, timeouts and allowed custom actions. |
| `actions`, `actions_file` | Trusted custom action definitions. The file is re-read for each new request. |
| `extractor` | OpenAI-compatible payload extraction before custom actions. |
| `context_file` | Optional routing context. Leave it empty for custom-action-only routing. |
| `speech` | Optional spoken acknowledgements and receipts. |
| `caco` | Optional Cacophony adapter. Disabled in public examples. |

A route action is `route` for Jev triage, `custom:NAME` for a fixed custom action,
or a Caco action when the Caco adapter is enabled.

```json
{
  "routes": {
    "/pebble/work": {"action": "route", "profile": "work"},
    "/pebble/note": {"action": "custom:note", "profile": "default"}
  },
  "jev_profiles": {
    "work": {
      "provider": "openrouter",
      "model": "openai/gpt-4.1",
      "actions": ["note", "reminder"]
    }
  }
}
```

The selected profile is stored with each admitted request. A retry after a
configuration change keeps the original profile, model and action set, while using
the currently installed executable paths.

## Provider environment

Use `providerEnvironment` for non-secret values and `providerSecretFiles` for keys:

```nix
services.pebble.providerEnvironment.OPENAI_BASE_URL = "https://api.openai.com/v1";
services.pebble.providerSecretFiles.OPENAI_API_KEY = "/run/user/1000/secrets/openai-api-key";
```

Secret files must be regular files owned by the service user and not readable by
group or other users. Plain environment names that look secret, such as
`OPENAI_API_KEY` or `WEBHOOK_TOKEN`, are rejected so keys do not enter the Nix store.

## Speech

Speech is disabled by default. Providers:

- `command`: receives text on stdin and voice details in
  `PEBBLE_SPEECH_VOICE`, `PEBBLE_SPEECH_SPEED` and `PEBBLE_SPEECH_EMBEDDING`.
- `aztts`: calls `aztts` and the configured player.
- `agent-utils-azure`: calls Agent Utils' Azure speech helper.
- `auto`: uses Agent Utils with `AZURE_SPEECH_AUTH=entra`, otherwise `aztts`.

Successful playback commands mean only that the command finished. They are not proof
that a listener heard the audio.
