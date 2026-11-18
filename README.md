# Pebble

Pebble is a small, durable intake service for voice and text commands. It accepts
recordings or text from a phone, ring, watch, shortcut or terminal, archives them
before doing any work, transcribes audio with a provider you choose, and either:

- forwards the transcript to a webhook, or
- uses [Jev](https://github.com/tumf/jev-cli) to triage the request into one of
  your configured actions.

It is designed for personal automation where a lost or repeated request matters.
Every request has a receipt, retries keep the original routing decision, and an
uncertain action is never silently resent.

## What it does

- **One endpoint for audio and text.** Multipart audio, multipart text or JSON
  text all enter the same durable pipeline.
- **Bring your own transcription.** Use a command that reads WAV on stdin and
  prints text or JSON, or use the optional Agent Utils MAI adapter.
- **Generic webhook forwarding.** Send transcripts to any HTTP endpoint, with no
  auth, a separate bearer token or the ingress token.
- **Multiple Jev triage endpoints.** Each HTTP path can choose a Jev profile with
  its own provider, model, timeout and allowed actions.
- **Trusted custom actions.** Actions are operator-written argv or shell commands.
  Jev chooses only from their descriptions and never sees command text.
- **Payload extraction.** Before a custom action runs, an OpenAI-compatible model
  extracts the part of the request the action needs.
- **Dry runs, receipts and cancellation.** Preview routing without running an
  action, inspect each request and cancel work before dispatch.
- **Optional speech.** Speak acknowledgements and receipts through a command,
  `aztts`, or the optional Agent Utils Azure speech adapter.
- **Optional integrations.** Cacophony actions, ctx/Caco routing context,
  Tailscale Funnel and the Pi Triage connector are adapters, off by default.

## Quick start with Home Manager

Add the flake:

```nix
{
  inputs.pebble.url = "github:harryaskham/pebble";
}
```

Import the module and choose a transcription command and destination:

```nix
{inputs, pkgs, ...}: let
  transcribe = pkgs.writeShellApplication {
    name = "pebble-transcribe";
    runtimeInputs = [pkgs.curl];
    text = builtins.readFile ./openai-compatible.sh;
  };
in {
  imports = [inputs.pebble.homeManagerModules.default];

  services.pebble = {
    enable = true;
    runtimePackages = [transcribe];
    transcription = {
      provider = "command";
      model = "whisper-1";
      command = ["pebble-transcribe"];
      output = "json";
      language = "en";
      timeout_secs = 60;
      max_audio_secs = 120;
    };
    providerSecretFiles.OPENAI_API_KEY = "/run/user/1000/secrets/openai-api-key";
    forward = {
      url = "https://hooks.example.invalid/pebble";
      timeout_secs = 30;
      auth = "bearer-file";
      token_file = "/run/user/1000/secrets/pebble-webhook-token";
      receipt = "http-2xx";
    };
  };
}
```

Create the ingress token, which clients must send as `Authorization: Bearer ...`:

```bash
mkdir -p ~/.config/pebble
umask 077
openssl rand -hex 32 > ~/.config/pebble/ingress.token
```

After Home Manager activates the service, Pebble listens on
`http://127.0.0.1:11301`. Put it behind your own authenticated reverse proxy, VPN
or Tailscale Funnel if a remote device must reach it.

See [`examples/home-manager/webhook.nix`](examples/home-manager/webhook.nix) and
[`examples/transcription/openai-compatible.sh`](examples/transcription/openai-compatible.sh).

## Jev triage

Enable command mode, define actions and map paths to Jev profiles:

```nix
services.pebble.commands = {
  enable = true;
  jev.provider = "openrouter";
  jev.model = "openai/gpt-4.1-mini";
  actionsFile = "/home/me/.config/pebble/actions.yaml";
  routes = {
    "/pebble/work" = { action = "route"; profile = "work"; };
    "/pebble/home" = { action = "route"; profile = "home"; };
  };
  jevProfiles = {
    work = { model = "openai/gpt-4.1"; actions = ["note" "reminder"]; };
    home = { actions = ["automation" "reminder"]; };
  };
};
```

A request to `/pebble/work` can choose only `note` or `reminder`. A request to
`/pebble/home` can choose only `automation` or `reminder`. Each custom action also
has a fixed endpoint, `/pebble/action/NAME`, which skips Jev.

See [`examples/home-manager/triage.nix`](examples/home-manager/triage.nix) and
[`examples/actions.yaml`](examples/actions.yaml).

## Send a request

Text:

```bash
curl --fail \
  -H "Authorization: Bearer $(cat ~/.config/pebble/ingress.token)" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: shortcut-2026-01-01T12:00:00Z' \
  --data '{"transcription":"remind me to call the plumber tomorrow","recordedAt":"1767268800000","client":"shortcut"}' \
  http://127.0.0.1:11301/pebble/work
```

Audio:

```bash
curl --fail \
  -H "Authorization: Bearer $(cat ~/.config/pebble/ingress.token)" \
  -F transcription='' \
  -F recordedAt="$(date +%s000)" \
  -F client=phone \
  -F audio=@recording.m4a\;type=audio/mp4 \
  http://127.0.0.1:11301/pebble/home
```

Add `X-Pebble-Dry-Run: true` to preview routing without running an action.

## Documentation

- [Configuration](docs/configuration.md)
- [HTTP API](docs/http-api.md)
- [Custom actions](docs/actions.md)
- [Optional integrations](docs/integrations.md)
- [Security model](docs/security.md)
- [Development](docs/development.md)

## Repository layout

| Path | Contents |
| --- | --- |
| `server/` | Python receiver, durable archive, routing, actions and adapters |
| `server/pi/` | Optional Pi Triage connector |
| `scripts/` | `pebble-route`, `pebble-org-append` and `pebble-chime` |
| `nix/` | Package and Home Manager module |
| `examples/` | Generic Home Manager, JSON, action and transcription examples |
| `docs/` | User and integration documentation |

## Status

Pebble is personal infrastructure published as a reusable project. The core
server and Home Manager module run on Linux with systemd user services. The
Cacophony and ctx adapters are optional and require those separate systems.

## License

MIT
