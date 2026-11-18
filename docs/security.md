# Security model

Pebble controls actions with the authority of its service user. Treat its ingress
token like a password for that user.

## Boundaries

- The receiver binds only to `127.0.0.1`. Remote access must go through a proxy,
  VPN or Funnel that you control.
- Every endpoint requires one bearer header. Query-string tokens are rejected.
- Token and secret files must be owner-private regular files. Managed symlinks are
  allowed when the final file is private.
- Provider keys are loaded when the service starts. They are not written to config,
  archives, command arguments or receipts.
- Custom commands are operator configuration. Request text is passed on stdin, not
  interpolated into argv or shell code.
- Jev sees bounded action descriptions and optional routing context, never command
  text or credentials.

## Durability and retries

Pebble writes each accepted request to SQLite before transcription, routing or any
external effect. It retries only safe stages:

- transcription and webhook delivery when the provider reports a retryable failure
- routing and payload extraction before an action starts

Once an action command starts, a timeout, lost receipt or ambiguous result is
retained as indeterminate. Pebble does not replay it automatically.

## Logs and exports

Normal logs contain stable error codes, not transcript bodies or credentials. The
archive contains raw requests and transcripts and must remain private. Exported
inputs are evidence bundles and can contain private text and audio.

## Public deployments

Before exposing an endpoint:

1. Use a random ingress token.
2. Limit the exposed paths to the routes you need.
3. Start custom actions in dry-run mode.
4. Keep the state directory and generated config out of backups or sync folders
   that other users can read.
5. Review every custom command as code running with your account's permissions.
