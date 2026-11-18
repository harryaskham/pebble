# Custom actions

Custom actions are trusted operator configuration. Jev chooses an action from its
description, then a payload extractor produces the text sent to the command. The
model never sees or edits the command.

```yaml
actions:
  note:
    description: Store a private note when the user asks to save a thought.
    extract: Extract the note text only. Preserve every detail.
    command: ["pebble-org-append", "--file", "~/org/notes.org"]
    timeout_secs: 30
    lane: notes
```

| Field | Required | Meaning |
| --- | --- | --- |
| `description` | yes | Text Jev uses to choose the action. |
| `command` | yes | Argv list, or a string run by `bash -euo pipefail -c`. |
| `extract` | no | Additional instructions for the payload extractor. |
| `timeout_secs` | no | Command timeout, 1 to 300 seconds. Default 60. |
| `lane` | no | Serialisation lane. Actions in the same lane run one at a time. |
| `enabled` | no | Set false to hide an action without deleting it. |

Names use lowercase letters, numbers and hyphens, and cannot shadow built-in actions.
Unknown or disabled actions fail closed.

## Command contract

- The extracted UTF-8 payload is stdin.
- `PEBBLE_ACTION`, `PEBBLE_INPUT_ID` and `PEBBLE_REQUEST_ID` identify the request.
- The command runs with the service user's authority.
- The service PATH contains Pebble's runtime packages plus `runtimePackages`.
- Exit 0 means completed. A command that cannot be started is `not_started`.
- A non-zero exit or timeout after start is retained as indeterminate.
- Pebble does not automatically retry an indeterminate custom command.

Never interpolate stdin into shell syntax. Read it as data, for example with
`curl --data-binary @-`.

## Runtime manifests

`actions_file` is re-read for every new request. Pending requests keep the action
snapshot they were admitted with. Check a replacement before moving it into place:

```bash
pebble-actions-check ~/.config/pebble/actions.yaml.new
mv ~/.config/pebble/actions.yaml.new ~/.config/pebble/actions.yaml
```

An invalid manifest makes new command requests fail closed until it is fixed.

## Jev profiles

A profile can restrict the actions visible to Jev:

```nix
services.pebble.commands.jevProfiles.work = {
  model = "openai/gpt-4.1";
  actions = ["note" "reminder"];
};
```

If a profile has no `actions` field, it sees every enabled custom action. With the
Caco adapter disabled, Jev sees only custom actions.
