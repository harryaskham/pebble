# Resolve the system CLI before the launcher's scoped `caco` relay enters PATH.
# Installation policy belongs to the host, not this consumer.
{
  lib,
  override ? "caco",
}: ''
  PEBBLE_CACO_BIN=${
    if override != "caco"
    then lib.escapeShellArg override
    else ''"''${CACO_BIN:-caco}"''
  }
  case "$PEBBLE_CACO_BIN" in
    '~/'*) PEBBLE_CACO_BIN="$HOME/''${PEBBLE_CACO_BIN:2}" ;;
  esac
  if [[ "$PEBBLE_CACO_BIN" != */* ]]; then
    PEBBLE_CACO_BIN="$(command -v -- "$PEBBLE_CACO_BIN" || true)"
  fi
  if [[ -z "$PEBBLE_CACO_BIN" || ! -f "$PEBBLE_CACO_BIN" || ! -x "$PEBBLE_CACO_BIN" ]]; then
    echo 'pebble: caco unavailable; correct commands.caco.binary, CACO_BIN or the system PATH' >&2
    exit 78
  fi
  if [[ "$PEBBLE_CACO_BIN" != /* ]]; then
    PEBBLE_CACO_BIN="$PWD/$PEBBLE_CACO_BIN"
  fi
  export PEBBLE_CACO_BIN
''
