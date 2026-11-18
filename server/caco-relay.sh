# Service-scoped alias only; never install this relay in the user's global PATH.
if [[ -z "${PEBBLE_CACO_BIN:-}" ]]; then
  echo 'pebble: Cacophony executable has not been resolved' >&2
  exit 78
fi
exec "$PEBBLE_CACO_BIN" "$@"
