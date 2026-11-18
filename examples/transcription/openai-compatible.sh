#!/usr/bin/env bash
# Example BYO transcription command for an OpenAI-compatible API.
# Pebble writes 24 kHz mono WAV to stdin. The API key must come from Pebble's
# providerSecretFiles, never from Nix-generated configuration.
set -euo pipefail
: "${OPENAI_API_KEY:?OPENAI_API_KEY is required}"
base_url="${PEBBLE_STT_BASE_URL:-https://api.openai.com/v1}"
model="${PEBBLE_STT_MODEL:-whisper-1}"
curl --fail --silent --show-error \
  -H "Authorization: Bearer ${OPENAI_API_KEY}" \
  -F "model=${model}" \
  -F "response_format=json" \
  -F "file=@-;filename=pebble.wav;type=audio/wav" \
  "${base_url%/}/audio/transcriptions"
