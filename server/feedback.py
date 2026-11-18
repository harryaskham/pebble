"""Pebble's two voice roles and configurable speech providers.

The owning job ledger must claim each logical announcement before invoking
these functions. Playback failure is not a reason to discard or replay a job.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile

from process_io import ProcessError, run

@dataclass(frozen=True)
class Voice:
    voice: str
    speed: str
    embedding: str

    @classmethod
    def from_env(cls, role, env=None):
        env = os.environ if env is None else env
        if role not in ('UI', 'SELF'):
            raise ValueError('invalid_voice_role')
        prefix = 'PEBBLE_' + role + '_'
        voice = env.get(prefix + 'VOICE', 'en-US-Ava' if role == 'UI' else 'en-US-Andrew')
        speed = env.get(prefix + 'SPEED', '1.0')
        embedding = env.get(prefix + 'EMBEDDING', '')
        if not voice or len(voice) > 256 or '\0' in voice or len(embedding) > 256 or '\0' in embedding:
            raise ValueError('invalid_voice')
        try:
            numeric = float(speed)
        except (ValueError, TypeError) as exc:
            raise ValueError('invalid_voice_speed') from exc
        if not math.isfinite(numeric) or numeric <= 0:
            raise ValueError('invalid_voice_speed')
        return cls(voice, str(speed), embedding)

    def arguments(self):
        # --voice accepts both explicit base voices and qualified personas;
        # unlike --model, it is not limited to aztts' built-in family aliases.
        # Explicit empty embeddings disable the CLI's cloned-voice defaults.
        return ['--voice', self.voice, '--speed', self.speed, '--embedding', self.embedding, '--whole']


class Feedback:
    def __init__(self, cache, *, aztts='aztts', player='paplay', node='node', agent_utils_dir=None, provider='auto', command=None,
                 env=None, runner=run, timeout=120):
        if provider not in ('auto', 'agent-utils-azure', 'aztts', 'command'):
            raise ValueError('invalid_speech_provider')
        if provider == 'command' and (not isinstance(command, list) or not command or any(not isinstance(v, str) or not v for v in command)):
            raise ValueError('invalid_speech_command')
        self.cache = Path(cache).expanduser()
        self.aztts, self.player = aztts, player
        self.node, self.agent_utils_dir = node, agent_utils_dir
        self.provider, self.command = provider, list(command or [])
        self.env = dict(os.environ if env is None else env)
        self.runner, self.timeout = runner, timeout
        self.ui, self.self_voice = Voice.from_env('UI', self.env), Voice.from_env('SELF', self.env)

    def uses_agent_utils(self):
        return self.provider == 'agent-utils-azure' or self.provider == 'auto' and self.env.get('AZURE_SPEECH_AUTH') == 'entra'

    def command_env(self, voice):
        return {**self.env, 'PEBBLE_SPEECH_VOICE': voice.voice, 'PEBBLE_SPEECH_SPEED': voice.speed,
                'PEBBLE_SPEECH_EMBEDDING': voice.embedding}

    def speak(self, text, voice=None):
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 32768:
            raise ValueError('invalid_utterance')
        voice = self.ui if voice is None else voice
        if self.provider == 'command':
            return self.runner([str(Path(self.command[0]).expanduser()), *self.command[1:]], text.encode(),
                               timeout=self.timeout, max_output=0, env=self.command_env(voice))
        if self.uses_agent_utils():
            with tempfile.TemporaryDirectory(prefix='pebble-speech-') as tmp:
                audio = Path(tmp) / 'speech.wav'
                self.synthesize(text, voice, audio)
                return self.runner([self.player, str(audio)], timeout=self.timeout, max_output=0, env=self.env)
        return self.runner([self.aztts, *voice.arguments()], text.encode(),
                           timeout=self.timeout, max_output=0, env=self.env)

    def synthesize(self, text, voice, destination):
        if self.provider == 'command':
            raise ProcessError('speech_command_has_no_file_output')
        if self.uses_agent_utils():
            request = {'text': text, **voice.__dict__, 'timeout_ms': self.timeout * 1000}
            return self.runner([self.node, str(Path(__file__).with_name('speech.mjs')), self.agent_utils_dir, str(destination)],
                               json.dumps(request).encode(), timeout=self.timeout + 5, max_output=0, env=self.env)
        return self.runner([self.aztts, *voice.arguments(), '--format', 'wav', '--output', str(destination)],
                           text.encode(), timeout=self.timeout, max_output=0, env=self.env)

    def cached_ui(self, text):
        # Only fixed UI phrases use this cache, never operator transcripts.
        if self.provider == 'command':
            return self.speak(text, self.ui)
        key = hashlib.sha256(json.dumps({'voice': self.ui.__dict__, 'text': text,
                                        'format': 'wav'}, sort_keys=True).encode()).hexdigest()
        self.cache.mkdir(parents=True, exist_ok=True, mode=0o700)
        destination = self.cache / (key + '.wav')
        if not destination.is_file():
            fd, temporary = tempfile.mkstemp(prefix='ui-', suffix='.wav', dir=self.cache)
            os.close(fd)
            try:
                self.synthesize(text, self.ui, temporary)
                data = Path(temporary).read_bytes()
                if not 44 <= len(data) <= 4 * 1024 * 1024 or data[:4] != b'RIFF' or data[8:12] != b'WAVE':
                    raise ProcessError('invalid_cached_speech')
                os.replace(temporary, destination)
            finally:
                Path(temporary).unlink(missing_ok=True)
        self.runner([self.player, str(destination)], timeout=self.timeout, max_output=0, env=self.env)

    def prefix(self):
        self.cached_ui('pebble ack: ')

    def acknowledge(self, text):
        self.prefix()
        self.speak(text, self.self_voice)

    def routing(self, action):
        if action not in ('pebble', 'bd-file', 'bd-expand', 'agent-msg', 'broadcast'):
            raise ValueError('invalid_route_announcement')
        self.cached_ui('pebble: routing to ' + action)

    def beads_created(self, beads):
        if not isinstance(beads, list) or not 1 <= len(beads) <= 50:
            raise ValueError('invalid_bead_receipt')
        descriptions = []
        for bead in beads:
            identifier, title = bead.get('id'), bead.get('title')
            if not isinstance(identifier, str) or not identifier.startswith('bd-') or not isinstance(title, str):
                raise ValueError('invalid_bead_receipt')
            descriptions.append(identifier + ' — ' + title[:300])
        self.speak(f'pebble: filed {len(beads)} ' + ('bead: ' if len(beads) == 1 else 'beads: ') + '; '.join(descriptions))

    def message_stored(self, target, admission=None):
        # Never derive delivery or execution from storage acceptance alone.
        admission = admission if isinstance(admission, dict) else {}
        pi_state = admission.get('pi_admission_state')
        runtime_state = admission.get('runtime_delivery_state')
        if pi_state == 'admitted':
            text = 'pebble: message admitted by ' + target
        elif 'rejected' in (pi_state, runtime_state):
            text = 'pebble: message stored for ' + target + '; agent delivery rejected'
        elif 'indeterminate' in (pi_state, runtime_state):
            text = 'pebble: message stored for ' + target + '; delivery unconfirmed; not resent'
        else:
            text = 'pebble: message stored for ' + target + '; delivery pending'
        self.speak(text)
