import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
import wave

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from feedback import Feedback, Voice
from process_io import Result, run


def tiny_wav():
    output = io.BytesIO()
    with wave.open(output, 'wb') as audio:
        audio.setnchannels(1)
        audio.setsampwidth(2)
        audio.setframerate(24000)
        audio.writeframes(b'\0\0' * 100)
    return output.getvalue()


class FeedbackTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.calls = []
        def runner(argv, payload=b'', **kwargs):
            self.calls.append((argv, payload, kwargs))
            if '--output' in argv:
                Path(argv[argv.index('--output') + 1]).write_bytes(tiny_wav())
            return Result(b'', 1)
        self.feedback = Feedback(self.temp.name, aztts='fixture-aztts', player='fixture-paplay', env={}, runner=runner)

    def test_both_voice_roles_default_to_named_mai_without_an_embedding(self):
        self.assertEqual(Voice.from_env('UI', {}).arguments(),
                         ['--voice', 'en-US-Ava', '--speed', '1.0', '--embedding', '', '--whole'])
        self.assertEqual(Voice.from_env('SELF', {}).arguments(),
                         ['--voice', 'en-US-Andrew', '--speed', '1.0', '--embedding', '', '--whole'])
        voice = Voice.from_env('UI', {'PEBBLE_UI_VOICE': 'operator-voice', 'PEBBLE_UI_SPEED': '1.7', 'PEBBLE_UI_EMBEDDING': ''})
        self.assertEqual(voice.voice, 'operator-voice')
        self.assertEqual(voice.speed, '1.7')

    def test_module_defaults_and_python_fallbacks_select_the_same_named_voice(self):
        settings = json.loads((Path(__file__).resolve().parent / 'fixtures/commands-caco.json').read_text())
        for role in ('UI', 'SELF'):
            self.assertEqual(Voice.from_env(role, settings['speech']['voices']), Voice.from_env(role, {}))

    def test_explicit_legacy_base_and_embedding_remain_operator_overrides(self):
        embedding = '00000000-0000-4000-8000-000000000001'
        voice = Voice.from_env('SELF', {'PEBBLE_SELF_VOICE': 'MAI-Voice-2', 'PEBBLE_SELF_EMBEDDING': embedding})
        self.assertEqual(voice.arguments()[:2], ['--voice', 'MAI-Voice-2'])
        self.assertEqual(voice.embedding, embedding)

    def test_acknowledgement_caches_only_the_prefix_and_uses_stdin(self):
        text = "No, don't add $(this)\nkeep the original words"
        self.feedback.acknowledge(text)
        self.feedback.acknowledge('a second fresh recording')
        synthesis = [call for call in self.calls if '--output' in call[0]]
        self.assertEqual(len(synthesis), 1)
        self.assertEqual(synthesis[0][1], b'pebble ack: ')
        playback = [call for call in self.calls if call[0][0] == 'fixture-paplay']
        self.assertEqual(len(playback), 2)
        readback = [call for call in self.calls if call[0][0] == 'fixture-aztts' and '--output' not in call[0]]
        self.assertEqual(readback[0][1].decode(), text)
        self.assertNotIn(text, readback[0][0])
        self.assertEqual(readback[0][0][1:], self.feedback.self_voice.arguments())

    def test_route_and_bead_confirmation_are_receipt_driven(self):
        self.feedback.routing('bd-file')
        self.feedback.beads_created([{'id': 'bd-test', 'title': 'SDK compatibility'}])
        self.assertEqual(self.calls[0][1], b'pebble: routing to bd-file')
        self.assertTrue(any(b'filed 1 bead' in call[1] for call in self.calls))
        with self.assertRaisesRegex(ValueError, 'invalid_bead_receipt'):
            self.feedback.beads_created([{'title': 'No authoritative ID'}])

    def test_fixed_route_announcements_reuse_audio_without_broker_or_synthesis(self):
        self.feedback.routing('pebble')
        self.feedback.routing('pebble')
        synthesis = [call for call in self.calls if '--output' in call[0]]
        playback = [call for call in self.calls if call[0][0] == 'fixture-paplay']
        self.assertEqual(len(synthesis), 1)
        self.assertEqual(synthesis[0][1], b'pebble: routing to pebble')
        self.assertEqual(len(playback), 2)
        self.assertEqual(playback[0][0], playback[1][0])
        self.feedback.routing('agent-msg')
        self.assertEqual(len([call for call in self.calls if '--output' in call[0]]), 2)

    def test_storage_is_not_reported_as_execution(self):
        self.feedback.message_stored('sdk-worker', {'storage_state': 'stored', 'pi_admission_state': 'unavailable'})
        self.assertEqual(self.calls[-1][1], b'pebble: message stored for sdk-worker; delivery pending')
        self.feedback.message_stored('sdk-worker', {'pi_admission_state': 'admitted'})
        self.assertEqual(self.calls[-1][1], b'pebble: message admitted by sdk-worker')
        self.assertNotIn(b'executed', self.calls[-1][1])
        self.feedback.message_stored('sdk-worker', {'runtime_delivery_state': 'rejected'})
        self.assertIn(b'agent delivery rejected', self.calls[-1][1])
        self.feedback.message_stored('sdk-worker', {'runtime_delivery_state': 'indeterminate'})
        self.assertIn(b'delivery unconfirmed; not resent', self.calls[-1][1])

    def test_non_finite_speed_is_rejected(self):
        for speed in ('nan', 'inf', '0', '-1'):
            with self.assertRaisesRegex(ValueError, 'invalid_voice_speed'):
                Voice.from_env('UI', {'PEBBLE_UI_SPEED': speed})

    @unittest.skipUnless(shutil.which('aztts'), 'actual aztts needed for offline SSML contract')
    def test_real_aztts_offline_syntax(self):
        ui = run(['aztts', *self.feedback.ui.arguments(), '--print-ssml'], b'pebble ack: ').stdout.decode()
        own = run(['aztts', *self.feedback.self_voice.arguments(), '--print-ssml'], b'original words').stdout.decode()
        self.assertIn("voice name='en-US-Ava", ui)
        self.assertIn("rate='+0.00%'", ui)
        self.assertNotIn('speakerProfileId=', ui)
        self.assertIn("voice name='en-US-Andrew", own)
        self.assertNotIn('speakerProfileId=', own)


if __name__ == '__main__':
    unittest.main()
