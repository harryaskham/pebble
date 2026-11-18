import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import provider_env


class ProviderEnvironmentTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.secret = self.root/'key'
        self.secret.write_text('fixture-private-key\n')
        self.secret.chmod(0o600)
        self.link = self.root/'managed-key'
        self.link.symlink_to(self.secret)
        self.path = self.root/'providers.json'
        self.config = {'environment': {'JEV_PROVIDER':'openrouter','AZURE_SPEECH_ENDPOINT':'https://speech.invalid',
                                       'AZURE_SPEECH_AUTH':'entra','AZURE_SPEECH_AZ':'/fixture/az-token','OPENAI_BASE_URL':'https://openai.invalid'},
                       'secret_files':{name:str(self.link) for name in ('OPENAI_API_KEY', 'OPENROUTER_API_KEY')}}
        self.save()

    def save(self): self.path.write_text(json.dumps(self.config))

    def test_selected_values_override_inherited_and_do_not_change_manifest(self):
        before = self.path.read_bytes()
        env = provider_env.load(self.path, {'PATH':'/fixture/bin','OPENAI_API_KEY':'old'})
        self.assertEqual(env['PATH'], '/fixture/bin')
        self.assertEqual(env['OPENAI_API_KEY'], 'fixture-private-key')
        self.assertEqual(env['JEV_PROVIDER'], 'openrouter')
        self.assertEqual(self.path.read_bytes(), before)
        self.assertNotIn(b'fixture-private-key', before)

    def test_entra_needs_no_speech_key_and_discards_stale_inherited_keys(self):
        self.assertNotIn('AZURE_SPEECH_API_KEY', self.config['secret_files'])
        env = provider_env.load(self.path, {'AZURE_SPEECH_API_KEY': 'old-key', 'AZURE_SPEECH_KEY': 'old-key'})
        self.assertNotIn('AZURE_SPEECH_API_KEY', env)
        self.assertNotIn('AZURE_SPEECH_KEY', env)
        self.assertEqual(env['AZURE_SPEECH_AUTH'], 'entra')
        self.assertEqual(env['AZURE_SPEECH_AZ'], '/fixture/az-token')
        self.config['secret_files'] = {}
        self.config['environment'].pop('OPENAI_BASE_URL')
        self.save()
        result = subprocess.run([sys.executable, str(Path(provider_env.__file__)), '--config', str(self.path), '--check'],
                                env={'PATH': os.environ['PATH']}, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0)
        self.assertTrue(json.loads(result.stdout)['ready'])

    def test_managed_symlink_rotation_is_read_on_each_start(self):
        self.assertEqual(provider_env.load(self.path, {})['OPENAI_API_KEY'], 'fixture-private-key')
        self.secret.write_text('rotated-fixture-key')
        self.assertEqual(provider_env.load(self.path, {})['OPENAI_API_KEY'], 'rotated-fixture-key')
        self.assertTrue(self.link.is_symlink())

    def test_exec_boundary_has_environment_but_check_never_prints_values(self):
        executable = str(Path(provider_env.__file__))
        check = subprocess.run([sys.executable,executable,'--config',str(self.path),'--check'],capture_output=True,timeout=5)
        self.assertEqual(check.returncode, 0)
        self.assertTrue(all(json.loads(check.stdout)['present'].values()))
        self.assertNotIn(b'fixture-private-key', check.stdout+check.stderr)
        child = subprocess.run([sys.executable,executable,'--config',str(self.path),'--',sys.executable,'-c',
            'import os; assert os.environ["OPENAI_API_KEY"] == "fixture-private-key"; assert os.environ["JEV_PROVIDER"] == "openrouter"'],capture_output=True,timeout=5)
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertEqual(child.stdout, b'')

    def test_missing_or_unsafe_secret_fails_without_disclosing_values(self):
        for mode in ('missing','public','nul','fifo'):
            with self.subTest(mode=mode):
                self.secret.unlink(missing_ok=True)
                if mode == 'fifo': os.mkfifo(self.secret, 0o600)
                elif mode != 'missing':
                    self.secret.write_bytes(b'fixture-private-key\0' if mode == 'nul' else b'fixture-private-key')
                    self.secret.chmod(0o644 if mode == 'public' else 0o600)
                with self.assertRaises(provider_env.EnvironmentError) as caught:
                    provider_env.load(self.path, {'OPENAI_API_KEY':'must-not-be-used'})
                self.assertNotIn('fixture-private-key', str(caught.exception))

    def test_manifest_cannot_supply_arbitrary_environment_or_inline_secrets(self):
        self.config['environment']['OPENAI_API_KEY'] = 'inline-secret'
        self.save()
        with self.assertRaises(provider_env.EnvironmentError): provider_env.load(self.path)
        self.config['environment'].pop('OPENAI_API_KEY')
        self.config['environment']['PATH'] = '/tmp/evil'
        self.save()
        with self.assertRaises(provider_env.EnvironmentError): provider_env.load(self.path)
        self.config['environment'].pop('PATH')
        self.config['secret_files']['BYO_STT_API_KEY'] = str(self.link)
        self.save()
        self.assertEqual(provider_env.load(self.path, {})['BYO_STT_API_KEY'], 'fixture-private-key')
        self.config['secret_files'].pop('BYO_STT_API_KEY')
        self.config['secret_files']['bad-name'] = str(self.secret)
        self.save()
        with self.assertRaises(provider_env.EnvironmentError): provider_env.load(self.path)

    def test_invalid_manifest_is_sanitized(self):
        for value in ('not json containing private stuff', '[]', '{}'):
            self.path.write_text(value)
            with self.assertRaisesRegex(provider_env.EnvironmentError, '^invalid_provider_environment_manifest$'):
                provider_env.load(self.path)
