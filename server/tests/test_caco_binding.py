import json
import os
from unittest.mock import patch
import unittest
import urllib.request

import test_command_pipeline as fixtures
import receiver


class CacoBindingTest(unittest.TestCase):
    setUp = fixtures.PipelineTest.setUp

    def load(self):
        path = self.root / 'binding-config.json'
        path.write_text(json.dumps(self.config))
        return receiver.load_config(str(path))

    def test_launcher_selection_is_used_for_default_caco(self):
        with patch.dict(os.environ, {'PEBBLE_CACO_BIN': '/fixture/home/.cargo/bin/caco'}):
            config = self.load()
        self.assertEqual(config['commands']['caco']['binary'], '/fixture/home/.cargo/bin/caco')

    def test_explicit_operator_override_remains_authoritative(self):
        self.config['commands']['caco']['binary'] = '/fixture/explicit/caco'
        with patch.dict(os.environ, {'PEBBLE_CACO_BIN': '/fixture/home/.cargo/bin/caco'}):
            config = self.load()
        self.assertEqual(config['commands']['caco']['binary'], '/fixture/explicit/caco')

    def test_read_only_diagnostics_report_selected_binary_without_running_it(self):
        selected = '/fixture/not-executable/selected-caco'
        self.config['commands']['caco']['binary'] = selected
        with fixtures.PipelineTest.http(self) as (url, _):
            for endpoint in ('/health', '/pebble/status'):
                request = urllib.request.Request(url + endpoint, headers={'Authorization': 'Bearer ' + self.legacy.token})
                with urllib.request.urlopen(request, timeout=3) as response:
                    data = json.load(response)
                self.assertEqual((data if endpoint == '/health' else data['service'])['caco_binary'], selected)
        self.assertEqual(self.sender.calls, [])

    def test_caco_subprocess_preserves_declared_template_environment(self):
        inherited = {
            'PARATENIC_AHP_URL': 'ws://fixture-host:7788/ahp',
            'PARATENIC_AHP_TOKEN_FILE': '/fixture/paratenic-token',
            'PARATENIC_BACKEND_SOCKET': '/fixture/paratenic.sock',
            'PARATENIC_EXTENSION_PATH': '/fixture/paratenic.mjs',
        }
        clients = []
        sender = self.sender
        class Caco:
            def __init__(self, **kwargs): clients.append(kwargs)
            def execute(self, recipe): return sender(recipe)
        self.processor.sender = None
        receipt = self.archive.accept(b'{"transcription":"fixture send","recordedAt":"2001"}', 'application/json', 'pebble')
        with patch.dict(os.environ, inherited), patch.object(fixtures.command_pipeline.dispatch, 'Caco', Caco):
            fixtures.PipelineTest.drain(self)
        self.assertEqual(len(clients), 1)
        for key, value in inherited.items():
            self.assertEqual(clients[0]['env'][key], value)
            self.assertNotIn(value, self.archive.get(receipt['id'])['settings'])
        self.assertEqual(clients[0]['env']['CACO_DAEMON_URL'], self.config['commands']['caco']['daemon_url'])
        self.assertEqual(clients[0]['env']['CACO_AGENT_TOKEN_FILE'], self.config['commands']['caco']['token_file'])
        self.assertEqual(self.archive.receipt(receipt['id'])['action']['state'], 'stored')

    def test_current_executable_rebinds_old_records_without_changing_intent(self):
        body = b'{"transcription":"File this","recordedAt":"1770000000009"}'
        receipt = self.archive.accept(body, 'application/json', 'bd-file')
        self.config['commands']['caco']['binary'] = '/fixture/home/.cargo/bin/caco'
        settings = self.processor.settings(self.archive.get(receipt['id']))
        self.assertEqual(settings['commands']['caco']['binary'], '/fixture/home/.cargo/bin/caco')
        self.assertEqual(settings['commands']['caco']['daemon_url'], self.config['commands']['caco']['daemon_url'])
        self.assertEqual(self.archive.get(receipt['id'])['text'], 'File this')
        self.assertEqual(self.sender.calls, [])
