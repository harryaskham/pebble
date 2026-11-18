import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import funnel


class FunnelCommandsTest(unittest.TestCase):
    def setUp(self):
        self.backend = 'http://127.0.0.1:11301'
        self.host = 'fixture.example:443'
        self.original = {'TCP': {'443': {'HTTPS': True}}, 'AllowFunnel': {self.host: True},
                         'Web': {self.host: {'Handlers': {'/': {'Proxy': 'http://127.0.0.1:11300'},
                                                         '/unrelated': {'Proxy': 'http://127.0.0.1:9999'}}}}}
        self.state = copy.deepcopy(self.original)
        self.calls = []

    def inspect(self, command):
        return copy.deepcopy(self.state)

    def run_command(self, argv, **kwargs):
        self.calls.append(argv)
        path = next(arg.split('=', 1)[1] for arg in argv if arg.startswith('--set-path='))
        handlers = self.state['Web'][self.host]['Handlers']
        if argv[-1] == 'off':
            handlers.pop(path, None)
        else:
            handlers[path] = {'Proxy': argv[-1]}

    def install(self, **kwargs):
        return funnel.install('tailscale', 'fixture.example', self.backend, run=self.run_command, inspect=self.inspect, **kwargs)

    def test_all_paths_restore_the_prefix_stripped_by_funnel(self):
        result = self.install(commands=True)
        self.assertEqual(len(result['urls']), len(funnel.COMMAND_PATHS))
        self.assertEqual(len(self.calls), len(funnel.COMMAND_PATHS))
        for path in funnel.COMMAND_PATHS:
            self.assertEqual(self.state['Web'][self.host]['Handlers'][path], {'Proxy': funnel.proxy_target(self.backend, path)})
        self.assertEqual(funnel.without_owned_path(self.state, self.host, funnel.COMMAND_PATHS), self.original)
        self.assertFalse(self.install(commands=True)['changed'])
        self.assertEqual(len(self.calls), len(funnel.COMMAND_PATHS))

    def test_foreign_path_refuses_entire_addition_before_first_change(self):
        self.state['Web'][self.host]['Handlers']['/hooks/global/pebble-bd-file'] = {'Proxy': 'http://127.0.0.1:8888'}
        original = copy.deepcopy(self.state)
        with self.assertRaisesRegex(RuntimeError, 'owned_by_another_backend'):
            self.install(commands=True)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.state, original)

    def test_interrupted_partial_install_converges_without_rewriting_existing_paths(self):
        self.state['Web'][self.host]['Handlers'][funnel.COMMAND_PATHS[0]] = {'Proxy': self.backend}
        self.state['Web'][self.host]['Handlers'][funnel.COMMAND_PATHS[1]] = {'Proxy': self.backend}
        self.install(commands=True)
        self.assertEqual(len(self.calls), len(funnel.COMMAND_PATHS))  # includes migration of old bare-root mappings
        self.assertFalse(self.install(commands=True)['changed'])

    def test_explicit_rollback_keeps_audio_root_and_foreign_handlers(self):
        self.install(commands=True)
        self.state['Web'][self.host]['Handlers']['/hooks/global/pebble-agent-msg'] = {'Proxy': 'http://127.0.0.1:9998'}
        result = self.install(retire_commands=True)
        handlers = self.state['Web'][self.host]['Handlers']
        self.assertEqual(len(result['retired_paths']), len(funnel.COMMAND_PATHS) - 2)
        self.assertEqual(handlers[funnel.PATH], {'Proxy': funnel.proxy_target(self.backend, funnel.PATH)})
        self.assertEqual(handlers['/'], self.original['Web'][self.host]['Handlers']['/'])
        self.assertEqual(handlers['/unrelated'], self.original['Web'][self.host]['Handlers']['/unrelated'])
        self.assertEqual(handlers['/hooks/global/pebble-agent-msg'], {'Proxy': 'http://127.0.0.1:9998'})
        self.assertNotIn('/hooks/global/pebble', handlers)
        self.assertFalse(self.install(retire_commands=True)['changed'])
        self.assertFalse(any('reset' in argv for argv in self.calls))

    def test_concurrent_change_is_not_overwritten(self):
        calls = 0
        def changed(command):
            nonlocal calls
            calls += 1
            if calls == 2:
                self.state['Web'][self.host]['Handlers'][funnel.PATH] = {'Proxy': 'http://127.0.0.1:1234'}
            return copy.deepcopy(self.state)
        with self.assertRaisesRegex(RuntimeError, 'changed_during_reconciliation'):
            funnel.install('tailscale', 'fixture.example', self.backend, run=self.run_command, inspect=changed, commands=True)
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
