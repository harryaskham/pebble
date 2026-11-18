"""Real Pi TUI + fake PTT bus + fake CAPI. No audio, credentials or live services."""
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = Path(__file__).resolve().parent
PI = os.environ.get('TRG_TEST_PI') or str(Path(os.environ.get('PI_PACKAGE_DIR', '/missing')) / 'pi')
TMUX = os.environ.get('TRG_TEST_TMUX')


def rasterize(frame, path, columns, rows):
    """Rasterize tmux's actual terminal-cell snapshot, retaining its ANSI colors."""
    from PIL import Image, ImageDraw, ImageFont
    font_path = subprocess.check_output(['fc-match', '-f', '%{file}', 'DejaVu Sans Mono'], text=True).strip()
    font = ImageFont.truetype(font_path, 16)
    cell, height = 10, 20
    image = Image.new('RGB', (columns * cell, rows * height), '#1b1d23')
    draw = ImageDraw.Draw(image)
    def color(index):
        palette = ['#000000', '#cd0000', '#00cd00', '#cdcd00', '#0000ee', '#cd00cd', '#00cdcd', '#e5e5e5', '#7f7f7f', '#ff0000', '#00ff00', '#ffff00', '#5c5cff', '#ff00ff', '#00ffff', '#ffffff']
        if index < 16: return palette[index]
        if index >= 232: return (8 + (index - 232) * 10,) * 3
        n = index - 16
        return tuple([0, 95, 135, 175, 215, 255][v] for v in (n // 36, n // 6 % 6, n % 6))
    fg, bg = '#e5e5e5', '#1b1d23'
    for y, line in enumerate(frame.splitlines()[:rows]):
        x = 0
        for part in re.split(r'(\x1b\[[0-9;]*m)', line):
            if part.startswith('\x1b['):
                values = [int(v or '0') for v in part[2:-1].split(';')]; i = 0
                while i < len(values):
                    v = values[i]
                    if v == 0: fg, bg = '#e5e5e5', '#1b1d23'
                    elif v == 39: fg = '#e5e5e5'
                    elif v == 49: bg = '#1b1d23'
                    elif 30 <= v <= 37: fg = color(v - 30)
                    elif 40 <= v <= 47: bg = color(v - 40)
                    elif v in (38, 48) and i + 2 < len(values):
                        if values[i+1] == 5: c = color(values[i+2]); i += 2
                        elif values[i+1] == 2 and i + 4 < len(values): c = tuple(values[i+2:i+5]); i += 4
                        else: i += 1; continue
                        if v == 38: fg = c
                        else: bg = c
                    i += 1
            else:
                for ch in part:
                    if x >= columns: break
                    draw.rectangle((x*cell, y*height, (x+1)*cell, (y+1)*height), fill=bg)
                    draw.text((x*cell, y*height), ch, font=font, fill=fg)
                    x += 1
    image.save(path)


@unittest.skipUnless(TMUX and Path(PI).is_file(), 'Set TRG_TEST_TMUX and PI_PACKAGE_DIR/TRG_TEST_PI')
class SpeechPtyTest(unittest.TestCase):
    def test_real_terminal(self):
        with tempfile.TemporaryDirectory(prefix='trg-voice-tui-') as root:
            root = Path(root)
            requests, errors = [], []
            state = root / 'turns'
            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *_): pass
                def do_POST(self):
                    try:
                        assert self.path == '/custom/v1/chat/completions'
                        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                        assert self.headers['Authorization'] == 'Bearer fake-only'
                        assert body['triage']['purpose'] == 'user'
                        assert (state / (body['triage']['turn_id'] + '.json')).is_file()
                        requests.append(body)
                        self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
                        def chunk(delta, finish=None):
                            return json.dumps({'id': 'chatcmpl-pty-fixture', 'model': body['model'], 'object': 'chat.completion.chunk', 'created': 1, 'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]})
                        for item in [chunk({'role': 'assistant'}), chunk({'content': 'Fake receipt only.'}), chunk({}, 'stop'), '[DONE]']:
                            self.wfile.write(('data: ' + item + '\n\n').encode())
                        self.wfile.flush()
                    except Exception as error:
                        errors.append(str(error)); self.send_error(500)
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            worker = threading.Thread(target=server.serve_forever, daemon=True); worker.start()
            env = {key: str(root) for key in ['HOME', 'TMPDIR', 'XDG_CONFIG_HOME', 'XDG_STATE_HOME', 'XDG_DATA_HOME', 'XDG_CACHE_HOME', 'PI_CODING_AGENT_DIR']}
            env.update(PATH=os.environ['PATH'], TERM='xterm-256color', PI_OFFLINE='1', PI_TELEMETRY='0', PI_SKIP_VERSION_CHECK='1', PI_PACKAGE_DIR=os.environ.get('PI_PACKAGE_DIR', str(Path(PI).parent)))
            (root / 'token').write_text('fake-only'); (root / 'token').chmod(0o600)
            (root / 'settings.json').write_text(json.dumps({'compaction': {'enabled': False}, 'retry': {'enabled': False}, 'enableInstallTelemetry': False}))
            config = root / 'trg.json'
            config.write_text(json.dumps({'baseUrl': f'http://127.0.0.1:{server.server_port}/custom/v1', 'tokenFile': str(root/'token'), 'stateDir': str(state), 'timeoutMs': 3000}))
            env['TRG_PI_CONFIG'] = str(config)
            log = root / 'bus.jsonl'
            fixture = root / 'fake-ptt.ts'
            fixture.write_text('''
import { appendFileSync } from 'node:fs';
export default function (pi) {
  let ctx, unsub, timer;
  const log = value => appendFileSync(LOG_PATH, JSON.stringify(value) + '\\n');
  pi.events.emit('realtime:controls', { snapshot: () => ({ previousModel: null, state: { connection: 'off', micMode: null } }) });
  pi.on('session_start', (_event, c) => { ctx = c; log({ action: 'ready' }); });
  pi.events.on('agent-utils:input-action', e => {
    log(e);
    if (e.action === 'freeform-enter') {
      unsub = ctx.ui.onTerminalInput(() => { log({ action: 'WRONG_GENERIC_RELEASE' }); return { consume: true }; });
      pi.events.emit('agent-utils:input-action', { action: 'freeform-update', source: 'ptt', sessionId: e.sessionId, text: 'Fixture speech for Triage' });
    } else if (e.action === 'freeform-ptt-commit') {
      unsub?.();
      timer = setTimeout(() => pi.events.emit('agent-utils:input-action', { action: 'freeform-submit', source: 'ptt', sessionId: e.sessionId, text: 'Fixture speech for Triage' }), 80);
    } else if (e.action === 'freeform-cancel') { unsub?.(); clearTimeout(timer); }
  });
  pi.on('session_shutdown', () => { unsub?.(); clearTimeout(timer); log({ action: 'shutdown' }); });
}
'''.replace('LOG_PATH', json.dumps(str(log))))
            socket = str(root / 'tmux.sock')
            def tmux(*args, check=True):
                return subprocess.run([TMUX, '-S', socket, '-f', '/dev/null', *args], env=env, cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=check, timeout=5).stdout
            def bus():
                return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
            def wait(predicate):
                end = time.monotonic() + 8
                while time.monotonic() < end:
                    if predicate(): return
                    time.sleep(.025)
                self.fail('bounded TUI wait expired: ' + tmux('capture-pane', '-p', '-t', 'trg:0.0', check=False)[-4000:])
            def command(text):
                tmux('send-keys', '-t', 'trg:0.0', '-l', text); tmux('send-keys', '-t', 'trg:0.0', 'Enter')
            def entered(count):
                wait(lambda: sum(row.get('action') == 'freeform-enter' for row in bus()) == count)
            artifacts = os.environ.get('TRG_TEST_ARTIFACTS')
            def capture(name, columns, rows):
                frame = tmux('capture-pane', '-p', '-e', '-t', 'trg:0.0')
                if artifacts:
                    dest = Path(artifacts); dest.mkdir(parents=True, exist_ok=True)
                    (dest / (name + '.ansi')).write_text(frame)
                    rasterize(frame, dest / (name + '.png'), columns, rows)
                return frame
            try:
                tmux('new-session', '-d', '-s', 'trg', '-x', '80', '-y', '24', PI, '--offline', '--no-approve', '--no-extensions', '-e', str(HERE/'index.ts'), '-e', str(fixture), '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--no-tools', '--provider', 'triage', '--model', 'triage-route-dry-run', '--tui-mode', 'fullscreen')
                tmux('set-option', '-g', 'remain-on-exit', 'on')
                wait(lambda: any(row.get('action') == 'ready' for row in bus()))
                time.sleep(.3)
                command('/trg-voice'); entered(1)
                wait(lambda: 'Triage voice' in tmux('capture-pane', '-p', '-t', 'trg:0.0'))
                frame = capture('voice-listening-80x24', 80, 24)
                self.assertIn('Fixture speech', frame)
                tmux('send-keys', '-t', 'trg:0.0', 'Enter')
                wait(lambda: len(requests) == 1)
                wait(lambda: 'Fake receipt only.' in tmux('capture-pane', '-p', '-t', 'trg:0.0'))
                command('/trg-voice'); entered(2)
                tmux('resize-window', '-t', 'trg:0', '-x', '30', '-y', '12')
                time.sleep(.1)
                capture('voice-listening-30x12', 30, 12)
                tmux('send-keys', '-t', 'trg:0.0', 'Escape')
                wait(lambda: sum(row.get('action') == 'freeform-cancel' for row in bus()) == 2)
                self.assertEqual(len(requests), 1)
                tmux('resize-window', '-t', 'trg:0', '-x', '80', '-y', '24')
                time.sleep(.1)
                command('/trg-voice draft'); entered(3)
                tmux('send-keys', '-t', 'trg:0.0', 'Enter')
                wait(lambda: sum(row.get('action') == 'freeform-cancel' for row in bus()) == 3)
                self.assertEqual(len(requests), 1)
                wait(lambda: 'Fixture speech for Triage' in tmux('capture-pane', '-p', '-t', 'trg:0.0'))
                capture('voice-draft-80x24', 80, 24)
                tmux('send-keys', '-t', 'trg:0.0', 'Enter')
                wait(lambda: len(requests) == 2)
                time.sleep(.2)
                self.assertNotEqual(requests[0]['triage']['turn_id'], requests[1]['triage']['turn_id'])
                self.assertFalse(any(row.get('action') == 'WRONG_GENERIC_RELEASE' for row in bus()))
                self.assertEqual(errors, [])
                command('/quit')
                wait(lambda: tmux('display-message', '-p', '-t', 'trg:0.0', '#{pane_dead}').strip() == '1')
                self.assertEqual(tmux('display-message', '-p', '-t', 'trg:0.0', '#{pane_dead_status}').strip(), '0')
                self.assertEqual(tmux('display-message', '-p', '-t', 'trg:0.0', '#{alternate_on}').strip(), '0')
                self.assertTrue(any(row.get('action') == 'shutdown' for row in bus()))
            finally:
                tmux('kill-server', check=False)
                server.shutdown(); server.server_close(); worker.join(timeout=2)
                self.assertFalse(worker.is_alive())


if __name__ == '__main__':
    unittest.main()
