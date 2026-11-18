// One-shot file synthesis through the same broker-aware helper as Pi speech.
// No playback here; Feedback owns its normal paplay call and speech receipt.
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

export async function synthesize(request, agentUtilsDir, env = process.env) {
  const directory = agentUtilsDir.startsWith('~/') ? path.join(os.homedir(), agentUtilsDir.slice(2)) : agentUtilsDir;
  const { synthesizeAzureSpeechDirect } = await import(pathToFileURL(path.join(directory, 'extensions/lib/tts.js')).href);
  return synthesizeAzureSpeechDirect({
    text: request.text, voice: request.voice, speed: Number(request.speed),
    speakerProfileId: request.embedding || null, lang: 'en-GB',
    outputFormat: 'riff-24khz-16bit-mono-pcm', timeoutMs: request.timeout_ms,
    auth: env.AZURE_SPEECH_AUTH, env,
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [agentUtilsDir, outputPath] = process.argv.slice(2);
    const request = JSON.parse(fs.readFileSync(0, 'utf8'));
    const audio = await synthesize(request, agentUtilsDir);
    fs.writeFileSync(outputPath, audio, { mode: 0o600 });
  } catch {
    // Do not forward provider bodies, credentials or broker output to logs.
    process.stderr.write('pebble: speech_synthesis_failed\n');
    process.exitCode = 1;
  }
}
