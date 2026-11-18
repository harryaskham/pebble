// Real OpenAI Node SDK against the isolated receiver started by test_triage_sdk.py.
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
const { default: OpenAI } = await import(pathToFileURL(process.env.TRIAGE_OPENAI_SDK).href);
const client = new OpenAI({ baseURL: process.env.TRIAGE_TEST_URL + '/route/v1', apiKey: 'fixture-sdk-token', maxRetries: 1, timeout: 5000 });
const messages = [{ role: 'user', content: 'File an isolated SDK fixture' }];
const extra_headers = { 'X-Triage-Session-ID': 'sdk-session', 'Idempotency-Key': 'sdk-turn' };
const stream = await client.chat.completions.create({ model: 'triage-route-dry-run', messages, stream: true, max_tokens: 1024, stream_options: { include_usage: true } }, { headers: extra_headers });
let text = '', id, finish;
for await (const chunk of stream) {
  id ??= chunk.id; assert.equal(chunk.id, id);
  text += chunk.choices[0]?.delta.content ?? '';
  finish = chunk.choices[0]?.finish_reason ?? finish;
}
assert.match(text, /No action executed/); assert.equal(finish, 'stop');
const again = await client.chat.completions.create({ model: 'triage-route-dry-run', messages, stream: false }, { headers: extra_headers });
assert.equal(again.id, id); assert.match(again.choices[0].message.content, /No action executed/);
await assert.rejects(client.chat.completions.create({ model: 'triage-route', messages }, { headers: { ...extra_headers, 'X-Triage-Purpose': 'user' } }), e => e.status === 409);
const models = await client.models.list(); assert(models.data.some(m => m.id === 'triage-route-dry-run'));
console.log(JSON.stringify({sdk_stream:true,sdk_nonstream:true,retry_identity:true,mode_conflict:true,models:true}));
