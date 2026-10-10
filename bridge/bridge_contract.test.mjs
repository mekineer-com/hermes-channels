import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { runInNewContext } from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const bridgeSource = readFileSync(join(here, 'bridge.js'), 'utf8');
const messageIngestSource = readFileSync(join(here, 'message_ingest.js'), 'utf8');
const socketLifecycleSource = readFileSync(join(here, 'socket_lifecycle.js'), 'utf8');

test('bridge preserves persist-only WhatsApp event field contract', () => {
  assert.match(messageIngestSource, /event\.eventType\s*=\s*'history_message'/);
  assert.match(messageIngestSource, /deliveryMode:\s*'persist_only'/);
});

test('bridge stamps explicit live and revoke delivery modes', () => {
  assert.match(messageIngestSource, /deliveryMode:\s*mode\.deliveryMode/);
  assert.match(messageIngestSource, /event\.deliveryMode\s*=\s*delivery\.deliveryMode/);
  assert.match(messageIngestSource, /deliveryMode:\s*'revoke'/);
});

test('bridge keeps Baileys live path light and bounded', () => {
  assert.match(bridgeSource, /fireInitQueries:\s*false/);
  assert.match(bridgeSource, /syncFullHistory:\s*false/);
  assert.match(socketLifecycleSource, /fetchLatestBaileysVersion\(\{\s*timeout:\s*timeoutMs\s*\}\)/);
});

test('bridge does not report connected until socket open', () => {
  assert.match(bridgeSource, /receivedPendingNotifications/);
  assert.match(bridgeSource, /socketLifecycle\.markReady\(socketId\)/);
  assert.match(socketLifecycleSource, /openGeneration\s*!==\s*socketId/);
  assert.match(bridgeSource, /status:\s*socketLifecycle\.getState\(\)/);
  assert.match(socketLifecycleSource, /state\s*=\s*'connecting'/);
});

test('bridge exposes current pairing QR through health only while unpaired', () => {
  assert.match(bridgeSource, /let currentQr\s*=\s*null/);
  assert.match(bridgeSource, /currentQr\s*=\s*qr/);
  assert.match(bridgeSource, /connection\s*===\s*'open'[\s\S]*currentQr\s*=\s*null/);
  assert.match(bridgeSource, /connection\s*===\s*'close'[\s\S]*currentQr\s*=\s*null/);
  assert.match(bridgeSource, /paired:\s*socketLifecycle\.isConnected\(\)/);
  assert.match(bridgeSource, /qr:\s*currentQr/);
});

test('send/edit and media identify the Soul, including text before voice', async () => {
  const routes = {};
  const sent = [];
  const cached = [];
  const prefix = '✦ Beacon: ';
  const context = {
    WHATSAPP_MODE: 'self-chat', REPLY_PREFIX: prefix,
    MAX_MESSAGE_LENGTH: 40, CHUNK_DELAY_MS: 0,
    app: { post: (route, handler) => { routes[route] = handler; } },
    sock: {}, socketLifecycle: { isConnected: () => true },
    presence: { hasUnreadMessages: () => false, postSendPresenceAndUnreadRestore: async () => {} },
    sendWithTimeout: async (_chat, payload) => {
      sent.push(payload);
      return { key: { id: `sent-${sent.length}` } };
    },
    sentStore: { trackSent() {}, storeSent: (_sent, payload) => cached.push(payload) },
    sleep: async () => {}, existsSync: () => true, readFileSync: () => Buffer.from('media'),
    path: { basename: () => 'file.png' },
    buildMediaRetryCachePayload: (_type, payload) => payload,
  };
  // Evaluate the actual formatter and route bodies, without bridge startup or sockets.
  runInNewContext(bridgeSource.slice(
    bridgeSource.indexOf('function formatOutgoingMessage('),
    bridgeSource.indexOf('function normalizeWhatsAppId('),
  ), context);
  runInNewContext(bridgeSource.slice(
    bridgeSource.indexOf("app.post('/send',"), bridgeSource.indexOf('// Typing indicator'),
  ), context);
  let response;
  const res = { json: value => { response = value; }, status: () => res };
  for (const route of ['/send', '/edit']) {
    sent.length = 0;
    const body = 'x'.repeat(85);
    await routes[route]({ body: { chatId: 'fictional@lid', messageId: 'edit-me', message: body } }, res);
    assert.equal(response.success, true);
    assert.equal(sent.length, 3);
    assert.ok(sent.every(payload => payload.text.startsWith(prefix) && payload.text.length <= 40));
    assert.equal(sent.map(payload => payload.text.slice(prefix.length)).join(''), body);
    if (route === '/send') assert.deepEqual(Array.from(response.messageIds), ['sent-1', 'sent-2', 'sent-3']);
    else assert.equal(sent[0].edit.id, 'edit-me');
  }
  for (const mediaType of ['image', 'video', 'document']) {
    for (const caption of ['caption', '']) {
      await routes['/send-media']({ body: { chatId: 'fictional@lid', filePath: 'file.png', mediaType, caption } }, res);
      assert.equal(response.success, true);
      assert.equal(sent.at(-1).caption, `${prefix}${caption}`);
      assert.equal(cached.at(-1).caption, sent.at(-1).caption);
    }
  }
  context.MIME_MAP = {opus: 'audio/ogg'};
  for (const caption of ['', 'voice caption']) {
    sent.length = 0;
    await routes['/send-media']({body: {chatId: 'fictional@lid', filePath: 'file.opus', mediaType: 'audio', caption}}, res);
    assert.equal(response.success, true);
    assert.equal(sent.length, 2);
    assert.equal(sent[0].text, `${prefix}${caption}`);
    assert.ok(sent[1].audio);
    assert.equal(sent[1].ptt, true);
  }
  context.WHATSAPP_MODE = 'bot';
  assert.equal(runInNewContext("formatOutgoingMessages('plain')[0]", context), `${prefix}plain`);
  context.REPLY_PREFIX = 'x'.repeat(40);
  assert.throws(() => runInNewContext("formatOutgoingMessages('plain')", context), /prefix exceeds/);
});

test('selected Soul alone defines the prefix; pair-only cannot send without one', () => {
  const declaration = bridgeSource.slice(bridgeSource.indexOf('const SOUL_ID ='), bridgeSource.indexOf('const MAX_MESSAGE_LENGTH'));
  for (const legacy of ['', 'custom prefix']) {
    const context = {getArg: () => 'Exact Soul', PAIR_ONLY: false, process: {env: {WHATSAPP_REPLY_PREFIX: legacy}}};
    runInNewContext(declaration + '\nglobalThis.prefix = REPLY_PREFIX;', context);
    assert.equal(context.prefix, '✦ Exact Soul: ');
  }
  const context = {getArg: () => '', PAIR_ONLY: true, SEND_TIMEOUT_MS: 1};
  runInNewContext(declaration + '\n' + bridgeSource.slice(
    bridgeSource.indexOf('function sendWithTimeout('), bridgeSource.indexOf('function formatOutgoingMessage('),
  ), context);
  assert.throws(() => runInNewContext("sendWithTimeout('chat', {})", context), /Select a Soul/);
  assert.throws(() => runInNewContext(declaration, {...context, PAIR_ONLY: false}), /selected Soul/);
});
