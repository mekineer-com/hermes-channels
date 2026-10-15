import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { runInNewContext } from 'node:vm';
import { DurableQueue } from './durable_queue.js';
import { createMessageIngest } from './message_ingest.js';

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
    trackRequest: handler => handler,
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

test('bridge EOF preserves inbound messages during Stop and socket teardown', async t => {
  const { EventEmitter } = await import('node:events');
  const process = new EventEmitter();
  process.stdin = new EventEmitter();
  process.stdin.resume = () => {};
  process.stdin.pause = () => {};
  process.env = { CHANNELS_MANAGED_STDIN: '1' };
  const events = new EventEmitter();
  const queueDir = mkdtempSync(join(tmpdir(), 'channels-stop-'));
  t.after(() => rmSync(queueDir, { recursive: true, force: true }));
  const queue = new DurableQueue({ queueDir });
  const payload = (id, type) => ({ type, messages: [{
    key: { remoteJid: '111@s.whatsapp.net', id, fromMe: true },
    message: { conversation: id }, messageTimestamp: 1002,
  }] });
  let releaseWrite, releaseHttp, releaseRequest, releaseCreds;
  let releaseIncoming, releaseClosing;
  const pendingIncoming = new Promise(resolve => { releaseIncoming = resolve; });
  const pendingClosing = new Promise(resolve => { releaseClosing = resolve; });
  const pendingWrite = new Promise(resolve => { releaseWrite = resolve; });
  const pendingRequest = new Promise(resolve => { releaseRequest = resolve; });
  const lateCreds = new Promise(resolve => { releaseCreds = resolve; });
  let currentWrite = pendingWrite;
  const calls = [];
  const context = {
    process, Promise, Set, clearTimeout, logger: { error: (...args) => assert.fail(String(args)) },
    sock: {
      user: { id: '111:1@s.whatsapp.net', name: 'Fixture Human' },
      ev: events,
      end: async () => {
        calls.push('socket-end');
        events.emit('messages.upsert', payload('during-end', 'append'));
      },
    },
    socketLifecycle: { stop: async () => { calls.push('reconnect-stop'); } },
  };
  runInNewContext(bridgeSource.slice(bridgeSource.indexOf('let server;'), bridgeSource.indexOf('async function startSocket()')) +
    '\nglobalThis.control = { shutdown, onSocket, trackRequest, setServer: value => { server = value; } };', context);
  const ingest = createMessageIngest({
    durableQueue: queue,
    identity: { learnAliasFromDm() {} },
    knownState: {
      extractPossibleSenderName: () => '', getChat: () => null, getPushName: () => '',
      rememberChat() {}, rememberPushName() {}, resolveDmDisplayName: () => 'Fixture chat',
    },
    presence: { rememberInboundLastMessage() {} },
    sentStore: { isEcho: () => false },
    normalizeId: value => String(value || '').replace(/:.*@/, '@'),
    getSock: () => context.sock, resolveGroupChatName: async () => '', logger: {},
    config: { allowedUsers: new Set(), bridgeStartedAtSeconds: 1000, startupReplayGraceSeconds: 120, whatsappMode: 'self-chat' },
  });
  context.control.onSocket('messages.upsert', async incoming => {
    await (incoming.messages[0].key.id === 'during-stop' ? pendingIncoming : pendingClosing);
    await ingest.handleUpsert(incoming);
  });
  context.control.setServer({ close: callback => { releaseHttp = callback; } });
  context.control.onSocket('creds.update', () => currentWrite);
  context.control.trackRequest(() => pendingRequest)({}, {}, assert.fail);
  events.emit('creds.update');
  process.stdin.emit('end');
  const stopping = context.control.shutdown();
  assert.equal(context.control.shutdown(), stopping);
  events.emit('messages.upsert', payload('during-stop', 'notify'));
  releaseHttp();
  await Promise.resolve();
  assert.deepEqual(calls, ['reconnect-stop']);
  releaseWrite();
  await Promise.resolve();
  assert.deepEqual(calls, ['reconnect-stop']);
  currentWrite = lateCreds;
  events.emit('creds.update');
  releaseRequest();
  await Promise.resolve();
  assert.deepEqual(calls, ['reconnect-stop']);
  releaseCreds();
  await new Promise(setImmediate);
  assert.deepEqual(calls, ['reconnect-stop']);
  assert.equal(queue.readUnacked(10).length, 0);
  releaseIncoming();
  let stopped = false;
  stopping.then(() => { stopped = true; });
  await new Promise(setImmediate);
  assert.deepEqual(calls, ['reconnect-stop', 'socket-end']);
  assert.equal(stopped, false);
  assert.deepEqual(queue.readUnacked(10).map(row => row.messageId), ['during-stop']);
  releaseClosing();
  await stopping;
  const reopened = new DurableQueue({ queueDir });
  assert.deepEqual(reopened.readUnacked(10).map(row => [row.messageId, row.body, row.deliveryMode]), [
    ['during-stop', 'during-stop', 'live'], ['during-end', 'during-end', 'persist_only'],
  ]);
});

test('bridge records synchronous socket handler failures during Stop', async () => {
  const { EventEmitter } = await import('node:events');
  const process = new EventEmitter();
  process.env = {};
  process.stdin = { pause() {} };
  const events = new EventEmitter();
  const errors = [];
  const context = {
    process, Promise, Set, clearTimeout, logger: { error: (...args) => errors.push(args) },
    sock: { ev: events, end: async () => {} }, socketLifecycle: { stop: async () => {} },
  };
  runInNewContext(bridgeSource.slice(bridgeSource.indexOf('let server;'), bridgeSource.indexOf('async function startSocket()')) +
    '\nglobalThis.control = { shutdown, onSocket };', context);
  const failure = new Error('Fixture ingest failure');
  context.control.onSocket('messages.update', () => { throw failure; });
  const stopping = context.control.shutdown();
  assert.doesNotThrow(() => events.emit('messages.update', []));
  await stopping;
  assert.equal(errors.length, 1);
  assert.equal(errors[0][0].err, failure);
  assert.equal(errors[0][1], 'socket handler failed');
  assert.equal(process.exitCode, 1);
});

test('bridge ignores connection lifecycle actions during Stop', () => {
  let handler;
  runInNewContext(bridgeSource.slice(bridgeSource.indexOf("  onSocket('connection.update',"), bridgeSource.indexOf("  onSocket('messages.upsert',")), {
    stopping: true, onSocket: (_event, callback) => { handler = callback; },
  });
  for (const update of [{ connection: 'open' }, { connection: 'close' }, { qr: 'fixture-qr' }]) {
    assert.doesNotThrow(() => handler(update));
  }
});

test('bridge does not create a socket when stop arrives during auth/version startup', async () => {
  let releaseVersion;
  const version = new Promise(resolve => { releaseVersion = resolve; });
  const lifecycle = { stopping: false, beginStart: () => 1, fetchVersion: () => version };
  const context = {
    socketLifecycle: lifecycle, SESSION_DIR: '/fictional',
    identity: { setKeyStore() {} }, useMultiFileAuthState: async () => ({ state: { keys: {} } }),
    makeWASocket: () => assert.fail('late socket creation'),
  };
  runInNewContext(bridgeSource.slice(bridgeSource.indexOf('async function startSocket()'), bridgeSource.indexOf('// HTTP server')) +
    '\nglobalThis.start = startSocket;', context);
  const starting = context.start();
  await Promise.resolve();
  lifecycle.stopping = true;
  releaseVersion([1, 2, 3]);
  await starting;
});

for (const outcome of ['resolve', 'reject']) {
  test(`bridge drains raw send after HTTP timeout until it ${outcome}s`, async () => {
    const { EventEmitter } = await import('node:events');
    const process = new EventEmitter();
    process.env = {};
    process.stdin = { pause() {} };
    let resolveSend, rejectSend, expire;
    const rawSend = new Promise((resolve, reject) => { resolveSend = resolve; rejectSend = reject; });
    const calls = [];
    const context = {
      process, Promise, Set, SOUL_ID: 'Beacon', SEND_TIMEOUT_MS: 60_000,
      setTimeout: callback => { expire = callback; }, clearTimeout() {},
      logger: { error: (...args) => assert.fail(String(args)) },
      sock: { sendMessage: () => rawSend, end: async () => { calls.push('socket-end'); } },
      socketLifecycle: { stop: async () => {} },
    };
    runInNewContext(bridgeSource.slice(bridgeSource.indexOf('let server;'), bridgeSource.indexOf('async function startSocket()')) +
      bridgeSource.slice(bridgeSource.indexOf('function sendWithTimeout('), bridgeSource.indexOf('function formatOutgoingMessage(')) +
      '\nglobalThis.control = { shutdown, trackRequest, sendWithTimeout };', context);
    const request = context.control.trackRequest(async () => {
      await assert.rejects(context.control.sendWithTimeout('fictional@lid', {}), /sendMessage timed out/);
      calls.push('http-timeout');
    })({}, {}, assert.fail);
    expire();
    await request;
    assert.deepEqual(calls, ['http-timeout']);
    const stopping = context.control.shutdown();
    try {
      await new Promise(setImmediate);
      assert.deepEqual(calls, ['http-timeout']);
    } finally {
      if (outcome === 'resolve') resolveSend({ key: { id: 'fictional-sent' } });
      else rejectSend(new Error('Fixture send failure'));
      await stopping;
    }
    assert.deepEqual(calls, ['http-timeout', 'socket-end']);
  });
}
