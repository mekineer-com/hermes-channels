import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { EventEmitter } from 'node:events';
import { createRequire } from 'node:module';
import { runInNewContext } from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const source = readFileSync(join(here, 'source-daemon.js'), 'utf8');

test('source daemon help preserves active-scope and diagnostics flags', () => {
  const output = execFileSync(process.execPath, [join(here, 'source-daemon.js'), '--help'], {
    encoding: 'utf8',
  });

  assert.match(output, /--active-since EPOCH/);
  assert.match(output, /--memory-diagnostics-interval SECONDS/);
  assert.match(output, /--backfill-since EPOCH/);
  assert.match(output, /--no-contact-snapshot/);
});

test('source daemon persists QR payload without logging it', () => {
  assert.match(source, /status\.write\(\{\s*state:\s*'pairing',\s*qr,/);
  assert.doesNotMatch(source, /console\.log\(qr\)/);
  assert.match(source, /state:\s*'authenticated',\s*qr:\s*null/);
  assert.match(source, /state:\s*'auth_failure',\s*qr:\s*null/);
  assert.match(source, /state:\s*'ready',\s*qr:\s*null/);
});

test('source daemon distinguishes logout for pairing recovery', () => {
  assert.match(source, /String\(reason\)\.toUpperCase\(\) === 'LOGOUT' \? 42 : 1/);
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

async function fixture({ startupBlocked = false, backfillMode = null } = {}) {
  const calls = [], statuses = [];
  const enrichment = deferred(), writerExit = deferred(), initialize = deferred();
  const backfill = deferred();
  const browserProcess = new EventEmitter();
  Object.assign(browserProcess, { exitCode: null, signalCode: null });
  const browser = {
    process: () => browserProcess,
    target: () => ({ createCDPSession: async () => ({ send: async (method, _args, options) => {
      assert.equal(method, 'Browser.close');
      assert.equal(options.timeout, 0);
      calls.push('browser-close-request');
    } }) }),
    disconnect: async () => { calls.push('browser-disconnect'); },
    close: () => assert.fail('Puppeteer force-capable close must not run'),
  };
  let client;
  class Client extends EventEmitter {
    constructor() {
      super();
      client = this;
      this.pupBrowser = browser;
      this.pupPage = { exposeFunction: async () => {} };
    }
    async initialize() {
      calls.push('initialize');
      if (startupBlocked) await initialize.promise;
      this.emit('ready');
    }
    async destroy() {
      calls.push('destroy');
      this.emit('disconnected', 'LOGOUT');
    }
    async getChatById() { return {}; }
  }
  class StoreWriter {
    async command(op) { calls.push(op); return {}; }
    drain() { calls.push('writer-drain'); }
    async close() { calls.push('writer-eof'); await writerExit.promise; calls.push('writer-exit'); }
  }
  class ContactManager {
    async persistForMessageRow() {
      await enrichment.promise;
      calls.push('contact-write');
    }
    async snapshot() {}
    async pruneScopeOnce() {}
    schedule() { calls.push('contact-schedule'); }
    stop() { calls.push('contact-stop'); }
  }
  class MemoryDiagnostics {
    schedule() {}
    stop() {}
  }
  class StatusWriter {
    write(row) { calls.push(`status:${row.state}`); statuses.push(row); }
    flush() { calls.push('status-flush'); }
  }
  const process = new EventEmitter();
  process.stdin = new EventEmitter();
  process.stdin.resume = () => {};
  process.stdin.pause = () => {};
  process.env = { CHANNELS_MANAGED_STDIN: '1', CHANNELS_HOME: '/fictional-disposable' };
  process.argv = ['node', 'source-daemon.js'];
  if (backfillMode === 'since') process.argv.push('--backfill-since', '1');
  if (backfillMode === 'chat') process.argv.push('--backfill-chat', 'fictional-chat');
  process.exit = code => assert.fail(`early process.exit(${code})`);
  const realRequire = createRequire(import.meta.url);
  const stubs = {
    'whatsapp-web.js': { Client, LocalAuth: class {}, Events: new Proxy({}, { get: (_target, event) => event.toLowerCase() }) },
    './store-writer': { StoreWriter }, './contact-manager': { ContactManager },
    './backfill-manager': { BackfillManager: class {
      chatsSince() { return backfill.promise; }
      chatMessages() { return backfill.promise; }
    } },
    './memory-diagnostics': { MemoryDiagnostics, memoryStatsMb: () => ({}) },
    './status-writer': { StatusWriter }, './browser-options': { buildClientOptions: () => ({}) },
    './page-hooks': { configureResourceBlocking: async () => {}, installRemoveMessageHook: async () => {} },
    './normalization': { normalizeMessage: m => ({ msg_key: m.id }), messageKey: m => m.id, jidLocal: id => id },
    './daemon-utils': { ...realRequire('./daemon-utils'), ensureDir() {} },
  };
  const context = { process, console, __dirname: here, require: name => stubs[name] || realRequire(name) };
  runInNewContext(source.slice(0, source.lastIndexOf('main().catch(')) + '\nglobalThis.started = main();', context);
  await new Promise(setImmediate);
  return { calls, statuses, process, client, browserProcess, enrichment, writerExit, initialize, backfill, started: context.started };
}

for (const backfillMode of ['since', 'chat']) {
  for (const disconnected of [false, true]) {
    test(`${backfillMode} backfill failure preserves readiness after disconnect=${disconnected}`, async () => {
      const f = await fixture({ backfillMode });
      await f.started;
      if (disconnected) f.client.emit('disconnected', 'Fixture connection lost');
      f.backfill.reject(new Error('Fixture backfill failure'));
      await new Promise(setImmediate);
      const failed = f.statuses.find(row => row.error === 'Fixture backfill failure');
      try {
        assert.equal(failed?.state, 'degraded');
        assert.equal(failed.wwebjs_ready, !disconnected);
      } finally {
        f.process.stdin.emit('end');
        await new Promise(setImmediate);
        f.browserProcess.exitCode = 0;
        f.browserProcess.emit('close', 0);
        f.writerExit.resolve();
        await new Promise(setImmediate);
      }
      assert.equal(f.process.exitCode, disconnected ? 1 : 0);
    });
  }
}

for (const backfillMode of ['since', 'chat']) {
  for (const reason of [null, 'Fixture connection lost', 'LOGOUT']) {
    test(`${backfillMode} late success cannot leave shutdown paired (${reason || 'Stop'})`, async () => {
      const f = await fixture({ backfillMode });
      await f.started;
      if (reason) f.client.emit('disconnected', reason);
      else f.process.stdin.emit('end');
      f.backfill.resolve({
        scannedChats: 1, fetched: 1, inserted: 1, updated: 0,
        backfilledChats: 1, incompleteChatIds: [], skippedBeforeSince: 0, reconciledRevoked: 0,
      });
      await new Promise(setImmediate);
      assert.ok(f.statuses.some(row => row.last_backfill_at), 'Late successful backfill must run');
      assert.ok(f.calls.includes('browser-close-request'));
      assert.ok(!f.calls.includes('writer-eof'));
      f.browserProcess.exitCode = 0;
      f.browserProcess.emit('close', 0);
      f.writerExit.resolve();
      await new Promise(setImmediate);
      const final = Object.assign({}, ...f.statuses);
      assert.equal(final.state, reason ? 'degraded' : 'stopping');
      assert.equal(final.wwebjs_ready, false);
      assert.equal(f.process.exitCode, reason === 'LOGOUT' ? 42 : reason ? 1 : 0);
      assert.equal(f.calls.at(-1), 'status-flush');
    });
  }
}

test('fatal shutdown clears readiness after admitted message persistence succeeds', async () => {
  const f = await fixture();
  await f.started;
  f.client.emit('message_create', { id: 'fictional-pending-message' });
  f.process.emit('unhandledRejection', new Error('Fixture fatal failure'));
  await new Promise(setImmediate);
  assert.ok(!f.calls.includes('browser-close-request'));
  f.enrichment.resolve();
  await new Promise(setImmediate);
  assert.ok(f.calls.includes('upsert_message'));
  assert.ok(f.calls.includes('browser-close-request'));
  f.browserProcess.exitCode = 0;
  f.browserProcess.emit('close', 0);
  f.writerExit.resolve();
  await new Promise(setImmediate);
  const final = Object.assign({}, ...f.statuses);
  assert.equal(final.state, 'degraded');
  assert.equal(final.wwebjs_ready, false);
  assert.equal(f.process.exitCode, 1);
});

test('authentication failure stays degraded after shutdown', async () => {
  const f = await fixture();
  await f.started;
  f.client.emit('auth_failure', 'Fictional authentication failure');
  await new Promise(setImmediate);
  f.browserProcess.exitCode = 0;
  f.browserProcess.emit('close', 0);
  f.writerExit.resolve();
  await new Promise(setImmediate);
  const final = Object.assign({}, ...f.statuses);
  assert.equal(final.state, 'degraded');
  assert.equal(final.error, 'Fictional authentication failure');
  assert.equal(final.wwebjs_ready, false);
  assert.equal(f.process.exitCode, 1);
});

test('shutdown failure clears readiness after late success', async () => {
  const f = await fixture({ backfillMode: 'chat' });
  await f.started;
  f.client.emit('disconnected', 'Fixture connection lost');
  f.backfill.resolve({ fetched: 1, inserted: 1, updated: 0 });
  await new Promise(setImmediate);
  f.browserProcess.exitCode = 0;
  f.browserProcess.emit('close', 0);
  f.writerExit.reject(new Error('Fixture writer close failure'));
  await new Promise(setImmediate);
  const final = Object.assign({}, ...f.statuses);
  assert.equal(final.state, 'degraded');
  assert.equal(final.error, 'Fixture writer close failure');
  assert.equal(final.wwebjs_ready, false);
  assert.equal(f.process.exitCode, 1);
});

for (const signal of ['SIGINT', 'SIGTERM', 'end']) {
  test(`source ${signal} drains contact enrichment, browser exit and writer exit in order`, async () => {
    const f = await fixture();
    await f.started;
    f.client.emit('message_create', { id: 'fictional-message' });
    await new Promise(setImmediate);
    const target = signal === 'end' ? f.process.stdin : f.process;
    target.emit(signal);
    target.emit(signal);
    f.client.emit('message_create', { id: 'not-admitted' });
    await new Promise(setImmediate);
    assert.ok(f.calls.includes('writer-drain'));
    assert.ok(!f.calls.includes('browser-close-request'));
    assert.ok(!f.calls.includes('writer-eof'));
    f.enrichment.resolve();
    await new Promise(setImmediate);
    assert.ok(f.calls.includes('contact-write'));
    assert.ok(f.calls.includes('browser-close-request'));
    assert.ok(!f.calls.includes('destroy'));
    f.browserProcess.exitCode = 0;
    f.browserProcess.emit('close', 0);
    await new Promise(setImmediate);
    assert.ok(f.calls.includes('destroy'));
    assert.ok(f.calls.includes('writer-eof'));
    assert.equal(f.process.exitCode, undefined);
    f.writerExit.resolve();
    await new Promise(setImmediate);
    assert.equal(f.process.exitCode, 0);
    assert.equal(f.calls.filter(call => call === 'upsert_message').length, 1);
    assert.equal(f.calls.filter(call => call === 'destroy').length, 1);
    assert.equal(f.calls.at(-1), 'status-flush');
  });
}

test('source EOF during initialize waits for startup and never schedules new ready work', async () => {
  const f = await fixture({ startupBlocked: true });
  f.process.stdin.emit('end');
  await new Promise(setImmediate);
  assert.ok(!f.calls.includes('browser-close-request'));
  f.initialize.resolve();
  await f.started;
  await new Promise(setImmediate);
  assert.ok(!f.calls.includes('contact-schedule'));
  assert.ok(f.calls.includes('browser-close-request'));
  f.browserProcess.exitCode = 0;
  f.browserProcess.emit('close', 0);
  f.writerExit.resolve();
  await new Promise(setImmediate);
  assert.equal(f.process.exitCode, 0);
});

test('source retains a fatal outcome arriving during ordinary shutdown', async () => {
  const f = await fixture({ startupBlocked: true });
  f.process.stdin.emit('end');
  f.process.emit('unhandledRejection', new Error('Fixture late failure'));
  f.process.stdin.emit('end');
  f.initialize.resolve();
  await f.started;
  await new Promise(setImmediate);
  assert.ok(f.calls.includes('status:degraded'));
  assert.ok(f.calls.includes('browser-close-request'));
  f.browserProcess.exitCode = 0;
  f.browserProcess.emit('close', 0);
  f.writerExit.resolve();
  await new Promise(setImmediate);
  assert.equal(f.calls.filter(call => call === 'writer-drain').length, 1);
  assert.equal(f.calls.filter(call => call === 'destroy').length, 1);
  assert.equal(f.statuses.at(-1).state, 'degraded');
  assert.equal(f.statuses.at(-1).wwebjs_ready, false);
  assert.equal(f.process.exitCode, 1);
});
