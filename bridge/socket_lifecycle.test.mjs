import test from 'node:test';
import assert from 'node:assert/strict';

import { SocketLifecycle } from './socket_lifecycle.js';

const quietLogger = {
  log() {},
  warn() {},
  error() {},
};

function makeLifecycle(overrides = {}) {
  return new SocketLifecycle({
    baileysVersionFetchTimeoutMs: 5000,
    baileysVersionFallback: [2, 3000, 1023223821],
    onStart: async () => {},
    logger: quietLogger,
    ...overrides,
  });
}

test('socket lifecycle does not report connected until socket open', () => {
  const lifecycle = makeLifecycle();
  const socketId = lifecycle.beginStart();

  lifecycle.markReady(socketId);
  assert.equal(lifecycle.getState(), 'connecting');
  assert.equal(lifecycle.isConnected(), false);

  lifecycle.markOpen(socketId);
  assert.equal(lifecycle.getState(), 'connected');
  assert.equal(lifecycle.isConnected(), true);
});

test('socket lifecycle ignores stale socket updates', () => {
  const lifecycle = makeLifecycle();
  const staleSocketId = lifecycle.beginStart();
  lifecycle.beginStart();

  lifecycle.markOpen(staleSocketId);
  assert.equal(lifecycle.getState(), 'connecting');
});

test('socket lifecycle falls back when Baileys version fetch is invalid', async () => {
  const lifecycle = makeLifecycle({
    fetchLatestBaileysVersionFn: async ({ timeout }) => {
      assert.equal(timeout, 5000);
      return { version: null };
    },
  });

  assert.deepEqual(await lifecycle.fetchVersion(), [2, 3000, 1023223821]);
});

test('stop cancels reconnect and waits for an admitted socket start without retry', async () => {
  let failStart;
  let starts = 0;
  const lifecycle = makeLifecycle({ onStart: () => {
    starts += 1;
    return new Promise((_resolve, reject) => { failStart = reject; });
  } });
  lifecycle.startNow();
  lifecycle.scheduleStart(10);
  const stopped = lifecycle.stop();
  assert.equal(lifecycle.stop(), stopped);
  assert.equal(lifecycle.reconnectTimer, null);
  failStart(new Error('fictional start failure during drain'));
  await stopped;
  lifecycle.scheduleStart(10);
  lifecycle.startNow();
  assert.equal(lifecycle.reconnectTimer, null);
  assert.equal(starts, 1);
});
