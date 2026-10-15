import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { spawn } from 'node:child_process';
import { once } from 'node:events';

import { StoreWriter } from './store-writer.js';

test('StoreWriter round-trips commands through the Python store process', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'web-source-store-'));
  const writer = new StoreWriter(join(dir, 'web_source.db'));
  try {
    const pong = await writer.command('ping');
    assert.equal(pong.status, 'ok');

    await writer.command('set_metadata', { key: 'test:key', value: 'value' });
    const row = await writer.command('get_metadata', { key: 'test:key' });
    assert.equal(row.value, 'value');
  } finally {
    await writer.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('StoreWriter command timeout kills hung store and rejects with descriptive error', { skip: process.platform === 'win32' }, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'web-source-store-timeout-'));
  let exitError;
  const writer = new StoreWriter(join(dir, 'web_source.db'), (err) => { exitError = err; });
  try {
    // Confirm the process is up.
    await writer.command('ping');

    // Suspend the store process so it can't respond to the next command.
    writer.proc.kill('SIGSTOP');

    // Issue a command with a short timeout — the stopped process won't respond.
    await assert.rejects(
      () => writer.command('ping', {}, 50),
      (err) => {
        assert.match(err.message, /timed out/);
        assert.match(err.message, /op=ping/);
        return true;
      },
    );

    // The SIGKILL should have triggered the exit handler.
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.ok(exitError, 'onExit callback should have been called after SIGKILL');
    assert.ok(writer.exitedError, 'exitedError should be set after process death');

    // Subsequent commands must reject immediately with the exit error.
    await assert.rejects(() => writer.command('ping'));
  } finally {
    await assert.rejects(writer.close(), /store writer exited/);
    rmSync(dir, { recursive: true, force: true });
  }
});

test('graceful close clears existing and new deadlines, drains writes and awaits child closure', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'web-source-store-drain-'));
  const db = join(dir, 'web_source.db');
  const writer = new StoreWriter(db);
  let locker, lockClosed;
  try {
    await writer.command('ping');
    locker = spawn(process.env.CHANNELS_STORE_PYTHON || 'python3', ['-u', '-c',
      'import sqlite3,sys; con=sqlite3.connect(sys.argv[1]); con.execute("BEGIN IMMEDIATE"); print("locked", flush=True); sys.stdin.readline(); con.rollback(); con.close()', db],
    { stdio: ['pipe', 'pipe', 'inherit'], windowsHide: true });
    lockClosed = once(locker, 'close');
    await once(locker.stdout, 'data');
    const write = writer.command('set_metadata', { key: 'fictional', value: 'durable' }, 20);
    writer.drain();
    const lateWrite = writer.command('set_metadata', { key: 'late-contact', value: 'saved' }, 20);
    const closing = writer.close();
    assert.equal(writer.close(), closing);
    let closed = false;
    closing.then(() => { closed = true; });
    await new Promise(resolve => setTimeout(resolve, 60));
    assert.equal(writer.exitedError, null);
    assert.equal(closed, false);
    locker.stdin.end('\n');
    await Promise.all([write, lateWrite]);
    await closing;
    assert.equal(writer.proc.exitCode, 0);
    assert.equal(writer.pending.size, 0);
    const reader = new StoreWriter(db);
    try {
      assert.equal((await reader.command('get_metadata', { key: 'fictional' })).value, 'durable');
      assert.equal((await reader.command('get_metadata', { key: 'late-contact' })).value, 'saved');
    } finally {
      await reader.close();
    }
  } finally {
    if (locker) locker.stdin.end();
    if (lockClosed) await lockClosed;
    await writer.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
