import test from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';

import { DurableQueue } from './durable_queue.js';

function mkQueueDir() {
  return mkdtempSync(path.join(os.tmpdir(), 'hermes-wa-queue-'));
}

function readLines(filePath) {
  const raw = readFileSync(filePath, 'utf8');
  return raw.split('\n').map((line) => line.trim()).filter(Boolean);
}

test('durable queue preserves unacked messages across restart and ack', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const one = queue.enqueue({
      messageId: 'm1',
      chatId: '247789598601266@lid',
      senderId: '247789598601266@lid',
      senderName: 'Liz',
      isGroup: false,
      body: 'hello',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 1,
    });
    const two = queue.enqueue({
      messageId: 'm2',
      chatId: '247789598601266@lid',
      senderId: '247789598601266@lid',
      senderName: 'Liz',
      isGroup: false,
      body: 'world',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 2,
    });
    assert.equal(one.seq, 1);
    assert.equal(two.seq, 2);

    const duplicate = queue.enqueue({
      messageId: 'm2',
      chatId: '247789598601266@lid',
      senderId: '247789598601266@lid',
      senderName: 'Liz',
      isGroup: false,
      body: 'world duplicate',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 3,
    });
    assert.equal(duplicate, two);

    assert.deepEqual(queue.readUnacked(10).map((item) => item.seq), [1, 2]);

    const ack1 = queue.ackThrough(1);
    assert.equal(ack1.ackedUpToSeq, 1);
    assert.deepEqual(queue.readUnacked(10).map((item) => item.seq), [2]);

    const reloaded = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    assert.equal(reloaded.getStats().ackedUpToSeq, 1);
    assert.deepEqual(reloaded.readUnacked(10).map((item) => item.seq), [2]);
    assert.equal(reloaded.getStats().nextSeq, 3);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue compacts on ack threshold', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    queue.enqueue({
      messageId: 'g1',
      chatId: '12025550100-1600000000@g.us',
      senderId: 'raquel@lid',
      senderName: 'Test Contact',
      isGroup: true,
      body: 'first',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 1,
    });
    queue.enqueue({
      messageId: 'g2',
      chatId: '12025550100-1600000000@g.us',
      senderId: 'test-user@lid',
      senderName: 'Test User',
      isGroup: true,
      body: 'second',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 2,
    });

    queue.ackThrough(1);
    const queueLines = readLines(path.join(queueDir, 'queue.jsonl'));
    assert.equal(queueLines.length, 1);
    const remaining = JSON.parse(queueLines[0]);
    assert.equal(remaining.seq, 2);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue does not serve malformed rows without seq', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    queue.unacked.push({ body: 'bad' });
    const good = queue.enqueue({
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'good',
      timestamp: 1,
    });

    assert.deepEqual(queue.readUnacked(10).map((item) => item.seq), [good.seq]);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue bootstraps seen ids from queue rows when seen file is missing', () => {
  const queueDir = mkQueueDir();
  try {
    const legacyRow = {
      seq: 1,
      event_uid: '114628432556258@lid:ACCB6730B9B318CD8D20AF8EA94082E1:12025550199@s.whatsapp.net',
      messageId: 'ACCB6730B9B318CD8D20AF8EA94082E1',
      chatId: '114628432556258@lid',
      senderId: '12025550199@s.whatsapp.net',
      senderName: 'Test User',
      isGroup: false,
      body: 'Please respond privately.',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 1,
    };
    writeFileSync(path.join(queueDir, 'queue.jsonl'), `${JSON.stringify(legacyRow)}\n`, 'utf8');
    writeFileSync(path.join(queueDir, 'queue.offset'), '1\n', 'utf8');

    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const seenLines = readLines(path.join(queueDir, 'queue.seen'));
    assert.deepEqual(seenLines, ['persist_only\t114628432556258@lid:ACCB6730B9B318CD8D20AF8EA94082E1\t1']);

    const duplicate = queue.enqueue({
      messageId: 'ACCB6730B9B318CD8D20AF8EA94082E1',
      chatId: '114628432556258@lid',
      senderId: '114628432556258@lid',
      senderName: 'Test User',
      isGroup: false,
      body: 'same message different sender alias',
      hasMedia: false,
      mediaType: '',
      mediaUrls: [],
      mentionedIds: [],
      quotedMessageId: null,
      quotedParticipant: null,
      quotedRemoteJid: null,
      hasQuotedMessage: false,
      botIds: [],
      timestamp: 2,
    });
    assert.equal(duplicate, null);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue gives deliveryMode revoke a distinct uid from the original message', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const original = queue.enqueue({
      deliveryMode: 'live',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'hello',
      timestamp: 1,
    });
    const revoke = queue.enqueue({
      deliveryMode: 'revoke',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      timestamp: 2,
    });

    assert.equal(original.event_uid, '12025550199@s.whatsapp.net:m1');
    assert.equal(revoke.event_uid, 'revoke:12025550199@s.whatsapp.net:m1');
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue merges history rows into later live rows', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const history = queue.enqueue({
      eventType: 'history_message',
      deliveryMode: 'persist_only',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      senderName: 'Old name',
      fromMe: true,
      body: 'history body',
      timestamp: 1,
    });
    const queueLinesAfterHistory = readLines(path.join(queueDir, 'queue.jsonl'));
    const live = queue.enqueue({
      deliveryMode: 'live',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      senderName: 'Live name',
      fromMe: false,
      body: 'live body',
      timestamp: 2,
    });
    const queueLinesAfterLive = readLines(path.join(queueDir, 'queue.jsonl'));
    const duplicateLive = queue.enqueue({
      deliveryMode: 'live',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'hello again',
      timestamp: 2,
    });

    assert.equal(history, live);
    assert.equal(history.event_uid, '12025550199@s.whatsapp.net:m1');
    assert.equal(history.deliveryMode, 'live');
    assert.equal(history.eventType, undefined);
    assert.equal(history.senderName, 'Live name');
    assert.equal(history.fromMe, false);
    assert.equal(history.body, 'live body');
    assert.equal(history.timestamp, 1);
    assert.equal(queueLinesAfterHistory.length, 1);
    assert.equal(queueLinesAfterLive.length, 1);
    assert.equal(duplicateLive.event_uid, '12025550199@s.whatsapp.net:m1');
    assert.deepEqual(queue.readUnacked(10).map((item) => item.seq), [1]);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});


test('durable queue lets a live row upgrade a previously seen history row after restart', () => {
  const queueDir = mkQueueDir();
  try {
    let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const history = queue.enqueue({
      eventType: 'history_message',
      deliveryMode: 'persist_only',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'history body',
      timestamp: 1,
    });
    queue.ackThrough(history.seq);

    queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const live = queue.enqueue({
      deliveryMode: 'live',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'history body',
      timestamp: 2,
    });
    const duplicateHistory = queue.enqueue({
      eventType: 'history_message',
      deliveryMode: 'persist_only',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'history body',
      timestamp: 1,
    });

    assert.equal(live.event_uid, '12025550199@s.whatsapp.net:m1');
    assert.equal(live.deliveryMode, 'live');
    assert.equal(live.timestamp, 1);
    assert.equal(duplicateHistory, live);
    assert.equal(live.eventType, undefined);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('durable queue normalizes long-shaped timestamps before live upgrade', () => {
  const queueDir = mkQueueDir();
  try {
    let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const history = queue.enqueue({
      eventType: 'history_message',
      deliveryMode: 'persist_only',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'history body',
      timestamp: { low: 1000, high: 0, unsigned: true },
    });
    queue.ackThrough(history.seq);

    queue = new DurableQueue({ queueDir, compactionEveryAcks: 1000 });
    const live = queue.enqueue({
      deliveryMode: 'live',
      messageId: 'm1',
      chatId: '12025550199@s.whatsapp.net',
      senderId: '12025550199@s.whatsapp.net',
      body: 'history body',
      timestamp: 2000,
    });

    assert.equal(live.timestamp, 1000);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

for (const completeFirst of [false, true]) {
  for (const stage of ['pending', 'acked', 'restarted']) {
    test(`history media completion survives ${stage}, completeFirst=${completeFirst}`, () => {
      const queueDir = mkQueueDir();
      try {
        let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
        const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
          hasMedia: true, mediaType: 'document', body: 'fictional document', timestamp: 1000 };
        const file = '/fictional/document.txt';
        const first = queue.enqueue({ ...base, mediaUrls: completeFirst ? [file] : [] });
        if (stage !== 'pending') queue.ackThrough(first.seq);
        if (stage === 'restarted') queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });

        const later = queue.enqueue({ ...base, timestamp: 2000, mediaUrls: completeFirst ? [] : [file] });
        const complete = completeFirst ? first : later;
        assert.ok(complete, 'a complete history copy must not be discarded after ACK');
        assert.equal(complete.deliveryMode, 'persist_only');
        assert.equal(complete.timestamp, 1000);
        assert.deepEqual(complete.mediaUrls, [file]);
        assert.equal(queue.readUnacked(10).length, completeFirst && stage !== 'pending' ? 0 : 1);
        if (stage === 'pending') assert.equal(later, first);
        else if (completeFirst) assert.equal(later, null);

        queue.ackThrough(complete.seq);
        queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
        assert.equal(queue.enqueue({ ...base, mediaUrls: [file] }), null);
        assert.equal(queue.enqueue({ ...base, mediaUrls: [] }), null);
        const live = queue.enqueue({ ...base, deliveryMode: 'live', mediaUrls: [file], timestamp: 3000 });
        assert.equal(live.deliveryMode, 'live');
        assert.equal(live.timestamp, 1000);
        queue.ackThrough(live.seq);
        assert.equal(queue.enqueue({ ...base, mediaUrls: [file] }), null);
      } finally {
        rmSync(queueDir, { recursive: true, force: true });
      }
    });
  }
}

for (const acked of [false, true]) {
  test(`complete history media does not upgrade a known revoke, acked=${acked}`, () => {
    const queueDir = mkQueueDir();
    try {
      let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
      const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
        hasMedia: true, mediaType: 'document', timestamp: 1000, mediaUrls: [] };
      const first = queue.enqueue(base);
      if (acked) queue.ackThrough(first.seq);
      const revoke = queue.enqueue({ ...base, deliveryMode: 'revoke' });
      if (acked) {
        queue.ackThrough(revoke.seq);
        queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
      }
      assert.equal(queue.enqueue({ ...base, mediaUrls: ['/fictional/document.txt'] }), null);
      assert.equal(queue.enqueue({ ...base, deliveryMode: 'live', mediaUrls: ['/fictional/document.txt'] }), null);
      if (!acked) assert.deepEqual(first.mediaUrls, []);
      assert.ok(queue.enqueue({ ...base, messageId: 'replacement-1', deliveryMode: 'live' }));
    } finally {
      rmSync(queueDir, { recursive: true, force: true });
    }
  });
}

test('a failed merged-payload write cannot commit media completeness first', () => {
  const queueDir = mkQueueDir();
  try {
    let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
      hasMedia: true, mediaType: 'document', timestamp: 1000, mediaUrls: [] };
    const first = queue.enqueue(base);
    const seenBefore = readFileSync(queue.seenPath, 'utf8');
    queue._compact = () => { throw new Error('Fictional payload persistence failure'); };
    assert.throws(() => queue.enqueue({ ...base, mediaUrls: ['/fictional/document.txt'] }), /payload persistence/);
    assert.equal(readFileSync(queue.seenPath, 'utf8'), seenBefore);
    queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    assert.deepEqual(queue.readUnacked(10)[0].mediaUrls, []);
    queue.ackThrough(first.seq);
    assert.ok(queue.enqueue({ ...base, mediaUrls: ['/fictional/document.txt'] }));
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

test('reload repairs seen metadata after payload commits but seen persistence fails', () => {
  const queueDir = mkQueueDir();
  try {
    let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
      hasMedia: true, mediaType: 'document', timestamp: 1000, mediaUrls: [] };
    const first = queue.enqueue(base);
    queue._persistSeen = () => { throw new Error('Fictional seen persistence failure'); };
    assert.throws(() => queue.enqueue({ ...base, mediaUrls: ['/fictional/document.txt'] }), /seen persistence/);
    queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    assert.deepEqual(queue.readUnacked(10)[0].mediaUrls, ['/fictional/document.txt']);
    queue.ackThrough(first.seq);
    queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
    assert.equal(queue.enqueue({ ...base, mediaUrls: ['/fictional/document.txt'] }), null);
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});

for (const live of [false, true]) {
  for (const restart of [false, true]) {
    test(`a handed-out history snapshot cannot ACK away an upgrade, live=${live}, restart=${restart}`, () => {
      const queueDir = mkQueueDir();
      try {
        let queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
        const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
          hasMedia: true, mediaType: 'document', timestamp: 1000, mediaUrls: [] };
        queue.enqueue(base);
        const held = JSON.parse(JSON.stringify(queue.readUnacked(100)));
        if (restart) queue = new DurableQueue({ queueDir, compactionEveryAcks: 1 });
        const upgraded = queue.enqueue({ ...base, deliveryMode: live ? 'live' : 'persist_only',
          timestamp: 2000, mediaUrls: ['/fictional/document.txt'] });
        assert.ok(upgraded.seq > held[0].seq);
        assert.deepEqual(held[0].mediaUrls, []);
        queue.ackThrough(held[0].seq);
        const remaining = queue.readUnacked(100);
        assert.equal(remaining.length, 1);
        assert.deepEqual(remaining[0].mediaUrls, ['/fictional/document.txt']);
        assert.equal(remaining[0].timestamp, 1000);
      } finally {
        rmSync(queueDir, { recursive: true, force: true });
      }
    });
  }
}

test('an appended live upgrade preserves the handed-out history attachment', () => {
  const queueDir = mkQueueDir();
  try {
    const queue = new DurableQueue({ queueDir });
    const base = { chatId: 'fictional@g.us', messageId: 'media-1', deliveryMode: 'persist_only',
      hasMedia: true, mediaType: 'document', timestamp: 1000, mediaUrls: ['/fictional/document.txt'] };
    queue.enqueue(base);
    const held = JSON.parse(JSON.stringify(queue.readUnacked(100)));
    const live = queue.enqueue({ ...base, deliveryMode: 'live', mediaUrls: [], timestamp: 2000 });
    assert.ok(live.seq > held[0].seq);
    assert.deepEqual(live.mediaUrls, ['/fictional/document.txt']);
    assert.equal(held[0].deliveryMode, 'persist_only');
  } finally {
    rmSync(queueDir, { recursive: true, force: true });
  }
});
