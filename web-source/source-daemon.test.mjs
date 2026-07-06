import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

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
