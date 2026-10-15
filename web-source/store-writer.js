'use strict';

const path = require('path');
const { spawn } = require('child_process');

class StoreWriter {
  constructor(dbPath, onExit) {
    this.nextId = 1;
    this.pending = new Map();
    this.exitedError = null;
    this.closing = false;
    this.draining = false;
    const scriptPath = path.join(__dirname, 'store.py');
    this.proc = spawn(process.env.CHANNELS_STORE_PYTHON || 'python3', [scriptPath, '--db', dbPath], {
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
      detached: process.platform === 'win32',
    });
    this.proc.stdout.setEncoding('utf8');
    this.proc.stderr.setEncoding('utf8');
    let stdout = '';
    this.proc.stdout.on('data', (chunk) => {
      stdout += chunk;
      let idx;
      while ((idx = stdout.indexOf('\n')) >= 0) {
        const line = stdout.slice(0, idx);
        stdout = stdout.slice(idx + 1);
        this._handleResponse(line);
      }
    });
    this.proc.stderr.on('data', (chunk) => process.stderr.write(`[store] ${chunk}`));
    this.closed = new Promise((resolve) => this.proc.once('close', (code, signal) => resolve({ code, signal })));
    this.proc.on('error', (error) => {
      this.exitedError = error;
      for (const { reject } of this.pending.values()) reject(error);
      this.pending.clear();
      if (onExit) onExit(error);
    });
    this.proc.on('close', (code, signal) => {
      const error = new Error(`store writer exited code=${code} signal=${signal}`);
      this.exitedError = error;
      for (const { reject } of this.pending.values()) reject(error);
      this.pending.clear();
      if (!this.closing && onExit) onExit(error);
    });
  }

  _handleResponse(line) {
    if (!line.trim()) return;
    let response;
    try {
      response = JSON.parse(line);
    } catch (error) {
      console.error('invalid store response', line);
      return;
    }
    const id = response.request_id;
    const pending = this.pending.get(id);
    if (!pending) return;
    this.pending.delete(id);
    if (response.status === 'error') pending.reject(new Error(response.error));
    else pending.resolve(response);
  }

  command(op, payload = {}, timeoutMs = 60_000) {
    if (this.exitedError) return Promise.reject(this.exitedError);
    if (this.closing) return Promise.reject(new Error('store writer is closing'));
    const requestId = this.nextId++;
    const command = { request_id: requestId, op, ...payload };
    return new Promise((resolve, reject) => {
      const timer = this.draining ? null : setTimeout(() => {
        this.pending.delete(requestId);
        const error = new Error(`store command timed out after ${timeoutMs}ms: op=${op}`);
        reject(error);
        this.proc.kill('SIGKILL');
      }, timeoutMs);
      this.pending.set(requestId, {
        timer,
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject: (e) => { clearTimeout(timer); reject(e); },
      });
      this.proc.stdin.write(`${JSON.stringify(command)}\n`, 'utf8', (error) => {
        if (!error) return;
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(error);
      });
    });
  }

  drain() {
    this.draining = true;
    for (const { timer } of this.pending.values()) clearTimeout(timer);
  }

  close() {
    if (this.closePromise) return this.closePromise;
    this.drain();
    this.closing = true;
    if (!this.exitedError) this.proc.stdin.end();
    this.closePromise = this.closed.then(({ code, signal }) => {
      if (code !== 0 || signal) throw this.exitedError;
    });
    return this.closePromise;
  }
}

module.exports = { StoreWriter };
