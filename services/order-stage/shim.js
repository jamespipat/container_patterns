'use strict';

// order-stage SHIM (framework-provided, generic, never edited by the user).
//
// Responsibility: watch the shared work volume for tasks the workqueue-framework
// has made ready, and for each one run the user's one-shot business logic
//   node stage.js <in/order.json> <out/result.json>
// mapping the child's exit code onto the file-handshake markers from
// CONTRACTS.md section 3:
//   exit 0        -> success        -> touch response.done
//   exit non-zero -> business reject -> touch error.failed
//
// The user never touches this file; they only write stage.js. This split is the
// literal proof of "user writes only file-in / file-out".

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

// Parent dir that the framework and stage both mount (emptyDir at /work).
// Per CONTRACTS.md the stable, never-deleted parent is /work/tasks.
const TASKS_DIR = process.env.TASKS_DIR || '/work/tasks';
const POLL_MS = Number(process.env.POLL_MS || 100);
const STAGE_SCRIPT = path.join(__dirname, 'stage.js');

// Markers written by the framework (input) and by us / stage (output).
const READY = 'request.ready';
const DONE = 'response.done';
const FAILED = 'error.failed';

// task_ids whose stage child is currently running, so a poll tick never
// double-spawns. Membership is transient: cleared when the child exits.
const inProgress = new Set();

function log(level, msg, extra) {
  const rec = Object.assign(
    { ts: new Date().toISOString(), level, logger: 'order-stage.shim', msg },
    extra || {}
  );
  process.stdout.write(JSON.stringify(rec) + '\n');
}

function exists(p) {
  try {
    fs.accessSync(p);
    return true;
  } catch {
    return false;
  }
}

// Create an empty marker file (idempotent; failure is logged but non-fatal so
// the loop keeps serving other tasks).
function touch(file) {
  try {
    fs.closeSync(fs.openSync(file, 'w'));
  } catch (err) {
    log('error', 'failed to write marker', { file, error: String(err) });
  }
}

// Decide whether a task dir needs processing right now. A task is eligible when
// the framework has signalled input-ready and we have not already produced an
// output marker and no child is mid-flight for it. Checking the on-disk markers
// (not just an in-memory set) makes reprocessing correct: the framework deletes
// the whole task dir after ack, so a fresh dir with the same id is genuinely new
// work and is picked up again.
function isEligible(taskId, dir) {
  if (inProgress.has(taskId)) return false;
  if (!exists(path.join(dir, READY))) return false;
  if (exists(path.join(dir, DONE))) return false;
  if (exists(path.join(dir, FAILED))) return false;
  return true;
}

function processTask(taskId, dir) {
  inProgress.add(taskId);

  const inFile = path.join(dir, 'in', 'order.json');
  const outDir = path.join(dir, 'out');
  const outFile = path.join(outDir, 'result.json');

  // Guarantee the output dir exists so stage.js can stay a trivial file writer.
  try {
    fs.mkdirSync(outDir, { recursive: true });
  } catch (err) {
    log('error', 'failed to create out dir', { taskId, error: String(err) });
    touch(path.join(dir, FAILED));
    inProgress.delete(taskId);
    return;
  }

  log('info', 'processing task', { taskId });

  const child = spawn('node', [STAGE_SCRIPT, inFile, outFile], {
    stdio: ['ignore', 'inherit', 'inherit'],
  });

  child.on('error', (err) => {
    log('error', 'failed to spawn stage', { taskId, error: String(err) });
    touch(path.join(dir, FAILED));
    inProgress.delete(taskId);
  });

  child.on('close', (code, signal) => {
    // The task dir can be deleted by the framework after ack; only write the
    // marker if the dir still exists so we never resurrect a reaped task.
    if (!exists(dir)) {
      inProgress.delete(taskId);
      return;
    }
    if (code === 0) {
      touch(path.join(dir, DONE));
      log('info', 'task succeeded', { taskId });
    } else {
      touch(path.join(dir, FAILED));
      log('info', 'task rejected', { taskId, code, signal: signal || null });
    }
    inProgress.delete(taskId);
  });
}

function poll() {
  let entries;
  try {
    entries = fs.readdirSync(TASKS_DIR, { withFileTypes: true });
  } catch (err) {
    // Volume may not be mounted yet at startup; keep polling.
    if (err.code !== 'ENOENT') {
      log('error', 'failed to read tasks dir', { dir: TASKS_DIR, error: String(err) });
    }
    return;
  }

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const taskId = entry.name;
    const dir = path.join(TASKS_DIR, taskId);
    if (isEligible(taskId, dir)) {
      processTask(taskId, dir);
    }
  }
}

let stopping = false;
const timer = setInterval(poll, POLL_MS);

function shutdown(sig) {
  if (stopping) return;
  stopping = true;
  log('info', 'shutting down', { signal: sig });
  clearInterval(timer);
  // In-flight children finish on their own; new work simply stops being claimed.
  // Give running children a brief chance to complete before exit.
  const waitExit = () => {
    if (inProgress.size === 0) {
      process.exit(0);
    }
  };
  const grace = setInterval(waitExit, 100);
  setTimeout(() => {
    clearInterval(grace);
    process.exit(0);
  }, 5000);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

log('info', 'watching for tasks', { dir: TASKS_DIR, pollMs: POLL_MS });
poll();
