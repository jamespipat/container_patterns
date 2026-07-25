// OrderForge log-shipper (Sidecar pattern).
//
// Tails every *.log file under LOG_DIR by polling and tracking a byte offset,
// batches complete lines (>= BATCH_MAX lines OR every FLUSH_MS), POSTs them to
// SINK_URL in Loki push format, and advances the per-file offset ONLY after a
// 2xx response. That gives at-least-once delivery: a failed push is retried on
// the next tick from the same committed offset, so nothing is dropped (a line
// may be shipped twice if the sink 2xx'd but we crashed before committing).
//
// No listening port. Graceful SIGTERM: stop the poll loop, drain remaining
// complete lines with a final flush, then exit.

import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";

const LOG_DIR = process.env.LOG_DIR || "/var/log/app";
const SINK_URL = process.env.SINK_URL || "http://log-sink:3100/loki/api/v1/push";
const POD_NAME = process.env.POD_NAME || os.hostname();

const BATCH_MAX = 200; // flush once this many lines are pending
const FLUSH_MS = 1000; // ...or at least this often, whichever comes first
const POLL_MS = 250; // filesystem poll interval
const POST_TIMEOUT_MS = 5000;

const NL = 0x0a; // '\n'

// Per-file tail state, keyed by absolute path.
//   offset: committed byte offset; only advanced on a successful (2xx) push.
/** @type {Map<string, {offset: number}>} */
const files = new Map();

let running = true;
let lastFlush = Date.now();

const log = (level, msg, extra = {}) =>
  // The shipper's own diagnostics go to stderr so they never land in LOG_DIR
  // and get re-shipped into a loop.
  process.stderr.write(
    JSON.stringify({ ts: new Date().toISOString(), level, logger: "log-shipper", msg, ...extra }) + "\n",
  );

// Read every *.log file from its committed offset to EOF and collect the
// COMPLETE lines found. Does not mutate committed offsets; instead it records
// the offset each file WOULD advance to (past the last newline) so the caller
// can commit atomically after a successful push. A trailing partial line (no
// newline yet) is left uncommitted and picked up once it completes.
async function collectBatch() {
  const streams = []; // one Loki stream per source file
  const pending = []; // [{ path, newOffset }] to commit on 2xx
  let totalLines = 0;

  let names;
  try {
    names = await fsp.readdir(LOG_DIR);
  } catch (err) {
    log("warn", "cannot read LOG_DIR", { dir: LOG_DIR, err: String(err) });
    return { streams, pending, totalLines };
  }

  for (const name of names) {
    if (!name.endsWith(".log")) continue;
    const full = path.join(LOG_DIR, name);

    let st;
    try {
      st = await fsp.stat(full);
    } catch {
      continue; // vanished between readdir and stat
    }
    if (!st.isFile()) continue;

    let state = files.get(full);
    if (!state) {
      state = { offset: 0 };
      files.set(full, state);
    }

    // Truncation / rotation in place: file shrank below our offset -> restart.
    if (st.size < state.offset) {
      log("info", "log truncated, resetting offset", { file: name, size: st.size, offset: state.offset });
      state.offset = 0;
    }
    if (st.size <= state.offset) continue; // nothing new

    // Read only the unread byte range.
    const length = st.size - state.offset;
    const buf = Buffer.allocUnsafe(length);
    let fh;
    try {
      fh = await fsp.open(full, "r");
      const { bytesRead } = await fh.read(buf, 0, length, state.offset);
      const slice = buf.subarray(0, bytesRead); // guard against a short read

      const lastNl = slice.lastIndexOf(NL);
      if (lastNl === -1) continue; // no complete line yet; wait for a newline

      const complete = slice.subarray(0, lastNl + 1);
      const newOffset = state.offset + complete.length;

      const values = [];
      for (const raw of complete.toString("utf8").split("\n")) {
        if (raw.length === 0) continue; // trailing empty after final newline
        // Loki value = [<nanosecond-epoch string>, <line>]. Uniqueness within a
        // stream is not required by log-sink, but we keep timestamps monotonic.
        values.push([nextTs(), raw]);
      }
      if (values.length === 0) continue;

      streams.push({ stream: { job: "log-shipper", pod: POD_NAME, filename: name }, values });
      pending.push({ path: full, newOffset });
      totalLines += values.length;
    } catch (err) {
      log("warn", "read failed", { file: name, err: String(err) });
    } finally {
      await fh?.close();
    }
  }

  return { streams, pending, totalLines };
}

let tsCounter = 0n;
function nextTs() {
  // Nanosecond epoch as a decimal string, nudged by a counter so lines emitted
  // within the same millisecond keep a stable increasing order.
  return (BigInt(Date.now()) * 1_000_000n + tsCounter++).toString();
}

// POST one Loki payload. Returns true only on a 2xx response.
async function pushToSink(streams) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), POST_TIMEOUT_MS);
  try {
    const res = await fetch(SINK_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ streams }),
      signal: ac.signal,
    });
    // Drain the body so the socket can be reused.
    await res.text().catch(() => {});
    if (res.ok) return true;
    log("warn", "sink rejected batch", { status: res.status });
    return false;
  } catch (err) {
    log("warn", "sink push failed", { err: String(err) });
    return false;
  } finally {
    clearTimeout(timer);
  }
}

// Read available lines and, if the batch threshold or flush interval is met,
// ship them. Offsets advance only on a 2xx. Returns true if a push was
// attempted-and-succeeded (used by the drain path).
async function tick(force = false) {
  const { streams, pending, totalLines } = await collectBatch();
  if (totalLines === 0) {
    if (force) return true; // nothing left to drain
    lastFlush = Date.now(); // keep the interval from firing on an empty tail
    return false;
  }

  const due = force || totalLines >= BATCH_MAX || Date.now() - lastFlush >= FLUSH_MS;
  if (!due) return false;

  const ok = await pushToSink(streams);
  if (ok) {
    for (const { path: p, newOffset } of pending) {
      const s = files.get(p);
      if (s) s.offset = newOffset; // commit: advance only on success
    }
    lastFlush = Date.now();
    log("info", "shipped batch", { lines: totalLines, streams: streams.length });
  }
  return ok;
}

async function pollLoop() {
  while (running) {
    try {
      await tick();
    } catch (err) {
      log("error", "tick failed", { err: String(err) });
    }
    await sleep(POLL_MS);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// On SIGTERM: stop polling and make a best-effort final drain so shutdown logs
// still reach the sink. Retry the drain a few times since a push may fail.
async function shutdown(signal) {
  if (!running) return;
  running = false;
  log("info", "received signal, draining", { signal });
  for (let attempt = 0; attempt < 5; attempt++) {
    const done = await tick(true);
    if (done) break;
    await sleep(500);
  }
  log("info", "shutdown complete");
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

log("info", "log-shipper starting", { LOG_DIR, SINK_URL, POD_NAME, BATCH_MAX, FLUSH_MS });
if (!fs.existsSync(LOG_DIR)) {
  log("warn", "LOG_DIR does not exist yet; will retry as files appear", { dir: LOG_DIR });
}
pollLoop();
