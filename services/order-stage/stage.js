'use strict';

// order-stage STAGE (the ONLY file a user writes for the work-queue pattern).
//
// It is a plain one-shot process: read an order from <inFile>, validate and
// price the items, write the result to <outFile>. No Redis, no HTTP, no
// knowledge of queues, retries or markers - that all lives in the generic shim.
//
//   node stage.js <in/order.json> <out/result.json>
//
// Exit code is the business signal the shim maps to the file handshake:
//   0        -> accepted (shim touches response.done)
//   non-zero -> rejected (shim touches error.failed)
//
// result.json shape (CONTRACTS.md section 3):
//   {"order_id","decision":"accepted"|"rejected","currency","priced_total",
//    "lines":[{"sku","qty","unit_price"}],"reason":""}

const fs = require('fs');
const path = require('path');

// Static catalog: sku -> unit price. A real stage would look this up; a fixed
// map keeps the teaching artifact self-contained and deterministic.
const CATALOG = {
  A1: 9.99,
  B2: 4.5,
  C3: 19.0,
  D4: 100.0,
  E5: 0.99,
};

function round2(n) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}

// Write JSON atomically: tmp file in the same dir then rename, so a reader that
// keyed off the response.done marker never observes a half-written result.json.
function writeAtomic(outFile, obj) {
  const dir = path.dirname(outFile);
  const tmp = path.join(dir, `.result.${process.pid}.tmp`);
  fs.writeFileSync(tmp, JSON.stringify(obj) + '\n');
  fs.renameSync(tmp, outFile);
}

function main() {
  const [inFile, outFile] = process.argv.slice(2);
  if (!inFile || !outFile) {
    process.stderr.write('usage: node stage.js <in/order.json> <out/result.json>\n');
    process.exit(2);
  }

  // Parse the task envelope. The framework writes the full envelope
  // {task_id,type,attempt,enqueued_at,payload:{...}}; tolerate a bare payload.
  let payload;
  try {
    const raw = JSON.parse(fs.readFileSync(inFile, 'utf8'));
    payload = raw && raw.payload ? raw.payload : raw;
  } catch (err) {
    process.stderr.write(`stage: cannot read/parse input: ${err}\n`);
    process.exit(1);
  }

  const orderId = (payload && payload.order_id) || '';
  const currency = (payload && payload.currency) || 'USD';
  const items = Array.isArray(payload && payload.items) ? payload.items : [];

  const lines = [];
  const reasons = [];
  let total = 0;

  if (items.length === 0) {
    reasons.push('order has no items');
  }

  for (const item of items) {
    const sku = item && item.sku;
    const qty = item && item.qty;

    if (typeof sku !== 'string' || !(sku in CATALOG)) {
      reasons.push(`unknown sku: ${sku}`);
      continue;
    }
    if (!Number.isInteger(qty) || qty <= 0) {
      reasons.push(`invalid qty for ${sku}: ${qty}`);
      continue;
    }
    const unitPrice = CATALOG[sku];
    lines.push({ sku, qty, unit_price: unitPrice });
    total += unitPrice * qty;
  }

  const accepted = reasons.length === 0;
  const result = {
    order_id: orderId,
    decision: accepted ? 'accepted' : 'rejected',
    currency,
    priced_total: accepted ? round2(total) : 0,
    lines,
    reason: accepted ? '' : reasons.join('; '),
  };

  // Always emit result.json so the outcome (including the rejection reason) is
  // inspectable; the exit code is what tells the shim which marker to write.
  try {
    writeAtomic(outFile, result);
  } catch (err) {
    process.stderr.write(`stage: cannot write result: ${err}\n`);
    process.exit(1);
  }

  process.exit(accepted ? 0 : 1);
}

main();
