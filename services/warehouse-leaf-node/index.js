'use strict';

// warehouse-leaf-node - OrderForge scatter/gather leaf (Node).
// Contract: CONTRACTS.md section 8.
//   POST /shard/availability  body {items:[{sku,qty}]}  (root relays the full
//   client body, which may also carry order_id; extra fields are ignored)
//   -> {"shard":"<SHARD_NAME>","lines":[{sku,available_qty,unit_price,eta_days}]}
// This shard owns a STATIC per-shard stock map and answers only for the SKUs it
// stocks; unknown SKUs are simply omitted from `lines`. Identical role to the
// Python sibling warehouse-leaf-py (us-east) - same contract, different language.

const http = require('http');

const PORT = 8080;
const SHARD_NAME = process.env.SHARD_NAME || 'us-west';

// Static stock/pricing/ETA catalog for this shard. Prices in USD.
// Keys are SKUs; a SKU absent here means this shard does not stock it.
const STOCK = {
  A1: { available_qty: 40, unit_price: 9.99, eta_days: 2 },
  A2: { available_qty: 15, unit_price: 14.5, eta_days: 3 },
  B1: { available_qty: 8, unit_price: 4.25, eta_days: 1 },
  C3: { available_qty: 0, unit_price: 29.0, eta_days: 5 },
  W9: { available_qty: 120, unit_price: 2.0, eta_days: 4 },
};

function readBody(req, limitBytes = 1 << 20) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limitBytes) {
        reject(new Error('request body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

// Build this shard's partial answer for the requested items.
function buildLines(items) {
  const lines = [];
  for (const item of items) {
    if (!item || typeof item.sku !== 'string') continue;
    const stock = STOCK[item.sku];
    if (!stock) continue; // not stocked here -> omit
    lines.push({
      sku: item.sku,
      available_qty: stock.available_qty,
      unit_price: stock.unit_price,
      eta_days: stock.eta_days,
    });
  }
  return lines;
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

async function handleAvailability(req, res) {
  let parsed;
  try {
    const raw = await readBody(req);
    parsed = raw ? JSON.parse(raw) : {};
  } catch (err) {
    sendJson(res, 400, { error: `invalid request body: ${err.message}` });
    return;
  }
  const items = Array.isArray(parsed.items) ? parsed.items : [];
  sendJson(res, 200, { shard: SHARD_NAME, lines: buildLines(items) });
}

const server = http.createServer((req, res) => {
  const path = (req.url || '').split('?')[0];

  if (req.method === 'POST' && path === '/shard/availability') {
    handleAvailability(req, res).catch((err) => {
      sendJson(res, 500, { error: `internal error: ${err.message}` });
    });
    return;
  }
  if (req.method === 'GET' && path === '/healthz') {
    sendJson(res, 200, { status: 'ok', shard: SHARD_NAME });
    return;
  }
  sendJson(res, 404, { error: 'not found' });
});

server.listen(PORT, () => {
  console.log(JSON.stringify({
    level: 'info',
    msg: 'warehouse-leaf-node listening',
    shard: SHARD_NAME,
    port: PORT,
  }));
});

// Graceful shutdown: stop accepting new connections, drain in-flight, exit.
function shutdown(signal) {
  console.log(JSON.stringify({ level: 'info', msg: 'shutting down', signal }));
  server.close(() => process.exit(0));
  // Safety net if connections do not drain promptly.
  setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
