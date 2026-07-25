"""warehouse-leaf-py: a single warehouse-shard availability leaf.

Scatter/gather leaf (CONTRACTS.md section 8). The inventory-root fans the same
query out to every leaf concurrently; each leaf answers for the requested SKUs
from a static per-shard map, reporting any it does not stock as zero-availability.

    POST /shard/availability  {"items":[{"sku","qty"}, ...]}
      -> {"shard": SHARD_NAME, "lines":[{"sku","available_qty","unit_price","eta_days"}]}
    GET  /healthz -> 200

Deliberately dependency-free (stdlib only): the leaf is a teaching artifact
whose whole job is a static lookup, so a WSGI framework would add nothing.
ThreadingHTTPServer gives us the concurrency the scatter step expects.
"""

import json
import logging
import os
import signal
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SHARD_NAME = os.environ.get("SHARD_NAME", "us-east")
PORT = 8080

# Static per-shard stock map. This process only knows about its own shard, so
# we key by SHARD_NAME and fall back to us-east's catalogue. A SKU absent from
# the map is reported as out of stock here (available_qty 0); the merge service
# sums availability across shards to decide overall fulfillability.
STOCK_BY_SHARD = {
    "us-east": {
        "A1": {"available_qty": 120, "unit_price": 9.99, "eta_days": 2},
        "B2": {"available_qty": 45, "unit_price": 19.50, "eta_days": 2},
        "C3": {"available_qty": 8, "unit_price": 4.25, "eta_days": 3},
        "D4": {"available_qty": 0, "unit_price": 129.00, "eta_days": 5},
        "E5": {"available_qty": 300, "unit_price": 1.75, "eta_days": 1},
        "F6": {"available_qty": 15, "unit_price": 59.99, "eta_days": 4},
    },
}

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"warehouse-leaf-py","msg":"%(message)s"}',
)
log = logging.getLogger("warehouse-leaf-py")


def line_for(sku):
    """Return the availability line for one SKU on this shard.

    Unknown or unstocked SKUs are reported with zero availability and a zero
    price so the response shape is uniform and the merge step can sum safely.
    """
    stock = STOCK_BY_SHARD.get(SHARD_NAME, STOCK_BY_SHARD["us-east"])
    entry = stock.get(sku)
    if entry is None:
        return {"sku": sku, "available_qty": 0, "unit_price": 0.0, "eta_days": 0}
    return {
        "sku": sku,
        "available_qty": entry["available_qty"],
        "unit_price": entry["unit_price"],
        "eta_days": entry["eta_days"],
    }


def availability(items):
    """Build the shard partial for the requested items (unknown/invalid skipped)."""
    lines = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        sku = item.get("sku")
        if not isinstance(sku, str) or sku in seen:
            continue
        seen.add(sku)
        lines.append(line_for(sku))
    return {"shard": SHARD_NAME, "lines": lines}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._send_json(200, {"status": "ok", "shard": SHARD_NAME})
        else:
            self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/shard/availability":
            self._send_json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
        except (TypeError, ValueError):
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        try:
            req = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            self._send_json(400, {"error": "invalid json"})
            return
        items = req.get("items") if isinstance(req, dict) else None
        if not isinstance(items, list):
            self._send_json(400, {"error": "missing items array"})
            return
        self._send_json(200, availability(items))

    def log_message(self, fmt, *args):
        # Route access logging through our JSON logger instead of stderr text.
        log.info("%s - %s", self.address_string(), fmt % args)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)

    def shutdown(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        # shutdown() must run off the serve_forever thread; we are in a signal
        # handler on the main thread, so stop by closing from a brief thread.
        import threading

        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("warehouse-leaf-py shard=%s listening on :%d", SHARD_NAME, PORT)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        log.info("stopped")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
