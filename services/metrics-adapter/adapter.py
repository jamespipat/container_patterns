"""metrics-adapter: transform order-api's native /stats JSON into Prometheus text.

Pattern: Adapter sidecar. Prometheus scrapes this process on :9102/metrics; on each
scrape we pull the (deliberately awkward) native JSON from NATIVE_STATS_URL and emit
Prometheus exposition format v0.0.4. The adapter is stateless: the native endpoint
already reports cumulative totals, so we never store counter state.

Key rule (CONTRACTS.md section 6): readiness/liveness is self-only, NEVER gated on the
upstream. If the native endpoint is unreachable we still return HTTP 200, but with
`orderforge_native_scrape_up 0` so Prometheus sees a clean "target up, source down".
"""

import json
import logging
import os
import signal
import sys
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NATIVE_STATS_URL = os.environ.get("NATIVE_STATS_URL", "http://127.0.0.1:9000/stats")
LISTEN_ADDR = os.environ.get("LISTEN_ADDR", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9102"))
SCRAPE_TIMEOUT_S = float(os.environ.get("SCRAPE_TIMEOUT_S", "2"))

# Prometheus text exposition format, version 0.0.4.
CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("metrics-adapter")


def fmt_num(value):
    """Format a number as Prometheus wants it: plain decimal, no scientific notation.

    Integers stay integral; whole floats drop the trailing '.0'; other floats use the
    shortest round-trip form (repr), which keeps bucket bounds like 0.005 exact.
    """
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, int):
        return str(value)
    f = float(value)
    if f.is_integer():
        return str(int(f))
    return repr(f)


class MetricWriter:
    """Accumulates exposition lines, emitting HELP/TYPE once per metric family."""

    def __init__(self):
        self._lines = []

    def _family(self, name, help_text, metric_type):
        self._lines.append(f"# HELP {name} {help_text}")
        self._lines.append(f"# TYPE {name} {metric_type}")

    def scalar(self, name, help_text, metric_type, value):
        """Emit a single-series counter or gauge. No-op if value is None."""
        if value is None:
            return
        self._family(name, help_text, metric_type)
        self._lines.append(f"{name} {fmt_num(value)}")

    def labeled(self, name, help_text, metric_type, samples):
        """Emit a family with per-label samples. `samples` is a list of (label_str, value)."""
        if not samples:
            return
        self._family(name, help_text, metric_type)
        for label_str, value in samples:
            self._lines.append(f"{name}{{{label_str}}} {fmt_num(value)}")

    def histogram(self, name, help_text, buckets_ms, count, total_ms):
        """Emit a Prometheus histogram, converting the native ms buckets to seconds.

        Native buckets are cumulative counts keyed by an upper-bound in milliseconds.
        We divide bounds by 1000 (le in seconds), keep them ascending, add the required
        +Inf bucket equal to the total count, and emit _sum (ms/1000) and _count.
        """
        if count is None or total_ms is None:
            return
        self._family(name, help_text, "histogram")
        # Sort by numeric bound so buckets stay ascending regardless of JSON key order.
        ordered = sorted(buckets_ms.items(), key=lambda kv: float(kv[0]))
        for bound_ms, cumulative in ordered:
            le_seconds = fmt_num(float(bound_ms) / 1000.0)
            self._lines.append(f'{name}_bucket{{le="{le_seconds}"}} {fmt_num(cumulative)}')
        # +Inf bucket must equal the observation count.
        self._lines.append(f'{name}_bucket{{le="+Inf"}} {fmt_num(count)}')
        self._lines.append(f"{name}_sum {fmt_num(float(total_ms) / 1000.0)}")
        self._lines.append(f"{name}_count {fmt_num(count)}")

    def render(self):
        # Prometheus requires a trailing newline.
        return "\n".join(self._lines) + "\n"


def transform(stats):
    """Map the native stats dict to Prometheus exposition text (scrape succeeded)."""
    w = MetricWriter()
    w.scalar(
        "orderforge_native_scrape_up",
        "1 if the native stats endpoint was scraped successfully, else 0",
        "gauge",
        1,
    )

    orders = stats.get("orders", {}) or {}
    w.scalar("orderforge_orders_received_total", "Orders received", "counter",
             orders.get("receivedTotal"))
    w.scalar("orderforge_orders_placed_total", "Orders placed", "counter",
             orders.get("placedTotal"))
    w.scalar("orderforge_orders_rejected_total", "Orders rejected", "counter",
             orders.get("rejectedTotal"))
    w.scalar("orderforge_orders_in_flight", "Orders currently in flight", "gauge",
             orders.get("inFlight"))

    http = stats.get("http", {}) or {}
    requests_total = http.get("requestsTotal", {}) or {}
    # Keep a stable order for readable output; label order is not significant to Prometheus.
    request_samples = [
        (f'code="{code}"', value)
        for code, value in sorted(requests_total.items())
    ]
    w.labeled("orderforge_http_requests_total", "HTTP requests by status code",
              "counter", request_samples)

    latency = http.get("requestLatencyMs", {}) or {}
    w.histogram(
        "orderforge_http_request_latency_seconds",
        "HTTP request latency in seconds",
        latency.get("buckets", {}) or {},
        latency.get("count"),
        latency.get("sum"),
    )

    cache = stats.get("cache", {}) or {}
    w.scalar("orderforge_cache_hits_total", "Cache hits", "counter",
             cache.get("hitsTotal"))
    w.scalar("orderforge_cache_misses_total", "Cache misses", "counter",
             cache.get("missesTotal"))

    w.scalar("orderforge_uptime_seconds", "Process uptime in seconds", "gauge",
             stats.get("uptimeSeconds"))

    return w.render()


def scrape_down():
    """Exposition text when the native endpoint could not be scraped."""
    w = MetricWriter()
    w.scalar(
        "orderforge_native_scrape_up",
        "1 if the native stats endpoint was scraped successfully, else 0",
        "gauge",
        0,
    )
    return w.render()


def fetch_native():
    """GET the native stats JSON with a hard timeout. Returns a dict or raises."""
    req = urllib.request.Request(NATIVE_STATS_URL, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=SCRAPE_TIMEOUT_S) as resp:
        if resp.status != 200:
            raise ValueError(f"native stats returned HTTP {resp.status}")
        return json.load(resp)


def build_metrics():
    """Return exposition text. Never raises: upstream failure => scrape_up 0."""
    try:
        stats = fetch_native()
        return transform(stats)
    except (urllib.error.URLError, ValueError, json.JSONDecodeError, OSError) as exc:
        # Expected failure modes (upstream down, timeout, bad payload). Log and degrade.
        log.warning("native scrape failed (%s): %s", NATIVE_STATS_URL, exc)
        return scrape_down()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, status, body, content_type="text/plain; charset=utf-8"):
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/metrics":
            self._respond(200, build_metrics(), CONTENT_TYPE)
        elif self.path == "/healthz":
            # Liveness/self only. Deliberately independent of the upstream.
            self._respond(200, "ok\n")
        else:
            self._respond(404, "not found\n")

    def log_message(self, fmt, *args):
        # Route access logs through the structured logger instead of stderr.
        log.info("%s - %s", self.address_string(), fmt % args)


def main():
    server = ThreadingHTTPServer((LISTEN_ADDR, LISTEN_PORT), Handler)

    def shutdown(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        # shutdown() must run off the serving thread.
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("metrics-adapter listening on %s:%d, upstream=%s",
             LISTEN_ADDR, LISTEN_PORT, NATIVE_STATS_URL)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        log.info("metrics-adapter stopped")


if __name__ == "__main__":
    main()
