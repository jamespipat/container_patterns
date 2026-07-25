"""OrderForge settlement sweeper.

Background reconciliation loop that finalizes completed orders, but only while
this pod's leader-elector sidecar reports that we hold the settlement lease.

Contract: CONTRACTS.md section 7.
  - Every SWEEP_INTERVAL_S: GET LEADER_URL. A poll failure/timeout, or a
    leaseValidUntil that is missing/unparseable/in the past, means we are NOT
    the leader (fail-closed: when in doubt, do nothing).
  - When leader: SMEMBERS orderforge:settle:pending, then for each order_id
    idempotently finalize (HGET orderforge:order:<id> finalized; if != "1" do
    the finalize work then HSET finalized 1) and SREM it from the pending set.
  - Serves :8090/healthz (liveness only; never gated on leadership or redis).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import redis

# ---- Redis keys (all live in redis-workqueue; CONTRACTS.md section 2) --------
PENDING_SET = "orderforge:settle:pending"


def order_key(order_id: str) -> str:
    return f"orderforge:order:{order_id}"


# ---- Config -----------------------------------------------------------------
LEADER_URL = os.getenv("LEADER_URL", "http://127.0.0.1:4040/leader")
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis-workqueue:6379")
SWEEP_INTERVAL_S = float(os.getenv("SWEEP_INTERVAL_S", "3"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8090"))

# Poll the sidecar with a bound well under the sweep interval so a hung elector
# cannot stall the loop. Fail-closed treats a timeout as "not leader".
LEADER_POLL_TIMEOUT_S = 2.0
# Bound redis calls so a hung server cannot wedge the sweeper indefinitely.
REDIS_TIMEOUT_S = 2.0

log = logging.getLogger("settlement")


# ---- Leadership -------------------------------------------------------------
def _parse_lease_valid_until(raw: str) -> datetime | None:
    """Parse an RFC3339 timestamp; return None if empty or unparseable."""
    if not raw:
        return None
    try:
        # Python 3.12 fromisoformat handles a trailing 'Z' and offsets.
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    # Go emits RFC3339 with an offset, but default naive values to UTC to be safe.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_leader() -> bool:
    """Ask the sidecar over localhost whether we currently hold the lease.

    Fail-closed: any failure, or a lease whose validity window has lapsed,
    means we are not the leader.
    """
    try:
        with urllib.request.urlopen(LEADER_URL, timeout=LEADER_POLL_TIMEOUT_S) as resp:
            body = resp.read()
    except (urllib.error.URLError, OSError) as exc:
        log.warning("leader poll failed, treating as not-leader: %s", exc)
        return False

    try:
        state = json.loads(body)
    except (json.JSONDecodeError, ValueError) as exc:
        log.warning("leader response not JSON, treating as not-leader: %s", exc)
        return False

    if not state.get("isLeader"):
        return False

    valid_until = _parse_lease_valid_until(str(state.get("leaseValidUntil", "")))
    if valid_until is None:
        log.warning("isLeader true but leaseValidUntil missing/invalid, treating as not-leader")
        return False
    if valid_until <= datetime.now(timezone.utc):
        log.warning("lease expired at %s, treating as not-leader (fail-closed)", valid_until)
        return False
    return True


# ---- Settlement work --------------------------------------------------------
def finalize_order(r: redis.Redis, order_id: str) -> None:
    """Idempotently finalize one order and drop it from the pending set.

    Safe to run twice: the finalize work is guarded by the `finalized` flag,
    and SREM is a no-op once the member is gone. This idempotence is what makes
    a brief two-leader overlap during failover harmless.
    """
    key = order_key(order_id)
    if r.hget(key, "finalized") != "1":
        # Finalize work is a no-op in this teaching artifact; the flag is the
        # settlement result. Real logic (capture payment, ledger write) goes here.
        r.hset(key, "finalized", "1")
        log.info("finalized order %s", order_id)
    r.srem(PENDING_SET, order_id)


def sweep(r: redis.Redis) -> None:
    if not is_leader():
        log.debug("not leader; skipping sweep")
        return
    try:
        pending = r.smembers(PENDING_SET)
    except redis.RedisError as exc:
        log.error("failed to read pending set: %s", exc)
        return
    for order_id in pending:
        try:
            finalize_order(r, order_id)
        except redis.RedisError as exc:
            # Skip this order; it stays in pending and is retried next sweep.
            log.error("failed to finalize order %s: %s", order_id, exc)


# ---- Health server ----------------------------------------------------------
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (http.server naming)
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok\n")
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *args) -> None:  # silence per-request stderr logging
        pass


def start_health_server() -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("", HEALTH_PORT), _HealthHandler)
    threading.Thread(target=server.serve_forever, name="healthz", daemon=True).start()
    log.info("healthz listening on :%d", HEALTH_PORT)
    return server


# ---- Main -------------------------------------------------------------------
def build_redis() -> redis.Redis:
    host, _, port = REDIS_ADDR.partition(":")
    return redis.Redis(
        host=host,
        port=int(port or "6379"),
        decode_responses=True,
        socket_connect_timeout=REDIS_TIMEOUT_S,
        socket_timeout=REDIS_TIMEOUT_S,
    )


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    stop = threading.Event()

    def _on_signal(signum, _frame) -> None:
        log.info("received signal %d, shutting down", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    health = start_health_server()
    r = build_redis()
    log.info(
        "settlement up: leader_url=%s redis=%s sweep=%.1fs",
        LEADER_URL, REDIS_ADDR, SWEEP_INTERVAL_S,
    )

    while not stop.is_set():
        try:
            sweep(r)
        except Exception:  # never let one bad cycle kill the loop
            log.exception("unexpected error during sweep")
        stop.wait(SWEEP_INTERVAL_S)

    health.shutdown()
    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
