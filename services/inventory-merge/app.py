"""inventory-merge HTTP service (scatter/gather merge step).

Listens on :9090 and exposes:
  POST /merge    {query, partials} -> merged fulfillment decision (section 8)
  GET  /healthz  -> 200

The merge logic lives in merge.py; this module is just the HTTP shell.
"""

from __future__ import annotations

import logging
import os

from flask import Flask, jsonify, request
from waitress import serve

from merge import merge_availability

logging.basicConfig(
    level=logging.INFO,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"inventory-merge","msg":"%(message)s"}',
)
log = logging.getLogger("inventory-merge")

app = Flask(__name__)


@app.get("/healthz")
def healthz():
    return jsonify(status="ok"), 200


@app.post("/merge")
def merge():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="body must be a JSON object"), 400

    query = payload.get("query")
    if not isinstance(query, dict):
        return jsonify(error="'query' object is required"), 400

    partials = payload.get("partials")
    if partials is None:
        partials = []
    if not isinstance(partials, list):
        return jsonify(error="'partials' must be an array"), 400

    result = merge_availability(query, partials)
    log.info(
        "merged order_id=%s fulfillable=%s partial=%s allocated=%d",
        result["order_id"],
        result["fulfillable"],
        result["partial"],
        len(result["allocation"]),
    )
    return jsonify(result), 200


def main():
    host, _, port = os.environ.get("MERGE_ADDR", ":9090").rpartition(":")
    host = host or "0.0.0.0"
    log.info("inventory-merge listening on %s:%s", host, port)
    serve(app, host=host, port=int(port))


if __name__ == "__main__":
    main()
