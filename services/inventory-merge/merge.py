"""Pure merge logic for the scatter/gather pattern.

Given the original availability query and the per-shard partial results collected
by inventory-root, combine them into a single fulfillment decision. Kept free of
any web/framework code so it can be unit-tested in isolation.

Contract (CONTRACTS.md section 8):
  input : {"query": {order_id, items:[{sku, qty}]}, "partials": [<partial>...]}
  output: {order_id, fulfillable, allocation:[{sku, warehouse, qty, unit_price,
           eta_days}], total_price, max_eta_days, partial}

A partial is either a success envelope carrying a leaf body
  {"shard","ok":true,"status","latency_ms","body":{"shard","lines":[
     {"sku","available_qty","unit_price","eta_days"}]}}
or a failure envelope {"shard","ok":false,"error":...}.
"""

from __future__ import annotations


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _collect_offers(partials):
    """Return (offers, partial_flag).

    offers maps sku -> list of {available_qty, unit_price, eta_days, warehouse}
    gathered from every shard that answered successfully. partial_flag is True
    when at least one shard failed (so the caller knows the view is incomplete).
    """
    offers = {}
    partial = False
    for p in partials or []:
        if not (isinstance(p, dict) and p.get("ok")):
            # Missing/false ok, or a malformed entry: treat as a failed shard.
            partial = True
            continue
        body = p.get("body") or {}
        warehouse = body.get("shard") or p.get("shard") or "unknown"
        for line in body.get("lines") or []:
            sku = line.get("sku")
            if sku is None:
                continue
            offers.setdefault(sku, []).append(
                {
                    "available_qty": _as_int(line.get("available_qty")),
                    "unit_price": _as_float(line.get("unit_price")),
                    "eta_days": _as_int(line.get("eta_days")),
                    "warehouse": warehouse,
                }
            )
    return offers, partial


def merge_availability(query, partials):
    query = query or {}
    order_id = query.get("order_id")
    items = query.get("items") or []

    offers, partial = _collect_offers(partials)

    fulfillable = True
    allocation = []
    total_price = 0.0
    max_eta_days = 0

    for item in items:
        sku = item.get("sku")
        requested = _as_int(item.get("qty"))

        candidates = [o for o in offers.get(sku, []) if o["available_qty"] > 0]
        available_total = sum(o["available_qty"] for o in candidates)
        if available_total < requested:
            fulfillable = False

        if not candidates:
            # Nothing to allocate from; already marked not fulfillable above
            # when a positive qty was requested.
            continue

        # Prefer the fastest shard, breaking ties by cheapest unit price.
        best = min(candidates, key=lambda o: (o["eta_days"], o["unit_price"]))
        allocation.append(
            {
                "sku": sku,
                "warehouse": best["warehouse"],
                "qty": requested,
                "unit_price": best["unit_price"],
                "eta_days": best["eta_days"],
            }
        )
        total_price += best["unit_price"] * requested
        max_eta_days = max(max_eta_days, best["eta_days"])

    return {
        "order_id": order_id,
        "fulfillable": fulfillable,
        "allocation": allocation,
        "total_price": round(total_price, 2),
        "max_eta_days": max_eta_days,
        "partial": partial,
    }
