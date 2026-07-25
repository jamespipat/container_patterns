#!/usr/bin/env bash
# SCATTER/GATHER demo: inventory-root fans one availability query out to two warehouse leaves (Python +
# Node, identical role, different language) and inventory-merge gathers them into one verdict. Kill a leaf
# and the verdict comes back partial:true instead of failing.
source "$(dirname "$0")/lib.sh"
pretty() { python3 -m json.tool 2>/dev/null || cat; }

hr "6 · SCATTER / GATHER"
source_block \
  "services/inventory-root/main.go   - concurrent scatter, per-leaf timeout, siblings not cancelled" \
  "services/warehouse-leaf-py/app.py + warehouse-leaf-node/index.js - identical role, two languages" \
  "services/inventory-merge/merge.py - gathers partials, sets partial:true on any missing shard" \
  "docs/design/CONTRACTS.md section 8"

# A1 is stocked by BOTH leaves (us-east + us-west) so we exercise a real cross-shard gather.
QUERY='{"order_id":"demo_scatter","items":[{"sku":"A1","qty":2}]}'
AVAIL="kubectl -n $NS run ofpost-\$RANDOM --rm -i --restart=Never -q --image=$ALPINE -- wget -qO- --header=Content-Type:application/json --post-data='$QUERY' http://inventory-root:8080/availability"

hr "both leaves UP: one query fans out to us-east (py) + us-west (node), gathered into one verdict"
run "$AVAIL" | pretty
note "partial:false - every warehouse answered."

hr "take one warehouse OFFLINE: scale warehouse-leaf-node (us-west) to 0 (deterministic, unlike a pod delete)"
run "kubectl -n $NS scale deploy/warehouse-leaf-node --replicas=0"
run "kubectl -n $NS rollout status deploy/warehouse-leaf-node --timeout=30s"
sleep 2

hr "same query, one leaf down: root does NOT cancel the survivor, merge flags the gap"
run "$AVAIL" | pretty
note "partial:true, meta.failed:1 - answered from the surviving shard, honestly marked incomplete."
note "by design the root scatters concurrently, so wall-clock is the slowest leaf, not the sum of both."

hr "restore the warehouse"
run "kubectl -n $NS scale deploy/warehouse-leaf-node --replicas=1"
run "kubectl -n $NS rollout status deploy/warehouse-leaf-node --timeout=60s"
note "back to full coverage."
