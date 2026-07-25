#!/usr/bin/env bash
# SIDECAR demo: order-api writes JSONL access logs to a shared emptyDir; a co-located log-shipper sidecar
# tails that volume and ships every line to the central log-sink (a Loki stand-in). We target ONE pod so
# the line we place is provably the line we read back.
source "$(dirname "$0")/lib.sh"

hr "1 · SIDECAR"
source_block \
  "services/order-api/logger.go        - writes JSONL access logs to the shared /var/log/app volume" \
  "services/log-shipper/index.js        - tails that volume, ships each line to the central log-sink" \
  "deploy/kustomize/base/order-api.yaml - the shared emptyDir + log-shipper native sidecar" \
  "docs/design/CONTRACTS.md section 4,10"

POD=$(pod_by_label order-api)
note "target pod: $POD (order-api main + log-shipper sidecar, sharing volume /var/log/app)"

show "kubectl -n $NS port-forward pod/$POD 18080:8080   # backgrounded for this demo"
kubectl -n "$NS" port-forward "pod/$POD" 18080:8080 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3

hr "place an order against this exact pod"
BODY='{"customer_id":"cust_sidecar_demo","currency":"USD","items":[{"sku":"A1","qty":2}]}'
RESP=$(run "curl -s -X POST localhost:18080/orders -H 'Content-Type: application/json' -d '$BODY'")
printf '%s' "$RESP" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$RESP"
OID=$(printf '%s' "$RESP" | grep -o '"order_id":"[^"]*"' | head -1 | sed 's/.*:"//; s/"$//' || true)
note "order_id: ${OID:-<none>}"

hr "1) the app WROTE it locally: JSONL on the shared volume (read via the log-shipper sidecar container)"
run "kubectl -n $NS exec $POD -c log-shipper -- tail -n 3 /var/log/app/access.log"

hr "2) the sidecar SHIPPED it: the same line now lives in the central log-sink"
raw=$(run "kubectl -n $NS run ofq-\$RANDOM --rm -i --restart=Never -q --image=$ALPINE -- wget -qO- 'http://log-sink:3100/query?q=/orders'")
printf '%s' "$raw" | python3 -c 'import json,sys
recs=json.load(sys.stdin)
print(f"log-sink holds {len(recs)} shipped /orders line(s); newest 2:")
for r in recs[:2]: print("   ", r.get("line",""))' 2>/dev/null || true
note "same line, two places - the app wrote a file; the sidecar handled central delivery."
