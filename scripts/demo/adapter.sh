#!/usr/bin/env bash
# ADAPTER demo: order-api emits scrappy native JSON on :9000; the Python adapter re-exposes it as standard
# Prometheus on :9102. Show both sides for the SAME pod - same numbers, translated.
source "$(dirname "$0")/lib.sh"

hr "2 · ADAPTER"
source_block \
  "services/order-api/stats.go         - emits scrappy native JSON on :9000 (camelCase, latency in ms)" \
  "services/metrics-adapter/adapter.py  - scrapes :9000, re-exposes Prometheus on :9102 (ms -> seconds)" \
  "deploy/kustomize/base/order-api.yaml - adapter native sidecar, liveness-only probe, scrape annotations" \
  "docs/design/CONTRACTS.md section 6"

POD=$(pod_by_label order-api)
note "target pod: $POD"

# Forward the pod's public API (8080), internal native stats (9000, NOT in any Service), and adapter (9102).
show "kubectl -n $NS port-forward pod/$POD 18080:8080 19000:9000 19102:9102   # backgrounded for this demo"
kubectl -n "$NS" port-forward "pod/$POD" 18080:8080 19000:9000 19102:9102 >/dev/null 2>&1 &
PF=$!; trap 'kill $PF 2>/dev/null' EXIT
sleep 3

hr "generate traffic (place a few orders directly against this pod)"
show "curl -s -X POST localhost:18080/orders -d '{customer_id,items}'   # x5, prints HTTP status"
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:18080/orders -H 'Content-Type: application/json' \
    -d "{\"customer_id\":\"cust_a_$i\",\"currency\":\"USD\",\"items\":[{\"sku\":\"A1\",\"qty\":2}]}" || true
done

hr "NATIVE format the app emits (:9000/stats) - camelCase JSON, latency in ms, NOT Prometheus"
run "curl -s localhost:19000/stats"; echo

hr "NORMALIZED by the adapter (:9102/metrics) - standard Prometheus exposition, ms -> seconds"
run "curl -s localhost:19102/metrics | grep -E '^orderforge_(orders|http_requests|cache|uptime)'"

note "same counts, two shapes: the adapter absorbed the mismatch so Prometheus sees one standard format."
note "small drift between the two sides (e.g. one extra 200) is just the two scrapes landing a moment apart."
