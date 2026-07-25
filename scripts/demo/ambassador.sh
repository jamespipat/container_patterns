#!/usr/bin/env bash
# AMBASSADOR demo: the app writes cart keys to what it thinks is one local cache; prove the ambassador
# actually split them across three separate Redis shards.
source "$(dirname "$0")/lib.sh"
N="${1:-12}"

hr "3 · AMBASSADOR"
source_block \
  "services/cache-ambassador/main.go + resp.go - RESP proxy, routes each key by crc32(key) % SHARD_COUNT" \
  "services/order-api/main.go            - dials a stock go-redis client at 127.0.0.1:6380, oblivious" \
  "deploy/kustomize/base/cache-shards.yaml - the 3-shard StatefulSet + headless Service" \
  "docs/design/CONTRACTS.md section 1,5  - shard addressing and the RESP subset"

hr "place $N orders, each writing cart:<customer> through the ambassador at localhost:6380"
show "kubectl -n $NS run ofpost --rm -i --image=alpine -- wget -O- --post-data='{customer_id,items}' http://order-api/orders   # x$N, distinct customers"
for i in $(seq 1 "$N"); do place_order "cust_demo_$i" >/dev/null && printf '.' || printf 'x'; done
echo

hr "keys actually stored on each shard - the app only ever saw one cache"
total=0
for i in 0 1 2; do
  keys=$(run "kubectl -n $NS exec cache-shard-$i -c redis -- redis-cli --scan | sort")
  printf '%s\n' "$keys" | sed 's/^/    /'
  total=$((total + $(printf '%s\n' "$keys" | grep -c . || true)))
done
note "total $total cart keys split across 3 shards by crc32(key) % 3 - one localhost cache, three backends."
