#!/usr/bin/env bash
# WORK QUEUE demo: place orders -> coordinator enqueues to redis-workqueue -> a worker pod claims each task
# atomically (BLMOVE), the reusable Go framework hands it to the Node user-stage file-in/file-out, acks it
# into the `done` set, and forwards the order to settlement.
source "$(dirname "$0")/lib.sh"
N="${1:-8}"

hr "5 · WORK QUEUE"
source_block \
  "services/workqueue-coordinator/*.go - /enqueue + reaper (requeues expired/abandoned claims)" \
  "services/workqueue-framework/main.go - atomic BLMOVE claim, heartbeat, ack into orderforge:done" \
  "services/order-stage/{shim,stage}.js - the user stage: read /in/order.json -> write /out/result.json" \
  "docs/design/CONTRACTS.md section 2,3,9 - the Redis key convention + the file handshake"

RQ=$(pod_by_label redis-workqueue)   # resolve the queue-redis pod once so the printed commands are real
rq() { kubectl -n "$NS" exec "$RQ" -c redis -- redis-cli "$@" | tr -d '\r'; }

before=$(run "kubectl -n $NS exec $RQ -c redis -- redis-cli SCARD orderforge:done" | tr -d '\r')
note "orderforge:done starts at $before"

hr "the worker fleet (each pod = framework [Go] + order-stage [Node] sharing an emptyDir /work)"
run "kubectl -n $NS get pods -l app=order-worker -o wide"

hr "place $N orders through the Order API (fulfillable -> enqueued)"
show "kubectl -n $NS run ofpost --rm -i --image=alpine -- wget -O- --post-data='{customer_id,items}' http://order-api/orders   # x$N"
last_task=""
for i in $(seq 1 "$N"); do
  resp=$(place_order "cust_wq_$i" || true)
  tid=$(printf '%s' "$resp" | grep -o '"task_id":"[^"]*"' | head -1 | sed 's/.*:"//; s/"$//' || true)
  if [ -n "$tid" ]; then last_task="$tid"; printf '.'; else printf 'x'; fi
done
echo
note "queue depth right after enqueue (orderforge:ready): $(rq LLEN orderforge:ready)  (workers may already be draining it)"

hr "watch the done set grow to +$N (workers claiming and acking)"
show "kubectl -n $NS exec $RQ -c redis -- redis-cli SCARD orderforge:done   # polled until it reaches $((before + N))"
target=$((before + N))
for _ in $(seq 1 30); do
  now=$(rq SCARD orderforge:done)
  printf '\r  orderforge:done = %s / %s' "$now" "$target"
  [ "$now" -ge "$target" ] 2>/dev/null && break
  sleep 1
done
echo

if [ -n "$last_task" ]; then
  hr "one completed task's hash (status flips to done; attempt stays 1 = no retries)"
  run "kubectl -n $NS exec $RQ -c redis -- redis-cli HGETALL orderforge:task:$last_task"
fi

hr "orders handed off to settlement (orderforge:settle:pending; the leader drains it every ~3s)"
run "kubectl -n $NS exec $RQ -c redis -- redis-cli SCARD orderforge:settle:pending"
note "0 is normal - the leader-elected settlement already finalized them."
note "task dirs under /work/tasks are created then deleted after ack, so they are intentionally ephemeral."
