# OrderForge - Canonical Contracts (single source of truth)

Every service and manifest MUST conform to this file. If code and this file disagree, this file wins.
Derived from the approved plan + the six design memos in this directory. Namespace: **`orderforge`**.

## 0. Languages, base images, build

| Service | Lang | Base image (build -> runtime) | Listens | In Service? |
|---|---|---|---|---|
| order-api | Go | `golang:1.24` -> `gcr.io/distroless/static:nonroot` | `:8080` API, `:9000` native stats | 8080 yes; 9000 NO |
| metrics-adapter | Python | `python:3.12-slim` | `:9102` /metrics | yes (scraped) |
| log-shipper | Node | `node:22-slim` | none (tails files) | no |
| cache-ambassador | Go | distroless static | `:6380` RESP (localhost) | no |
| leader-elector | Go | distroless static | `:4040` /leader,/healthz (localhost) | no |
| settlement | Python | `python:3.12-slim` | `:8090` /healthz | no |
| inventory-root | Go | distroless static | `:8080` /availability | yes |
| warehouse-leaf-py | Python | `python:3.12-slim` | `:8080` /shard/availability | yes |
| warehouse-leaf-node | Node | `node:22-slim` | `:8080` /shard/availability | yes |
| inventory-merge | Python | `python:3.12-slim` | `:9090` /merge | yes |
| workqueue-coordinator | Go | distroless static | `:8080` /enqueue,/healthz | yes |
| workqueue-framework | Go | distroless static | `:8091` /healthz | no |
| order-stage | Node | `node:22-slim` | none (file watcher) | no |
| log-sink | Go | distroless static | `:3100` push+query | yes |

Go images cross-compile: `FROM --platform=$BUILDPLATFORM golang:1.24 AS build` +
`CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH`. All run `runAsNonRoot`, drop ALL caps.
Every pod sets `requests: cpu 50m / memory 64Mi` (main apps may ask more).

## 1. Backing stores (plain redis, single-node, emptyDir - NON-DURABLE by design)

- **`redis-workqueue`** Deployment, Service `redis-workqueue:6379`. Holds the work-queue + settlement keys.
- **`cache-shard`** **StatefulSet** (replicas = `SHARD_COUNT`, default 3) + **headless Service**
  `cache-shards`. Stable DNS: `cache-shard-0.cache-shards.orderforge.svc.cluster.local:6379`, `-1`, `-2`.
  These are the ambassador's shard backends.

## 2. Redis key convention (ALL in `redis-workqueue`, never the cache shards)

```
orderforge:ready                 LIST   task_ids ready to claim (coordinator RPUSHes)
orderforge:task:<id>             HASH   {payload<json>, status, attempt, enqueued_at}
orderforge:inflight              ZSET   member=task_id  score=visibility_deadline (epoch seconds)
orderforge:processing:<worker>   LIST   atomic-claim target (framework BLMOVE dest)
orderforge:worker:<worker>       STRING SET EX <2*HEARTBEAT_S>  (liveness; reaper checks existence)
orderforge:done                  SET    completed task_ids (idempotency)
orderforge:dead                  LIST   task_ids past MAX_ATTEMPTS (DLQ)
orderforge:settle:pending        SET    completed order_ids awaiting finalize
orderforge:order:<id>            HASH   {..., finalized: "0"|"1"}
```

**Claim protocol (framework):** `BLMOVE orderforge:ready orderforge:processing:<worker> LEFT RIGHT` (5s
timeout loop) -> `SISMEMBER orderforge:done <id>` (if member: LREM from processing, skip) ->
`ZADD orderforge:inflight <now+VISIBILITY_S> <id>` -> process -> on ack: `SADD orderforge:done <id>`,
`SADD orderforge:settle:pending <order_id>`, `LREM processing 1 <id>`, `ZREM inflight <id>`,
`HSET task:<id> status done`. Heartbeat goroutine every `HEARTBEAT_S`: `ZADD inflight GT` + `SET worker:<w> 1 EX`.

**Reaper (in coordinator, minimal core):** every ~2s: (a) `ZRANGEBYSCORE inflight -inf now` -> for each
expired id: attempt++, requeue to `ready` or move to `dead` if attempt>MAX_ATTEMPTS; (b) scan
`orderforge:processing:*` and for any list whose `orderforge:worker:<w>` key is gone, requeue its ids.

## 3. Work-queue task + file handshake

Task envelope (JSON in `task:<id>.payload` and written to the input file):
```json
{"task_id":"<uuid>","type":"order.process","attempt":1,"enqueued_at":"<RFC3339>",
 "payload":{"order_id":"ord_4816","customer_id":"cust_1","items":[{"sku":"A1","qty":2}],"currency":"USD"}}
```

Shared `emptyDir` mounted at `/work` in BOTH framework and order-stage. Per task, unique dir under the
**stable, never-deleted** parent `/work/tasks/`:
```
/work/tasks/<task_id>/in/order.json     framework writes (tmp -> rename)
/work/tasks/<task_id>/request.ready     framework touches LAST  => input ready
/work/tasks/<task_id>/out/result.json   stage writes (tmp -> rename)
/work/tasks/<task_id>/response.done      stage touches LAST => output ready   (success)
/work/tasks/<task_id>/error.failed       stage touches instead => business rejection
```
Framework deletes `/work/tasks/<task_id>/` only AFTER ack. Stage watches `/work/tasks/` (poll 100ms) for
new subdirs containing `request.ready`. **order-stage = provided `shim.js` (watch loop) + user `stage.js`
(one-shot `node stage.js <in> <out>`)** so the "user writes only file-in/file-out" claim is literal.

`result.json`:
```json
{"order_id":"ord_4816","decision":"accepted","currency":"USD","priced_total":19.98,
 "lines":[{"sku":"A1","qty":2,"unit_price":9.99}],"reason":""}
```

## 4. order-api (Go) - the single-pod trio host

Env: `HTTP_ADDR=:8080`, `STATS_ADDR=127.0.0.1:9000`, `CACHE_ADDR=127.0.0.1:6380`, `LOG_DIR=/var/log/app`,
`INVENTORY_ROOT_URL=http://inventory-root:8080`, `COORDINATOR_URL=http://workqueue-coordinator:8080`.

Endpoints:
- `POST /orders` body `{customer_id, items:[{sku,qty}], currency}` ->
  1) read `cart:<customer_id>` from cache via go-redis @ `CACHE_ADDR` (ambassador);
  2) `POST {INVENTORY_ROOT_URL}/availability` with `{order_id,items}` (scatter/gather);
  3) if `fulfillable`, `POST {COORDINATOR_URL}/enqueue` with the task payload;
  4) return `201 {order_id, task_id, availability}`. Increment native counters throughout.
- `GET /healthz` -> 200 (on :8080).
- `GET /stats` on **:9000** only -> the awkward native JSON (section 6).
- Logs: append JSONL to `/var/log/app/access.log` (one per request) and `/var/log/app/app.log` (events).
  Line schema: `{"ts":"<RFC3339nano>","level":"info","logger":"access","method":..,"path":..,"status":..,"latency_ms":..,"order_id":..,"msg":..}`.

go-redis v9 Options for the ambassador: `Protocol: 2`, `DisableIndentity: true` (v9 field is misspelled
`DisableIndentity`; use `DisableIdentity` if the pinned version renamed it) - prevents HELLO 3 / CLIENT SETINFO.

## 5. cache-ambassador (Go)

Listens `LISTEN_ADDR=127.0.0.1:6380`. Env `SHARDS=cache-shard-0.cache-shards:6379,cache-shard-1.cache-shards:6379,cache-shard-2.cache-shards:6379`
(comma list; `SHARD_COUNT = len`). Speaks a **RESP subset**: `PING`, `GET k`, `SET k v [EX n]`, `SETEX k n v`,
`DEL k`. Routing: `shard = crc32(k) % SHARD_COUNT`; proxy the command to that shard's pooled conn, return its
reply verbatim. Handshake tolerance (REQUIRED): reply `-ERR unknown command 'HELLO'` to `HELLO` (client
falls back to RESP2) and `+OK` to any `CLIENT ...`. Use a real multibulk RESP reader (handle pipelined /
partial reads); `tidwall/redcon` is acceptable. If a spike test against go-redis v9 fails, fall back to an
HTTP cache API (`GET/PUT/DELETE /cache/{key}`) - documented escape hatch only.

## 6. metrics-adapter (Python) + native /stats

Native `order-api :9000/stats` (deliberately awkward: camelCase, latency in **ms**):
```json
{"uptimeSeconds":1234.5,
 "orders":{"receivedTotal":900,"placedTotal":850,"rejectedTotal":50,"inFlight":12},
 "http":{"requestsTotal":{"200":800,"400":40,"500":10},
         "requestLatencyMs":{"count":850,"sum":42000,"buckets":{"5":100,"10":300,"25":600,"50":800,"100":840,"250":850}}},
 "cache":{"hitsTotal":1200,"missesTotal":300}}
```
Adapter env `NATIVE_STATS_URL=http://127.0.0.1:9000/stats`. On each scrape of `:9102/metrics`: GET native
(2s timeout) -> emit Prometheus text `version=0.0.4`. Mapping (prefix `orderforge_`, ms->seconds):
`orders.receivedTotal`->`orderforge_orders_received_total`(counter); `placedTotal`,`rejectedTotal` counters;
`inFlight`->`orderforge_orders_in_flight`(gauge); `http.requestsTotal.<code>`->`orderforge_http_requests_total{code=".."}`;
`cache.hitsTotal/missesTotal`->counters; `uptimeSeconds`->`orderforge_uptime_seconds`(gauge);
`http.requestLatencyMs`->`orderforge_http_request_latency_seconds`(histogram, buckets/1000 cumulative,
`+Inf`==count, `_sum`=sum/1000). Probes: **liveness only** (or self `/healthz`); NEVER gate readiness on
upstream. Upstream down -> HTTP 200 with `orderforge_native_scrape_up 0`.

## 7. leader-elector (Go) + settlement (Python)

leader-elector: `client-go` `leaderelection.RunOrDie` + `resourcelock.LeaseLock`, Lease
`orderforge-settlement` in ns from `POD_NAMESPACE`, identity = `POD_NAME`. Timings
`LeaseDuration 15s > RenewDeadline 10s > RetryPeriod 2s`, `ReleaseOnCancel: true`, SIGTERM->cancel,
`Coordinated: false`. Serves on **:4040**:
```
GET /leader  -> {"identity":"<pod>","leader":"<pod|>","isLeader":true|false,"leaseValidUntil":"<RFC3339>"}
GET /healthz -> 200
```
`leaseValidUntil` = observed renew time + LeaseDuration while leading (else empty/past). settlement env
`LEADER_URL=http://127.0.0.1:4040/leader`, `REDIS_ADDR=redis-workqueue:6379`, `SWEEP_INTERVAL_S=3`,
`:8090/healthz`. Each sweep: GET LEADER_URL; **poll failure/timeout OR stale `leaseValidUntil` => treat as
NOT leader** (fail-closed). If leader: `SMEMBERS orderforge:settle:pending`; for each order_id, idempotently
finalize (`HGET order:<id> finalized`; if not "1": do finalize work, `HSET order:<id> finalized 1`),
`SREM settle:pending order_id`. RBAC: SA `settlement` + namespaced Role verbs
`get,list,watch,create,update,patch` on `coordination.k8s.io/leases`.

## 8. Scatter/Gather

inventory-root (Go) env: `LEAF_ENDPOINTS=http://warehouse-leaf-py:8080,http://warehouse-leaf-node:8080`,
`LEAF_PATH=/shard/availability`, `MERGE_URL=http://inventory-merge:9090/merge`, `SCATTER_TIMEOUT_MS=300`.
`POST /availability {order_id, items:[{sku,qty}]}` -> fan out the SAME body to each leaf concurrently
(per-leaf context timeout; **do not cancel siblings on one failure**) -> POST `{query, partials:[...]}` to
merge -> relay merge's response. Partial envelope:
```json
{"query":{...},"partials":[{"shard":"us-east","ok":true,"status":200,"latency_ms":41,"body":{...}},
                           {"shard":"us-west","ok":false,"error":"timeout"}],
 "meta":{"scattered":2,"succeeded":1,"failed":1}}
```
leaf: `POST /shard/availability {items:[{sku,qty}]}` -> `{"shard":"<SHARD_NAME>","lines":[{"sku","available_qty","unit_price","eta_days"}]}`.
Each leaf has a static per-shard stock map; env `SHARD_NAME` (py=`us-east`, node=`us-west`).
merge: `POST /merge {query,partials}` -> `{"order_id","fulfillable":bool,"allocation":[{sku,warehouse,qty,unit_price,eta_days}],"total_price","max_eta_days","partial":bool}`.
`partial:true` if any shard failed. `fulfillable` if summed available_qty across shards >= requested qty for every sku.

## 9. workqueue-coordinator (Go)

Env `REDIS_ADDR=redis-workqueue:6379`, `MAX_ATTEMPTS=5`, `VISIBILITY_S=30`, `REAPER_INTERVAL_S=2`.
`POST /enqueue` body = the order payload -> mint `task_id`, `HSET task:<id> ...`, `RPUSH orderforge:ready <id>`,
return `201 {"task_id":"<id>"}`. `GET /healthz` 200. Runs the reaper goroutine (section 2).

## 10. log-sink (Go) + log-shipper (Node)

log-sink `:3100`: `POST /loki/api/v1/push` (accepts Loki JSON `{streams:[{stream:{...},values:[["<ns>","<line>"]]}]}`),
stores lines in an in-memory ring (cap ~5000); `GET /query?q=<substr>` returns matching lines (JSON array);
`GET /healthz` 200. log-shipper env `LOG_DIR=/var/log/app`, `SINK_URL=http://log-sink:3100/loki/api/v1/push`,
`POD_NAME`. Tails `*.log` (poll+offset, reset on truncate), batches (>=200 lines or 1s), POSTs Loki format,
advances offset only on 2xx (at-least-once).

## 11. Ports quick-ref (no collisions)
`8080` order-api/inventory-root/coordinator API; `9000` order-api native stats (localhost); `9102` adapter;
`6380` ambassador (localhost); `4040` elector (localhost); `8090` settlement healthz; `8091` framework healthz;
`9090` merge; `3100` log-sink; `6379` all redis.
