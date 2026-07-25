package main

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// requeueExpiredScript atomically requeues a single expired in-flight task.
//
// It only acts if the id is still a member of the inflight ZSET (ZREM > 0), so
// concurrent reapers, or a worker that acked between our scan and this call,
// can never cause a double requeue. The hash `attempt` counter is the source of
// truth for the DLQ decision.
//
//	KEYS[1] = inflight ZSET   KEYS[2] = ready LIST
//	KEYS[3] = dead LIST       KEYS[4] = task:<id> HASH
//	ARGV[1] = task_id         ARGV[2] = max_attempts
//
// returns: 0 not-inflight (skipped), 1 requeued to ready, 2 moved to dead.
var requeueExpiredScript = redis.NewScript(`
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then return 0 end
local attempt = redis.call('HINCRBY', KEYS[4], 'attempt', 1)
if attempt > tonumber(ARGV[2]) then
  redis.call('RPUSH', KEYS[3], ARGV[1])
  redis.call('HSET', KEYS[4], 'status', 'dead')
  return 2
end
redis.call('RPUSH', KEYS[2], ARGV[1])
redis.call('HSET', KEYS[4], 'status', 'ready')
return 1
`)

// drainDeadWorkerScript requeues every id stranded in a dead worker's
// processing list, then clears the list. The whole thing runs atomically, so
// the liveness re-check and the drain cannot interleave with a worker.
//
// The worker is considered dead only if its liveness key is absent (it lets its
// SET EX 2*HEARTBEAT_S lapse). Each id is also removed from the inflight ZSET so
// the visibility-timeout path (requeueExpiredScript) will not requeue it again.
// Task hash keys are addressed by concatenating ARGV[3]; this is safe on the
// single-node redis-workqueue (not a cluster).
//
//	KEYS[1] = processing:<w> LIST   KEYS[2] = worker:<w> STRING
//	KEYS[3] = ready LIST            KEYS[4] = dead LIST
//	KEYS[5] = inflight ZSET
//	ARGV[1] = max_attempts          ARGV[2] = task_prefix ("orderforge:task:")
//
// returns: -1 worker still alive (skipped), otherwise count of ids requeued.
var drainDeadWorkerScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then return -1 end
local ids = redis.call('LRANGE', KEYS[1], 0, -1)
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[5], id)
  local tk = ARGV[2] .. id
  local attempt = redis.call('HINCRBY', tk, 'attempt', 1)
  if attempt > tonumber(ARGV[1]) then
    redis.call('RPUSH', KEYS[4], id)
    redis.call('HSET', tk, 'status', 'dead')
  else
    redis.call('RPUSH', KEYS[3], id)
    redis.call('HSET', tk, 'status', 'ready')
  end
end
redis.call('DEL', KEYS[1])
return #ids
`)

// runReaper runs both reaper passes on every tick until ctx is cancelled
// (CONTRACTS.md section 2). A failing pass is logged and retried on the next
// tick; the loop never exits on transient Redis errors.
func (c *coordinator) runReaper(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.reaperInterval)
	defer ticker.Stop()
	log.Printf("reaper started, interval=%s", c.cfg.reaperInterval)

	for {
		select {
		case <-ctx.Done():
			log.Print("reaper stopped")
			return
		case <-ticker.C:
			c.reapExpiredInflight(ctx)
			c.reapDeadWorkers(ctx)
		}
	}
}

// reapExpiredInflight handles pass (a): tasks whose visibility deadline has
// passed are requeued, or moved to the DLQ once past MAX_ATTEMPTS.
func (c *coordinator) reapExpiredInflight(ctx context.Context) {
	now := strconv.FormatInt(time.Now().Unix(), 10)
	ids, err := c.rdb.ZRangeByScore(ctx, inflightKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: now,
	}).Result()
	if err != nil {
		log.Printf("reaper: ZRANGEBYSCORE inflight failed: %v", err)
		return
	}

	maxAttempts := strconv.Itoa(c.cfg.maxAttempts)
	for _, id := range ids {
		res, err := requeueExpiredScript.Run(ctx, c.rdb,
			[]string{inflightKey, readyKey, deadKey, taskKey(id)},
			id, maxAttempts,
		).Int()
		if err != nil {
			log.Printf("reaper: requeue of expired task %s failed: %v", id, err)
			continue
		}
		switch res {
		case 1:
			log.Printf("reaper: requeued expired task %s to ready", id)
		case 2:
			log.Printf("reaper: task %s exceeded max_attempts, moved to dead", id)
		}
	}
}

// reapDeadWorkers handles pass (b): scan the per-worker processing lists and
// drain any whose liveness key has vanished.
func (c *coordinator) reapDeadWorkers(ctx context.Context) {
	maxAttempts := strconv.Itoa(c.cfg.maxAttempts)
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, processingPrefix+"*", 100).Result()
		if err != nil {
			log.Printf("reaper: SCAN processing:* failed: %v", err)
			return
		}
		for _, procKey := range keys {
			worker := strings.TrimPrefix(procKey, processingPrefix)
			res, err := drainDeadWorkerScript.Run(ctx, c.rdb,
				[]string{procKey, workerPrefix + worker, readyKey, deadKey, inflightKey},
				maxAttempts, taskPrefix,
			).Int()
			if err != nil {
				log.Printf("reaper: draining processing list for worker %s failed: %v", worker, err)
				continue
			}
			if res > 0 {
				log.Printf("reaper: worker %s is dead, requeued %d stranded task(s)", worker, res)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}
