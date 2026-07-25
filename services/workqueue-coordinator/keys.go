package main

// Redis key names and helpers, kept in one place so the namespace convention
// from CONTRACTS.md section 2 lives in exactly one spot.
const (
	namespace = "orderforge:"

	readyKey    = namespace + "ready"    // LIST  task_ids ready to claim
	inflightKey = namespace + "inflight" // ZSET  member=task_id score=deadline
	deadKey     = namespace + "dead"     // LIST  DLQ (past MAX_ATTEMPTS)

	taskPrefix       = namespace + "task:"       // HASH   per-task envelope + bookkeeping
	orderPrefix      = namespace + "order:"      // HASH   per-order state ({finalized})
	processingPrefix = namespace + "processing:" // LIST   per-worker claim target
	workerPrefix     = namespace + "worker:"     // STRING per-worker liveness key
)

func taskKey(id string) string  { return taskPrefix + id }
func orderKey(id string) string { return orderPrefix + id }
