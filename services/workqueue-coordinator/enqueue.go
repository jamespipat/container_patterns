package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// coordinator carries the shared dependencies for the HTTP handlers and reaper.
type coordinator struct {
	rdb *redis.Client
	cfg config
}

// orderPayload is the body order-api POSTs to /enqueue: the inner order the
// worker will process. Only order_id is load-bearing for the coordinator (it
// keys the per-order finalize state); the rest is preserved verbatim inside the
// task envelope and never inspected here.
type orderPayload struct {
	OrderID string `json:"order_id"`
}

// taskEnvelope is the JSON stored in task:<id>.payload and later written to the
// worker's input file (CONTRACTS.md section 3). The initial order body is
// embedded verbatim as RawMessage so no field is lost or reordered.
type taskEnvelope struct {
	TaskID     string          `json:"task_id"`
	Type       string          `json:"type"`
	Attempt    int             `json:"attempt"`
	EnqueuedAt string          `json:"enqueued_at"`
	Payload    json.RawMessage `json:"payload"`
}

// handleEnqueue implements POST /enqueue (CONTRACTS.md section 9): mint a
// task_id, persist the task hash, register the per-order finalize state, push
// onto the ready list, and return 201 {"task_id": ...}.
func (c *coordinator) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// Read the raw body so we can both validate it and store it byte-for-byte.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unable to read request body")
		return
	}

	var order orderPayload
	if err := json.Unmarshal(body, &order); err != nil {
		writeError(w, http.StatusBadRequest, "body is not valid JSON")
		return
	}
	if order.OrderID == "" {
		writeError(w, http.StatusBadRequest, "order_id is required")
		return
	}

	taskID := uuid.NewString()
	enqueuedAt := time.Now().UTC().Format(time.RFC3339)

	envelope := taskEnvelope{
		TaskID:     taskID,
		Type:       "order.process",
		Attempt:    1,
		EnqueuedAt: enqueuedAt,
		Payload:    json.RawMessage(body),
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to build task envelope")
		return
	}

	// Persist task state, register the order for finalization, and make the
	// task claimable. A pipeline keeps this to a single round trip; on a
	// single-node redis these commands are applied in order. HSET on the task
	// hash before RPUSH guarantees a claiming worker never sees a ready id
	// whose hash is missing.
	pipe := c.rdb.TxPipeline()
	pipe.HSet(r.Context(), taskKey(taskID), map[string]any{
		"payload":     string(envelopeJSON),
		"status":      "ready",
		"attempt":     1,
		"enqueued_at": enqueuedAt,
	})
	pipe.HSet(r.Context(), orderKey(order.OrderID), "finalized", "0")
	pipe.RPush(r.Context(), readyKey, taskID)
	if _, err := pipe.Exec(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to enqueue task")
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"task_id": taskID})
}

// maxBodyBytes caps the enqueue body; order payloads are tiny and this stops a
// malformed or hostile client from streaming an unbounded request.
const maxBodyBytes = 1 << 20 // 1 MiB

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
