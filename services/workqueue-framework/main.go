// Command workqueue-framework is the reusable, language-agnostic worker
// framework container of OrderForge's Work Queue pattern.
//
// It runs beside a user-written "order-stage" container in the same pod, sharing
// an emptyDir at /work. The framework owns ALL queue mechanics (atomic claim,
// idempotency, visibility bookkeeping, heartbeat, ack) and NEVER runs business
// logic. It hands each task to the stage purely through files:
//
//	/work/tasks/<id>/in/order.json     framework writes (tmp -> rename)
//	/work/tasks/<id>/request.ready     framework touches LAST  => input ready
//	/work/tasks/<id>/out/result.json   stage writes (tmp -> rename)
//	/work/tasks/<id>/response.done     stage touches LAST => success
//	/work/tasks/<id>/error.failed      stage touches instead => business rejection
//
// The framework deletes the task dir only AFTER acking to Redis, so the stage is
// never mid-read of a directory that vanishes. See docs/design/CONTRACTS.md
// sections 2, 3, 11 (this file is the authority when they disagree).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis keys (all in redis-workqueue; see CONTRACTS.md section 2).
const (
	readyKey         = "orderforge:ready"
	inflightKey      = "orderforge:inflight"
	doneKey          = "orderforge:done"
	settlePendingKey = "orderforge:settle:pending"
)

func taskKey(id string) string       { return "orderforge:task:" + id }
func processingKey(w string) string  { return "orderforge:processing:" + w }
func workerKey(w string) string      { return "orderforge:worker:" + w }

// blmoveTimeout is the blocking-claim wait per loop iteration. Kept short so the
// SIGTERM check runs promptly between claims.
const blmoveTimeout = 5 * time.Second

// config holds all env-driven settings, resolved once at startup.
type config struct {
	redisAddr      string
	workerID       string
	workDir        string // parent of tasks/, shared emptyDir mount (e.g. /work)
	healthAddr     string
	visibility     time.Duration
	heartbeat      time.Duration
	processTimeout time.Duration
	pollInterval   time.Duration
}

func loadConfig() (config, error) {
	worker := getenv("POD_NAME", "")
	if worker == "" {
		// POD_NAME is injected via the downward API; hostname is a sane local
		// fallback so the binary is runnable outside Kubernetes.
		if h, err := os.Hostname(); err == nil {
			worker = h
		}
	}
	if worker == "" {
		return config{}, errors.New("cannot determine WORKER_ID: POD_NAME unset and hostname unavailable")
	}

	visibility, err := getenvSeconds("VISIBILITY_S", 30)
	if err != nil {
		return config{}, err
	}
	heartbeat, err := getenvSeconds("HEARTBEAT_S", 5)
	if err != nil {
		return config{}, err
	}
	processTimeout, err := getenvSeconds("PROCESS_TIMEOUT_S", 300)
	if err != nil {
		return config{}, err
	}
	pollMS, err := getenvInt("POLL_INTERVAL_MS", 100)
	if err != nil {
		return config{}, err
	}

	return config{
		redisAddr:      getenv("REDIS_ADDR", "redis-workqueue:6379"),
		workerID:       worker,
		workDir:        getenv("WORK_DIR", "/work"),
		healthAddr:     getenv("HEALTH_ADDR", ":8091"),
		visibility:     visibility,
		heartbeat:      heartbeat,
		processTimeout: processTimeout,
		pollInterval:   time.Duration(pollMS) * time.Millisecond,
	}, nil
}

// taskEnvelope mirrors the JSON stored in task:<id>.payload and written verbatim
// to in/order.json. Only the fields the framework needs are modeled; the rest
// pass through opaquely because the framework does not interpret business data.
type taskEnvelope struct {
	Payload struct {
		OrderID string `json:"order_id"`
	} `json:"payload"`
}

// worker orchestrates the claim/process/ack loop for one pod.
type worker struct {
	cfg config
	rdb *redis.Client

	mu      sync.Mutex // guards current
	current string     // task_id in flight, "" when idle (read by heartbeat)
}

func main() {
	log.SetFlags(0) // structured-ish single-line logs; container runtime adds time

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf(`{"level":"error","msg":"config error: %v"}`, err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer rdb.Close()

	w := &worker{cfg: cfg, rdb: rdb}

	// ctx is cancelled on SIGTERM/SIGINT: the claim loop stops claiming and the
	// process exits 0. In-flight work is either finished first or left for the
	// coordinator's visibility-timeout reaper to requeue.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go w.serveHealth(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.heartbeatLoop(ctx)
	}()

	log.Printf(`{"level":"info","msg":"framework started","worker":%q,"redis":%q,"work_dir":%q}`,
		cfg.workerID, cfg.redisAddr, cfg.workDir)

	w.runLoop(ctx)

	stop()    // ensure heartbeat sees cancellation
	wg.Wait() // let heartbeat exit cleanly
	log.Printf(`{"level":"info","msg":"framework stopped","worker":%q}`, cfg.workerID)
}

// runLoop claims and processes tasks until the context is cancelled. It only
// checks for shutdown between tasks (never mid-task), so a claimed task always
// runs to ack or timeout before the process exits.
func (w *worker) runLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		id, err := w.claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown during a blocking claim
			}
			log.Printf(`{"level":"error","msg":"claim failed: %v"}`, err)
			// Brief backoff so a persistent Redis outage doesn't hot-loop.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		if id == "" {
			continue // claim timed out with no task; re-check shutdown and retry
		}

		w.handleTask(ctx, id)
	}
}

// claim performs the atomic move of one ready task into this worker's private
// processing list, then applies the idempotency gate. It returns "" when the
// blocking move times out or when the claimed task was already completed.
func (w *worker) claim(ctx context.Context) (string, error) {
	// BLMOVE ready -> processing:<worker> LEFT RIGHT: atomically pops the head of
	// the ready list and appends it to our processing list, so a crash between
	// pop and bookkeeping cannot lose the task (the reaper finds it in
	// processing:<worker> once our liveness key expires).
	id, err := w.rdb.BLMove(ctx, readyKey, processingKey(w.cfg.workerID), "LEFT", "RIGHT", blmoveTimeout).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // no task within the blocking window
	}
	if err != nil {
		return "", err
	}

	// Idempotency gate: if a previous attempt already completed this task
	// (e.g. it was reaped and re-run), drop our duplicate copy and skip.
	done, err := w.rdb.SIsMember(ctx, doneKey, id).Result()
	if err != nil {
		return "", fmt.Errorf("done-set check for %s: %w", id, err)
	}
	if done {
		if err := w.rdb.LRem(ctx, processingKey(w.cfg.workerID), 1, id).Err(); err != nil {
			log.Printf(`{"level":"warn","msg":"LREM of already-done task failed","task":%q,"err":%q}`, id, err.Error())
		}
		log.Printf(`{"level":"info","msg":"skipped already-done task","task":%q}`, id)
		return "", nil
	}
	return id, nil
}

// handleTask claims visibility, runs the file handshake, and acks. It never
// returns an error: failures are logged and the task is left for the reaper.
func (w *worker) handleTask(ctx context.Context, id string) {
	// Take ownership of the visibility deadline BEFORE doing work, so the reaper
	// won't requeue a task we are actively processing (heartbeat keeps bumping).
	deadline := float64(time.Now().Add(w.cfg.visibility).Unix())
	if err := w.rdb.ZAdd(ctx, inflightKey, redis.Z{Score: deadline, Member: id}).Err(); err != nil {
		log.Printf(`{"level":"error","msg":"ZADD inflight failed; abandoning task","task":%q,"err":%q}`, id, err.Error())
		w.lremProcessing(ctx, id)
		return
	}

	w.setCurrent(id)         // heartbeat now protects this task
	defer w.setCurrent("")   // stop protecting it once we're done either way

	envelope, err := w.rdb.HGet(ctx, taskKey(id), "payload").Result()
	if err != nil {
		log.Printf(`{"level":"error","msg":"HGET payload failed; leaving for reaper","task":%q,"err":%q}`, id, err.Error())
		w.lremProcessing(ctx, id)
		return
	}

	orderID := extractOrderID(envelope, id)

	success, err := w.runStage(ctx, id, envelope)
	if err != nil {
		// Timed out or the context was cancelled: don't ack. Remove our
		// processing copy and leave the inflight entry; once the heartbeat stops
		// bumping it, the reaper requeues it after the visibility deadline.
		log.Printf(`{"level":"warn","msg":"stage did not complete; leaving for reaper","task":%q,"err":%q}`, id, err.Error())
		w.lremProcessing(ctx, id)
		return
	}

	// Ack against a fresh (non-cancelled) context so shutdown mid-task still
	// records the result rather than leaving a completed task to be re-run.
	ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.ack(ackCtx, id, orderID, success); err != nil {
		log.Printf(`{"level":"error","msg":"ack failed; leaving for reaper","task":%q,"err":%q}`, id, err.Error())
		return // dir intentionally NOT deleted: a retry may need it
	}

	// Delete the task dir only AFTER a successful ack.
	if err := os.RemoveAll(w.taskDir(id)); err != nil {
		log.Printf(`{"level":"warn","msg":"task dir cleanup failed","task":%q,"err":%q}`, id, err.Error())
	}

	decision := "done"
	if !success {
		decision = "rejected"
	}
	log.Printf(`{"level":"info","msg":"task acked","task":%q,"order_id":%q,"result":%q}`, id, orderID, decision)
}

// runStage writes the input files and waits for the user-stage's response
// marker. It returns success=true on response.done, success=false on
// error.failed (a business rejection), and an error on timeout/cancellation.
func (w *worker) runStage(ctx context.Context, id, envelope string) (bool, error) {
	dir := w.taskDir(id)

	// Start clean: a leftover dir from a crashed prior attempt must not fool the
	// poll below into reading a stale marker.
	if err := os.RemoveAll(dir); err != nil {
		return false, fmt.Errorf("reset task dir: %w", err)
	}
	inDir := filepath.Join(dir, "in")
	if err := os.MkdirAll(inDir, 0o755); err != nil {
		return false, fmt.Errorf("mkdir in: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "out"), 0o755); err != nil {
		return false, fmt.Errorf("mkdir out: %w", err)
	}

	// Write the payload atomically, THEN touch request.ready last, so the stage
	// (which acts only on the marker) can never read a half-written order.json.
	if err := writeFileAtomic(filepath.Join(inDir, "order.json"), []byte(envelope)); err != nil {
		return false, fmt.Errorf("write order.json: %w", err)
	}
	if err := touch(filepath.Join(dir, "request.ready")); err != nil {
		return false, fmt.Errorf("touch request.ready: %w", err)
	}

	donePath := filepath.Join(dir, "response.done")
	failPath := filepath.Join(dir, "error.failed")
	deadline := time.Now().Add(w.cfg.processTimeout)
	ticker := time.NewTicker(w.cfg.pollInterval)
	defer ticker.Stop()

	for {
		if exists(failPath) {
			return false, nil // business rejection
		}
		if exists(donePath) {
			return true, nil // success; caller reads out/result.json if needed
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("stage timeout after %s", w.cfg.processTimeout)
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ack finalizes a completed task in one transaction: mark done (idempotency),
// enqueue the order for settlement (success only), drop our processing copy,
// clear the visibility entry, and record the terminal status.
func (w *worker) ack(ctx context.Context, id, orderID string, success bool) error {
	status := "done"
	if !success {
		status = "failed"
	}
	pipe := w.rdb.TxPipeline()
	pipe.SAdd(ctx, doneKey, id)
	if success && orderID != "" {
		pipe.SAdd(ctx, settlePendingKey, orderID)
	}
	pipe.LRem(ctx, processingKey(w.cfg.workerID), 1, id)
	pipe.ZRem(ctx, inflightKey, id)
	pipe.HSet(ctx, taskKey(id), "status", status)
	_, err := pipe.Exec(ctx)
	return err
}

// lremProcessing removes our copy of a task we are abandoning, best-effort.
func (w *worker) lremProcessing(ctx context.Context, id string) {
	c := ctx
	if c.Err() != nil {
		c = context.Background()
	}
	if err := w.rdb.LRem(c, processingKey(w.cfg.workerID), 1, id).Err(); err != nil {
		log.Printf(`{"level":"warn","msg":"LREM processing failed","task":%q,"err":%q}`, id, err.Error())
	}
}

// heartbeatLoop keeps this worker's liveness key alive and, while a task is in
// flight, extends its visibility deadline (GT = only ever push the deadline
// forward). The liveness key lets the reaper distinguish a live worker's
// processing list from a dead one's.
func (w *worker) heartbeatLoop(ctx context.Context) {
	ttl := 2 * w.cfg.heartbeat
	beat := func() {
		bctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := w.rdb.Set(bctx, workerKey(w.cfg.workerID), "1", ttl).Err(); err != nil {
			log.Printf(`{"level":"warn","msg":"heartbeat SET worker failed","err":%q}`, err.Error())
		}
		if id := w.getCurrent(); id != "" {
			deadline := float64(time.Now().Add(w.cfg.visibility).Unix())
			args := redis.ZAddArgs{GT: true, Members: []redis.Z{{Score: deadline, Member: id}}}
			if err := w.rdb.ZAddArgs(bctx, inflightKey, args).Err(); err != nil {
				log.Printf(`{"level":"warn","msg":"heartbeat ZADD GT failed","task":%q,"err":%q}`, id, err.Error())
			}
		}
	}

	beat() // publish liveness immediately so the reaper sees us before first claim
	ticker := time.NewTicker(w.cfg.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			beat()
		}
	}
}

// serveHealth exposes GET /healthz on :8091 for the Kubernetes liveness probe.
func (w *worker) serveHealth(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: w.cfg.healthAddr, Handler: mux}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf(`{"level":"error","msg":"health server error: %v"}`, err)
	}
}

func (w *worker) taskDir(id string) string {
	return filepath.Join(w.cfg.workDir, "tasks", id)
}

func (w *worker) setCurrent(id string) {
	w.mu.Lock()
	w.current = id
	w.mu.Unlock()
}

func (w *worker) getCurrent() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.current
}

// extractOrderID pulls payload.order_id from the envelope; on a parse miss it
// logs and returns "" (settlement enqueue is then skipped rather than wrong).
func extractOrderID(envelope, taskID string) string {
	var t taskEnvelope
	if err := json.Unmarshal([]byte(envelope), &t); err != nil {
		log.Printf(`{"level":"warn","msg":"cannot parse envelope for order_id","task":%q,"err":%q}`, taskID, err.Error())
		return ""
	}
	return t.Payload.OrderID
}

// writeFileAtomic writes data to a temp file in the same directory and renames
// it into place; rename is atomic within a directory, so a reader never sees a
// partial file.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func touch(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("env %s: %w", key, err)
	}
	return n, nil
}

func getenvSeconds(key string, def int) (time.Duration, error) {
	n, err := getenvInt(key, def)
	if err != nil {
		return 0, err
	}
	return time.Duration(n) * time.Second, nil
}
