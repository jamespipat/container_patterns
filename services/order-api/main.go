// Command order-api is the OrderForge front-door HTTP service.
//
// It hosts three colocated sidecar patterns from the app's point of view:
//   - AMBASSADOR: it dials a plain go-redis client at CACHE_ADDR, believing it is
//     talking to one Redis, while a localhost ambassador shards behind it.
//   - ADAPTER: it exposes a deliberately non-Prometheus native /stats surface on a
//     separate internal-only listener (STATS_ADDR); a Python adapter normalizes it.
//   - SIDECAR: it appends JSONL logs to files under LOG_DIR that a Node shipper tails.
//
// The request path for POST /orders is: cache read -> inventory-root /availability
// (scatter/gather) -> if fulfillable, coordinator /enqueue -> 201.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// config holds all runtime configuration, sourced exclusively from env vars per
// CONTRACTS.md section 4. Defaults match the contract so the binary runs locally.
type config struct {
	httpAddr       string
	statsAddr      string
	cacheAddr      string
	logDir         string
	inventoryURL   string
	coordinatorURL string
}

func loadConfig() config {
	return config{
		httpAddr:       env("HTTP_ADDR", ":8080"),
		statsAddr:      env("STATS_ADDR", "127.0.0.1:9000"),
		cacheAddr:      env("CACHE_ADDR", "127.0.0.1:6380"),
		logDir:         env("LOG_DIR", "/var/log/app"),
		inventoryURL:   env("INVENTORY_ROOT_URL", "http://inventory-root:8080"),
		coordinatorURL: env("COORDINATOR_URL", "http://workqueue-coordinator:8080"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	cfg := loadConfig()

	lg, err := newLogger(cfg.logDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "order-api: cannot open log dir %q: %v\n", cfg.logDir, err)
		os.Exit(1)
	}
	defer lg.close()

	// go-redis v9 talking to the ambassador. Protocol:2 + DisableIndentity:true keep
	// the client on RESP2 and suppress the HELLO 3 / CLIENT SETINFO handshake that the
	// minimal ambassador does not implement. DisableIndentity is the (misspelled) v9
	// field name; the pinned v9.5.1 has not renamed it.
	rdb := redis.NewClient(&redis.Options{
		Addr:             cfg.cacheAddr,
		Protocol:         2,
		DisableIndentity: true,
	})
	defer rdb.Close()

	srv := &server{
		cfg:    cfg,
		log:    lg,
		stats:  newStats(),
		cache:  rdb,
		client: &http.Client{Timeout: 5 * time.Second},
	}

	// Two independent listeners: the public API on HTTP_ADDR and the internal-only
	// native stats surface on STATS_ADDR (never placed in a Service).
	apiSrv := &http.Server{Addr: cfg.httpAddr, Handler: srv.apiMux()}
	statsSrv := &http.Server{Addr: cfg.statsAddr, Handler: srv.statsMux()}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	errCh := make(chan error, 2)
	go func() { errCh <- listen(apiSrv, "api") }()
	go func() { errCh <- listen(statsSrv, "stats") }()

	lg.app("info", "order-api started", map[string]any{
		"http_addr": cfg.httpAddr, "stats_addr": cfg.statsAddr, "cache_addr": cfg.cacheAddr,
	})

	select {
	case <-ctx.Done():
		lg.app("info", "shutdown signal received, draining", nil)
	case err := <-errCh:
		// A listener failed to bind/serve; treat as fatal.
		lg.app("error", "listener failed", map[string]any{"error": err.Error()})
	}

	// Graceful drain: stop accepting, let in-flight requests finish (bounded).
	drainCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = apiSrv.Shutdown(drainCtx)
	_ = statsSrv.Shutdown(drainCtx)
	lg.app("info", "order-api stopped", nil)
}

// listen runs a server and returns nil on a clean shutdown, or the error otherwise.
func listen(s *http.Server, name string) error {
	if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s server: %w", name, err)
	}
	return nil
}

// server bundles the request-handling dependencies.
type server struct {
	cfg    config
	log    *logger
	stats  *stats
	cache  *redis.Client
	client *http.Client
}

func (s *server) apiMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", s.withAccessLog(s.handleOrders))
	mux.HandleFunc("/healthz", s.withAccessLog(s.handleHealthz))
	return mux
}

func (s *server) statsMux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", s.handleStats)
	return mux
}

// --- wire types (CONTRACTS.md sections 3, 4, 8, 9) ---

type item struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type orderRequest struct {
	CustomerID string `json:"customer_id"`
	Items      []item `json:"items"`
	Currency   string `json:"currency"`
}

type availabilityRequest struct {
	OrderID string `json:"order_id"`
	Items   []item `json:"items"`
}

// enqueueRequest is the order payload POSTed to the coordinator (section 9).
type enqueueRequest struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Items      []item `json:"items"`
	Currency   string `json:"currency"`
}

type orderResponse struct {
	OrderID      string          `json:"order_id"`
	TaskID       string          `json:"task_id"`
	Availability json.RawMessage `json:"availability"`
}

// handleOrders implements the POST /orders pipeline.
func (s *server) handleOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	// inFlight is a live gauge of concurrently handled orders.
	s.stats.orderReceived()
	defer s.stats.orderDone()

	var req orderRequest
	body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err := json.Unmarshal(body, &req); err != nil || req.CustomerID == "" || len(req.Items) == 0 {
		s.stats.orderRejected()
		s.log.app("warn", "invalid order request", map[string]any{"error": errString(err)})
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid order request"})
		return
	}

	orderID := newOrderID()
	setOrderID(w, orderID)

	// 1) Cache read via the ambassador. The value is not used to alter the order in
	// this teaching artifact; the read exists to exercise the ambassador path and to
	// feed the cache hit/miss counters.
	s.readCart(r.Context(), req.CustomerID)

	// 2) Availability (scatter/gather) via inventory-root.
	availRaw, fulfillable, err := s.checkAvailability(r.Context(), orderID, req.Items)
	if err != nil {
		s.stats.orderRejected()
		s.log.app("error", "availability check failed", map[string]any{"order_id": orderID, "error": err.Error()})
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "availability check failed", "order_id": orderID})
		return
	}

	if !fulfillable {
		s.stats.orderRejected()
		s.log.app("info", "order rejected: not fulfillable", map[string]any{"order_id": orderID})
		writeJSON(w, http.StatusOK, orderResponse{OrderID: orderID, TaskID: "", Availability: availRaw})
		return
	}

	// 3) Enqueue for asynchronous fulfilment via the coordinator.
	taskID, err := s.enqueue(r.Context(), enqueueRequest{
		OrderID: orderID, CustomerID: req.CustomerID, Items: req.Items, Currency: req.Currency,
	})
	if err != nil {
		s.stats.orderRejected()
		s.log.app("error", "enqueue failed", map[string]any{"order_id": orderID, "error": err.Error()})
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "enqueue failed", "order_id": orderID})
		return
	}

	s.stats.orderPlaced()
	s.log.app("info", "order placed", map[string]any{"order_id": orderID, "task_id": taskID})
	writeJSON(w, http.StatusCreated, orderResponse{OrderID: orderID, TaskID: taskID, Availability: availRaw})
}

func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readCart performs the ambassador cache lookup and records hit/miss, then WRITES the
// key back through the ambassador (write-through) so a returning customer is a cache hit
// and, more importantly for the demo, so keys actually land on the shards the ambassador
// routes to. A transport error (ambassador unreachable) is logged and treated as a
// non-fatal miss. Both the GET and the SET are oblivious to sharding - that is the point.
func (s *server) readCart(ctx context.Context, customerID string) {
	key := "cart:" + customerID
	_, err := s.cache.Get(ctx, key).Result()
	switch {
	case err == nil:
		s.stats.cacheHit()
	case errors.Is(err, redis.Nil):
		s.stats.cacheMiss()
	default:
		s.stats.cacheMiss()
		s.log.app("warn", "cache read error", map[string]any{"key": key, "error": err.Error()})
	}
	// Write-through: seed/refresh the cart entry (1h TTL). Best-effort; a failure here
	// must never block an order, so we only log it.
	if err := s.cache.Set(ctx, key, "seen", time.Hour).Err(); err != nil {
		s.log.app("warn", "cache write error", map[string]any{"key": key, "error": err.Error()})
	}
}

// checkAvailability POSTs to inventory-root and returns the raw response body (for
// passthrough), whether the order is fulfillable, and any transport/protocol error.
func (s *server) checkAvailability(ctx context.Context, orderID string, items []item) (json.RawMessage, bool, error) {
	raw, err := s.postJSON(ctx, s.cfg.inventoryURL+"/availability", availabilityRequest{OrderID: orderID, Items: items})
	if err != nil {
		return nil, false, err
	}
	var parsed struct {
		Fulfillable bool `json:"fulfillable"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, false, fmt.Errorf("decode availability: %w", err)
	}
	return json.RawMessage(raw), parsed.Fulfillable, nil
}

// enqueue POSTs the order payload to the coordinator and returns the minted task id.
func (s *server) enqueue(ctx context.Context, payload enqueueRequest) (string, error) {
	raw, err := s.postJSON(ctx, s.cfg.coordinatorURL+"/enqueue", payload)
	if err != nil {
		return "", err
	}
	var parsed struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return "", fmt.Errorf("decode enqueue: %w", err)
	}
	if parsed.TaskID == "" {
		return "", errors.New("coordinator returned empty task_id")
	}
	return parsed.TaskID, nil
}

// postJSON marshals v, POSTs it, and returns the response body for any 2xx, or an
// error (including a non-2xx status) otherwise.
func (s *server) postJSON(ctx context.Context, url string, v any) ([]byte, error) {
	buf, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s returned %d", url, resp.StatusCode)
	}
	return respBody, nil
}

// --- helpers ---

func newOrderID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "ord_" + hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
