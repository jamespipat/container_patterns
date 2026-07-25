// Command workqueue-coordinator is the reliable-queue coordinator for OrderForge.
//
// It owns two responsibilities described in CONTRACTS.md (sections 2 and 9):
//
//  1. POST /enqueue - mint a task, persist its envelope, and push it onto the
//     ready list so a worker framework can claim it.
//  2. A reaper goroutine that keeps the queue self-healing: it requeues tasks
//     whose visibility deadline lapsed and drains the per-worker processing
//     lists of dead workers.
//
// The coordinator talks only to redis-workqueue; it never touches the cache
// shards. All keys live under the "orderforge:" namespace.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// config holds the coordinator's runtime settings, all sourced from the env
// vars named in CONTRACTS.md section 9.
type config struct {
	redisAddr      string
	maxAttempts    int
	visibilityS    int // framework-owned; logged for parity, not used on enqueue
	reaperInterval time.Duration
	httpAddr       string
}

func loadConfig() config {
	return config{
		redisAddr:      getEnv("REDIS_ADDR", "redis-workqueue:6379"),
		maxAttempts:    getEnvInt("MAX_ATTEMPTS", 5),
		visibilityS:    getEnvInt("VISIBILITY_S", 30),
		reaperInterval: time.Duration(getEnvInt("REAPER_INTERVAL_S", 2)) * time.Second,
		httpAddr:       ":8080",
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%q, using default %d: %v", key, v, def, err)
		return def
	}
	return n
}

func main() {
	cfg := loadConfig()
	log.Printf("workqueue-coordinator starting: redis=%s max_attempts=%d visibility_s=%d reaper_interval=%s http=%s",
		cfg.redisAddr, cfg.maxAttempts, cfg.visibilityS, cfg.reaperInterval, cfg.httpAddr)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer rdb.Close()

	// Root context cancelled on SIGTERM/SIGINT so the reaper and in-flight
	// requests unwind cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	coord := &coordinator{rdb: rdb, cfg: cfg}

	mux := http.NewServeMux()
	mux.HandleFunc("/enqueue", coord.handleEnqueue)
	mux.HandleFunc("/healthz", handleHealthz)

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Reaper runs for the lifetime of the process; it stops when ctx is done.
	reaperDone := make(chan struct{})
	go func() {
		defer close(reaperDone)
		coord.runReaper(ctx)
	}()

	// Serve until a signal arrives, then drain gracefully.
	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	select {
	case <-ctx.Done():
		log.Print("shutdown signal received, draining")
	case err := <-serverErr:
		if err != nil {
			log.Fatalf("http server failed: %v", err)
		}
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}
	<-reaperDone
	log.Print("workqueue-coordinator stopped")
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
