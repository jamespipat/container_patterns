// Command log-sink is a tiny in-memory stand-in for Loki used by the OrderForge
// demo. It accepts Loki push requests, keeps the most recent log lines in a
// fixed-size ring buffer, and lets you grep them back out over HTTP.
//
// Contract (CONTRACTS.md section 10):
//   POST /loki/api/v1/push  - Loki JSON {streams:[{stream:{...},values:[[ts,line]]}]}
//   GET  /query?q=<substr>  - JSON array of stored records whose line contains q
//   GET  /healthz           - 200 OK
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	listenAddr  = ":3100"
	ringCap     = 5000            // most-recent lines retained; older ones are overwritten
	maxBodySize = 8 << 20         // 8 MiB cap on a push body to bound memory per request
	shutdownGP  = 5 * time.Second // grace period for in-flight requests on SIGTERM
)

// record is one stored log line plus the low-cardinality labels it arrived with.
// ts is the nanosecond-epoch timestamp string from the Loki value tuple, kept
// verbatim because it is opaque to the sink.
type record struct {
	TS     string            `json:"ts"`
	Labels map[string]string `json:"labels,omitempty"`
	Line   string            `json:"line"`
}

// ring is a fixed-capacity, overwrite-oldest buffer of records, safe for
// concurrent push and query. It holds at most cap records; once full, each new
// record overwrites the oldest.
type ring struct {
	mu    sync.RWMutex
	buf   []record
	next  int  // index where the next record will be written
	full  bool // whether buf has wrapped at least once
	cap   int
}

func newRing(capacity int) *ring {
	return &ring{buf: make([]record, capacity), cap: capacity}
}

func (r *ring) add(rec record) {
	r.mu.Lock()
	r.buf[r.next] = rec
	r.next = (r.next + 1) % r.cap
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

// snapshot returns the stored records in insertion order (oldest first).
func (r *ring) snapshot() []record {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.full {
		out := make([]record, r.next)
		copy(out, r.buf[:r.next])
		return out
	}
	out := make([]record, 0, r.cap)
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}

// Loki push wire format. Only the fields we consume are modelled.
type pushRequest struct {
	Streams []struct {
		Stream map[string]string `json:"stream"`
		// Values is a list of [timestamp, line] tuples. Loki also permits a
		// third structured-metadata element which we ignore.
		Values [][]string `json:"values"`
	} `json:"streams"`
}

type server struct {
	ring *ring
}

func (s *server) handlePush(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	req.Body = http.MaxBytesReader(w, req.Body, maxBodySize)
	var pr pushRequest
	if err := json.NewDecoder(req.Body).Decode(&pr); err != nil {
		http.Error(w, "invalid loki push json: "+err.Error(), http.StatusBadRequest)
		return
	}

	stored := 0
	for _, st := range pr.Streams {
		for _, v := range st.Values {
			// A well-formed value is [timestamp, line]; tolerate line-only.
			var ts, line string
			switch len(v) {
			case 0:
				continue
			case 1:
				line = v[0]
			default:
				ts, line = v[0], v[1]
			}
			s.ring.add(record{TS: ts, Labels: st.Stream, Line: line})
			stored++
		}
	}

	w.WriteHeader(http.StatusNoContent) // Loki returns 204 on a successful push
	log.Printf("push: streams=%d lines=%d", len(pr.Streams), stored)
}

func (s *server) handleQuery(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query().Get("q")
	all := s.ring.snapshot()

	// Return newest-first so a grep surfaces the most recent matches first.
	matches := make([]record, 0)
	for i := len(all) - 1; i >= 0; i-- {
		if q == "" || strings.Contains(all[i].Line, q) {
			matches = append(matches, all[i])
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(matches); err != nil {
		log.Printf("query: encode error: %v", err)
	}
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func main() {
	capacity := ringCap
	if v := os.Getenv("RING_CAP"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid RING_CAP %q: must be a positive integer", v)
		}
		capacity = n
	}

	srv := &server{ring: newRing(capacity)}

	mux := http.NewServeMux()
	mux.HandleFunc("/loki/api/v1/push", srv.handlePush)
	mux.HandleFunc("/query", srv.handleQuery)
	mux.HandleFunc("/healthz", handleHealthz)

	httpServer := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Graceful shutdown on SIGTERM/SIGINT so in-flight pushes/queries finish.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	go func() {
		log.Printf("log-sink listening on %s (ring cap %d)", listenAddr, capacity)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("shutdown signal received, draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGP)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
