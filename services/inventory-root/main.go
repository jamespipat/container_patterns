// Command inventory-root is the generic scatter/gather framework for OrderForge.
//
// It owns NO business logic. On POST /availability it fans the client's request
// body out, unchanged, to every configured warehouse leaf concurrently, collects
// each leaf's partial result (or failure) without letting one slow/failing leaf
// affect the others, wraps the partials in an envelope, POSTs that to the merge
// service, and relays merge's response back to the caller verbatim.
//
// See docs/design/CONTRACTS.md section 8.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// config holds the fully-resolved runtime configuration read from the
// environment at startup. Resolving it once keeps request handling free of
// env lookups and makes misconfiguration fail loudly at boot.
type config struct {
	httpAddr      string
	leafEndpoints []string
	leafPath      string
	mergeURL      string
	scatterTO     time.Duration
}

func loadConfig() (config, error) {
	c := config{
		httpAddr: ":8080",
		leafPath: getenv("LEAF_PATH", "/shard/availability"),
		mergeURL: getenv("MERGE_URL", "http://inventory-merge:9090/merge"),
	}

	raw := strings.TrimSpace(os.Getenv("LEAF_ENDPOINTS"))
	if raw == "" {
		return config{}, errors.New("LEAF_ENDPOINTS is required (comma-separated leaf base URLs)")
	}
	for _, e := range strings.Split(raw, ",") {
		if e = strings.TrimSpace(e); e != "" {
			c.leafEndpoints = append(c.leafEndpoints, e)
		}
	}
	if len(c.leafEndpoints) == 0 {
		return config{}, errors.New("LEAF_ENDPOINTS contained no non-empty entries")
	}

	ms := 300
	if v := strings.TrimSpace(os.Getenv("SCATTER_TIMEOUT_MS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return config{}, errors.New("SCATTER_TIMEOUT_MS must be a positive integer")
		}
		ms = n
	}
	c.scatterTO = time.Duration(ms) * time.Millisecond
	return c, nil
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// partial is one leaf's contribution to the gather envelope. On success it
// carries the leaf's parsed body and shard label; on failure it carries the
// error string and ok=false. The shape matches CONTRACTS.md section 8.
type partial struct {
	Endpoint  string          `json:"endpoint"`
	Shard     string          `json:"shard,omitempty"`
	OK        bool            `json:"ok"`
	Status    int             `json:"status,omitempty"`
	LatencyMs int64           `json:"latency_ms"`
	Error     string          `json:"error,omitempty"`
	Body      json.RawMessage `json:"body,omitempty"`
}

// envelope is what the root POSTs to the merge service: the original client
// query plus every leaf's partial and a small summary.
type envelope struct {
	Query    json.RawMessage `json:"query"`
	Partials []partial       `json:"partials"`
	Meta     meta            `json:"meta"`
}

type meta struct {
	Scattered int `json:"scattered"`
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
}

type server struct {
	cfg    config
	client *http.Client
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("inventory-root: config error: %v", err)
	}

	s := &server{
		cfg: cfg,
		// No per-client timeout here: per-leaf deadlines are enforced via
		// context so that a single slow leaf cannot stall siblings and each
		// scatter/merge call carries its own deadline.
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/availability", s.handleAvailability)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})

	srv := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("inventory-root: listening on %s, leaves=%v path=%s merge=%s timeout=%s",
		cfg.httpAddr, cfg.leafEndpoints, cfg.leafPath, cfg.mergeURL, cfg.scatterTO)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("inventory-root: server error: %v", err)
	}
}

func (s *server) handleAvailability(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	if !json.Valid(body) {
		http.Error(w, "request body must be valid JSON", http.StatusBadRequest)
		return
	}

	// Scatter to all leaves concurrently; the parent context is bounded only by
	// the caller's connection, while each leaf request gets its own timeout.
	partials := s.scatter(r.Context(), body)

	env := envelope{
		Query:    json.RawMessage(body),
		Partials: partials,
		Meta:     summarize(partials),
	}

	// Gather: hand the whole envelope to merge and relay its answer verbatim.
	s.relayMerge(w, r.Context(), env)
}

// scatter fans body out to every leaf concurrently and returns one partial per
// leaf, in configured order. A failure on one leaf never cancels the others:
// each leaf request has an independent, per-leaf timeout context.
func (s *server) scatter(parent context.Context, body []byte) []partial {
	partials := make([]partial, len(s.cfg.leafEndpoints))
	var wg sync.WaitGroup
	for i, ep := range s.cfg.leafEndpoints {
		wg.Add(1)
		go func(i int, ep string) {
			defer wg.Done()
			partials[i] = s.callLeaf(parent, ep, body)
		}(i, ep)
	}
	wg.Wait()
	return partials
}

// callLeaf performs a single leaf request under its own timeout and shapes the
// result into a partial. Any error (timeout, dial, non-2xx, unreadable body)
// yields ok=false with a description rather than propagating - the whole point
// of scatter/gather is that partial failure is expected and surfaced, not fatal.
func (s *server) callLeaf(parent context.Context, endpoint string, body []byte) partial {
	p := partial{Endpoint: endpoint}
	url := strings.TrimRight(endpoint, "/") + s.cfg.leafPath

	ctx, cancel := context.WithTimeout(parent, s.cfg.scatterTO)
	defer cancel()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		p.Error = "bad request: " + err.Error()
		p.LatencyMs = time.Since(start).Milliseconds()
		return p
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			p.Error = "timeout"
		} else {
			p.Error = err.Error()
		}
		p.LatencyMs = time.Since(start).Milliseconds()
		return p
	}
	defer resp.Body.Close()

	leafBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	p.LatencyMs = time.Since(start).Milliseconds()
	p.Status = resp.StatusCode
	if err != nil {
		p.Error = "failed to read leaf response: " + err.Error()
		return p
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		p.Error = "leaf returned status " + strconv.Itoa(resp.StatusCode)
		return p
	}
	if !json.Valid(leafBody) {
		p.Error = "leaf returned non-JSON body"
		return p
	}

	p.OK = true
	p.Body = json.RawMessage(leafBody)
	p.Shard = extractShard(leafBody)
	return p
}

// extractShard reads the "shard" label the leaf reports in its body so the
// partial can be tagged even though the root has no static shard->endpoint map.
// Best-effort: an absent field just leaves the tag empty.
func extractShard(body []byte) string {
	var probe struct {
		Shard string `json:"shard"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return ""
	}
	return probe.Shard
}

func summarize(partials []partial) meta {
	m := meta{Scattered: len(partials)}
	for _, p := range partials {
		if p.OK {
			m.Succeeded++
		} else {
			m.Failed++
		}
	}
	return m
}

// relayMerge POSTs the gather envelope to the merge service and copies merge's
// status code and body straight back to the client. Merge owns the business
// answer; the root only transports it.
func (s *server) relayMerge(w http.ResponseWriter, parent context.Context, env envelope) {
	payload, err := json.Marshal(env)
	if err != nil {
		http.Error(w, "failed to encode merge payload", http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(parent, s.cfg.scatterTO+2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.mergeURL, bytes.NewReader(payload))
	if err != nil {
		http.Error(w, "failed to build merge request", http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "merge request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	mergeBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		http.Error(w, "failed to read merge response", http.StatusBadGateway)
		return
	}

	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/json"
	}
	w.Header().Set("Content-Type", ct)
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(mergeBody)
}
