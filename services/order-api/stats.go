package main

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// latencyBucketsMs are the upper bounds (in milliseconds) of the native latency
// histogram, exactly as enumerated in CONTRACTS.md section 6. Values are cumulative:
// bucket[b] counts requests whose latency <= b ms.
var latencyBucketsMs = []int{5, 10, 25, 50, 100, 250}

// stats holds the in-memory native counters that back the :9000 /stats surface.
// A single mutex guards all fields so a scrape sees a self-consistent snapshot and
// so the histogram (map + count + sum) is updated atomically as a unit.
type stats struct {
	mu        sync.Mutex
	startTime time.Time

	ordersReceived int64
	ordersPlaced   int64
	ordersRejected int64
	ordersInFlight int64

	httpRequests map[int]int64 // status code -> total
	latCount     int64
	latSumMs     float64
	latBuckets   map[int]int64 // upper-bound ms -> cumulative count

	cacheHits   int64
	cacheMisses int64
}

func newStats() *stats {
	buckets := make(map[int]int64, len(latencyBucketsMs))
	for _, b := range latencyBucketsMs {
		buckets[b] = 0
	}
	return &stats{
		startTime:    time.Now(),
		httpRequests: make(map[int]int64),
		latBuckets:   buckets,
	}
}

func (s *stats) orderReceived() {
	s.mu.Lock()
	s.ordersReceived++
	s.ordersInFlight++
	s.mu.Unlock()
}

// orderDone decrements the in-flight gauge; called once per received order (deferred).
func (s *stats) orderDone() {
	s.mu.Lock()
	if s.ordersInFlight > 0 {
		s.ordersInFlight--
	}
	s.mu.Unlock()
}

func (s *stats) orderPlaced()   { s.mu.Lock(); s.ordersPlaced++; s.mu.Unlock() }
func (s *stats) orderRejected() { s.mu.Lock(); s.ordersRejected++; s.mu.Unlock() }
func (s *stats) cacheHit()      { s.mu.Lock(); s.cacheHits++; s.mu.Unlock() }
func (s *stats) cacheMiss()     { s.mu.Lock(); s.cacheMisses++; s.mu.Unlock() }

// recordHTTP records one completed API request against the status-code counter and
// the latency histogram.
func (s *stats) recordHTTP(status int, latencyMs float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.httpRequests[status]++
	s.latCount++
	s.latSumMs += latencyMs
	for _, b := range latencyBucketsMs {
		if latencyMs <= float64(b) {
			s.latBuckets[b]++
		}
	}
}

// --- native /stats JSON (CONTRACTS.md section 6: camelCase, latency in ms) ---

type statsSnapshot struct {
	UptimeSeconds float64        `json:"uptimeSeconds"`
	Orders        ordersSnapshot `json:"orders"`
	HTTP          httpSnapshot   `json:"http"`
	Cache         cacheSnapshot  `json:"cache"`
}

type ordersSnapshot struct {
	ReceivedTotal int64 `json:"receivedTotal"`
	PlacedTotal   int64 `json:"placedTotal"`
	RejectedTotal int64 `json:"rejectedTotal"`
	InFlight      int64 `json:"inFlight"`
}

type httpSnapshot struct {
	RequestsTotal    map[string]int64 `json:"requestsTotal"`
	RequestLatencyMs latencySnapshot  `json:"requestLatencyMs"`
}

type latencySnapshot struct {
	Count   int64            `json:"count"`
	Sum     float64          `json:"sum"`
	Buckets map[string]int64 `json:"buckets"`
}

type cacheSnapshot struct {
	HitsTotal   int64 `json:"hitsTotal"`
	MissesTotal int64 `json:"missesTotal"`
}

func (s *stats) snapshot() statsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	requests := make(map[string]int64, len(s.httpRequests))
	for code, n := range s.httpRequests {
		requests[strconv.Itoa(code)] = n
	}
	buckets := make(map[string]int64, len(s.latBuckets))
	for b, n := range s.latBuckets {
		buckets[strconv.Itoa(b)] = n
	}

	return statsSnapshot{
		UptimeSeconds: time.Since(s.startTime).Seconds(),
		Orders: ordersSnapshot{
			ReceivedTotal: s.ordersReceived,
			PlacedTotal:   s.ordersPlaced,
			RejectedTotal: s.ordersRejected,
			InFlight:      s.ordersInFlight,
		},
		HTTP: httpSnapshot{
			RequestsTotal: requests,
			RequestLatencyMs: latencySnapshot{
				Count:   s.latCount,
				Sum:     s.latSumMs,
				Buckets: buckets,
			},
		},
		Cache: cacheSnapshot{HitsTotal: s.cacheHits, MissesTotal: s.cacheMisses},
	}
}

func (s *server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.stats.snapshot())
}
