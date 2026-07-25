package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// logger appends structured JSONL to two files under LOG_DIR, matching the sidecar
// contract (CONTRACTS.md section 4): access.log (one line per API request) and
// app.log (business/lifecycle events). A Node log-shipper tails these files.
type logger struct {
	mu         sync.Mutex
	accessFile *os.File
	appFile    *os.File
}

// logLine is the fixed JSONL schema. Optional fields are omitted when zero so app
// events stay compact.
type logLine struct {
	TS        string  `json:"ts"`
	Level     string  `json:"level"`
	Logger    string  `json:"logger"`
	Method    string  `json:"method,omitempty"`
	Path      string  `json:"path,omitempty"`
	Status    int     `json:"status,omitempty"`
	LatencyMs float64 `json:"latency_ms,omitempty"`
	OrderID   string  `json:"order_id,omitempty"`
	Msg       string  `json:"msg"`
}

func newLogger(dir string) (*logger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	access, err := openAppend(filepath.Join(dir, "access.log"))
	if err != nil {
		return nil, err
	}
	app, err := openAppend(filepath.Join(dir, "app.log"))
	if err != nil {
		access.Close()
		return nil, err
	}
	return &logger{accessFile: access, appFile: app}, nil
}

func openAppend(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func (l *logger) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.accessFile != nil {
		l.accessFile.Close()
	}
	if l.appFile != nil {
		l.appFile.Close()
	}
}

// access writes one access.log line for a completed API request.
func (l *logger) access(status int, method, path string, latencyMs float64, orderID, msg string) {
	l.write(l.accessFile, logLine{
		TS:        now(),
		Level:     "info",
		Logger:    "access",
		Method:    method,
		Path:      path,
		Status:    status,
		LatencyMs: latencyMs,
		OrderID:   orderID,
		Msg:       msg,
	})
}

// app writes one app.log event line. Only the order_id key from fields is read;
// everything else is ignored, keeping the line schema stable.
func (l *logger) app(level, msg string, fields map[string]any) {
	line := logLine{TS: now(), Level: level, Logger: "app", Msg: msg}
	if fields != nil {
		if oid, ok := fields["order_id"].(string); ok {
			line.OrderID = oid
		}
	}
	// Mirror app events to stderr so `kubectl logs` shows lifecycle/errors even before
	// the shipper has drained the file.
	l.write2(l.appFile, os.Stderr, line)
}

func (l *logger) write(f *os.File, line logLine) {
	b, err := json.Marshal(line)
	if err != nil {
		return
	}
	b = append(b, '\n')
	l.mu.Lock()
	f.Write(b)
	l.mu.Unlock()
}

func (l *logger) write2(f, mirror *os.File, line logLine) {
	b, err := json.Marshal(line)
	if err != nil {
		return
	}
	b = append(b, '\n')
	l.mu.Lock()
	f.Write(b)
	mirror.Write(b)
	l.mu.Unlock()
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

// respRecorder captures the status code and lets a handler stamp the order_id so the
// access-log middleware can record both without the handler duplicating logging.
type respRecorder struct {
	http.ResponseWriter
	status  int
	orderID string
}

func (r *respRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// setOrderID stamps the order id on the response recorder if present.
func setOrderID(w http.ResponseWriter, id string) {
	if rec, ok := w.(*respRecorder); ok {
		rec.orderID = id
	}
}

// withAccessLog wraps an API handler: it times the request, records the status/latency
// into the native counters, and appends one access.log line.
func (s *server) withAccessLog(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &respRecorder{ResponseWriter: w, status: http.StatusOK}
		next(rec, r)
		latencyMs := float64(time.Since(start).Microseconds()) / 1000.0
		s.stats.recordHTTP(rec.status, latencyMs)
		s.log.access(rec.status, r.Method, r.URL.Path, latencyMs, rec.orderID, "request")
	}
}
