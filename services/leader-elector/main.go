// Command leader-elector is the Go election sidecar for OrderForge's settlement
// pod. It runs a client-go Lease-based leader election against the kube-apiserver
// and exposes the result over localhost so the language-agnostic main app (Python
// settlement) can ask "may I act?" without ever talking to Kubernetes itself.
//
// Contract: CONTRACTS.md section 7.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

const (
	leaseName = "orderforge-settlement"
	httpAddr  = ":4040"

	// Election timings, per contract: LeaseDuration > RenewDeadline > RetryPeriod.
	leaseDuration = 15 * time.Second
	renewDeadline = 10 * time.Second
	retryPeriod   = 2 * time.Second
)

// leaderState is the in-memory view of leadership that the HTTP handlers read.
// All fields are updated only from leaderelection callbacks and read only from
// HTTP handlers, so atomics keep them race-free without a mutex.
type leaderState struct {
	identity string

	isLeader atomic.Bool
	// leader is the identity of the leader most recently observed by this
	// pod (may be another pod). Stored as string via atomic.Value.
	leader atomic.Value
	// leaseValidUntilUnix is the Unix-nanosecond instant until which this pod's
	// leadership is guaranteed while it holds the lease: the moment leadership
	// was (re)confirmed plus LeaseDuration. Zero when not leading.
	leaseValidUntilUnix atomic.Int64
}

func (s *leaderState) currentLeader() string {
	v, _ := s.leader.Load().(string)
	return v
}

// leaseValidUntil returns the RFC3339 instant until which leadership is valid,
// or "" when this pod is not the leader. Callers that observe an empty or past
// value MUST treat this pod as NOT the leader (fail-closed).
func (s *leaderState) leaseValidUntil() string {
	if !s.isLeader.Load() {
		return ""
	}
	unix := s.leaseValidUntilUnix.Load()
	if unix == 0 {
		return ""
	}
	return time.Unix(0, unix).UTC().Format(time.RFC3339)
}

func main() {
	klog.InitFlags(nil)

	// Identity MUST be unique per pod; the Downward API supplies POD_NAME.
	id := os.Getenv("POD_NAME")
	if id == "" {
		// Fallback keeps local/dev runs from silently sharing an identity.
		if h, err := os.Hostname(); err == nil {
			id = h
		}
	}
	ns := os.Getenv("POD_NAMESPACE")
	if id == "" || ns == "" {
		klog.Fatalf("POD_NAME (%q) and POD_NAMESPACE (%q) are both required", id, ns)
	}

	st := &leaderState{identity: id}
	st.leader.Store("")

	// In-cluster config authenticates to the apiserver with the pod's
	// ServiceAccount token + cluster CA. No cloud IAM (IRSA / Workload Identity) is involved in election.
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("in-cluster config: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("kubernetes client: %v", err)
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: ns},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}

	// SIGTERM/SIGINT -> cancel the election context. With ReleaseOnCancel the
	// lease is released immediately on a graceful pod delete, so a standby takes
	// over in well under LeaseDuration instead of waiting for it to expire.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := &http.Server{Addr: httpAddr, Handler: newMux(st)}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("http server: %v", err)
		}
	}()

	go func() {
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   leaseDuration,
			RenewDeadline:   renewDeadline,
			RetryPeriod:     retryPeriod,
			Name:            leaseName,
			// Coordinated stays false: classic Lease election, not KEP-4355 CLE.
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(ctx context.Context) {
					st.leaseValidUntilUnix.Store(time.Now().Add(leaseDuration).UnixNano())
					st.isLeader.Store(true)
					klog.Infof("BECAME LEADER: %s", id)
					// While we lead, keep leaseValidUntil moving forward. The
					// election loop renews the lease within RenewDeadline; if it
					// ever fails, OnStoppedLeading fires and clears leadership.
					refreshValidUntil(ctx, st)
				},
				OnStoppedLeading: func() {
					// Flip leadership off FIRST so any in-flight poll observes
					// isLeader=false before we even log the loss.
					st.isLeader.Store(false)
					st.leaseValidUntilUnix.Store(0)
					klog.Infof("LOST LEADERSHIP: %s", id)
				},
				OnNewLeader: func(identity string) {
					st.leader.Store(identity)
					klog.Infof("NEW LEADER OBSERVED: %s", identity)
				},
			},
		})
		// RunOrDie returns only when ctx is cancelled (shutdown). Trigger the
		// HTTP drain by cancelling via stop() and let main proceed to shutdown.
		klog.Info("leaderelection loop exited")
		stop()
	}()

	<-ctx.Done()
	klog.Info("shutdown signal received, draining")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		klog.Errorf("http shutdown: %v", err)
	}
	// Give RunOrDie a brief moment to release the lease (ReleaseOnCancel).
	time.Sleep(retryPeriod)
	klog.Info("exiting")
}

// refreshValidUntil advances leaseValidUntil once per RetryPeriod while this pod
// leads. It blocks until leadership ends (ctx cancelled by the election loop),
// which also keeps OnStartedLeading blocked as the callback contract requires.
func refreshValidUntil(ctx context.Context, st *leaderState) {
	t := time.NewTicker(retryPeriod)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if st.isLeader.Load() {
				st.leaseValidUntilUnix.Store(time.Now().Add(leaseDuration).UnixNano())
			}
		}
	}
}

func newMux(st *leaderState) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/leader", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"identity":        st.identity,
			"leader":          st.currentLeader(),
			"isLeader":        st.isLeader.Load(),
			"leaseValidUntil": st.leaseValidUntil(),
		})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
