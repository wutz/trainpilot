/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package ui serves the read-only TrainPilot dashboard: a table of TrainTasks
// with phase, run/restart counters, worker placement and billed cost.
//
// It is deliberately read-only. Every action (create, stop, continue, re-run)
// goes through the Kubernetes API against the TrainTask resource, so the
// dashboard never becomes a second source of truth.
package ui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"sort"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	trainpilotv1alpha1 "github.com/wutz/trainpilot/api/v1alpha1"
)

//go:embed static
var static embed.FS

// Handler returns the dashboard mux.
func Handler(c client.Reader) http.Handler {
	mux := http.NewServeMux()

	content, err := fs.Sub(static, "static")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(content)))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		writeTasks(w, r, c)
	})
	return mux
}

// Server is a manager.Runnable that serves the dashboard on a plain HTTP
// listener; the ingress normally terminates TLS in front of it.
type Server struct {
	Addr   string
	Reader client.Reader
	Log    logr.Logger
}

// NeedLeaderElection reports false: every replica may serve the read-only view.
func (*Server) NeedLeaderElection() bool { return false }

// Start implements manager.Runnable.
func (s *Server) Start(ctx context.Context) error {
	srv := &http.Server{
		Addr:              s.Addr,
		Handler:           Handler(s.Reader),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	s.Log.Info("dashboard listening", "addr", s.Addr)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

type taskView struct {
	Name             string   `json:"name"`
	Namespace        string   `json:"namespace"`
	Phase            string   `json:"phase"`
	Message          string   `json:"message"`
	RunID            int32    `json:"runId"`
	Round            int32    `json:"round"`
	RestartCount     int32    `json:"restartCount"`
	MaxRestarts      int32    `json:"maxRestarts"`
	PreemptionCount  int32    `json:"preemptionCount"`
	MaxPreemptions   int32    `json:"maxPreemptions"`
	Workers          int32    `json:"workers"`
	ReadyWorkers     int      `json:"readyWorkers"`
	GPUsPerWorker    int32    `json:"gpusPerWorker"`
	GPUType          string   `json:"gpuType"`
	Image            string   `json:"image"`
	BilledSeconds    int64    `json:"billedSeconds"`
	AccumulatedCost  string   `json:"accumulatedCost"`
	PricePerWorker   string   `json:"pricePerWorkerHour"`
	Nodes            []string `json:"nodes"`
	ExcludedNodes    []string `json:"excludedNodes"`
	StartedAt        string   `json:"startedAt,omitempty"`
	CompletionTime   string   `json:"completionTime,omitempty"`
	CreatedAt        string   `json:"createdAt"`
	StartingStep     string   `json:"startingStep,omitempty"`
	LastSegmentEnd   string   `json:"lastSegmentEndReason,omitempty"`
}

func writeTasks(w http.ResponseWriter, r *http.Request, c client.Reader) {
	var list trainpilotv1alpha1.TrainTaskList
	if err := c.List(r.Context(), &list); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	views := make([]taskView, 0, len(list.Items))
	for i := range list.Items {
		t := &list.Items[i]
		nodes := []string{}
		ready := 0
		for _, wp := range t.Status.Workers {
			if wp.Node != "" {
				nodes = append(nodes, wp.Node)
			}
			if wp.Phase == "Running" {
				ready++
			}
		}
		v := taskView{
			Name:            t.Name,
			Namespace:       t.Namespace,
			Phase:           string(t.Status.Phase),
			Message:         t.Status.Message,
			RunID:           t.Spec.RunID,
			Round:           t.Status.Round,
			RestartCount:    t.Status.RestartCount,
			MaxRestarts:     t.Spec.EffectiveMaxRestarts(),
			PreemptionCount: t.Status.PreemptionCount,
			MaxPreemptions:  t.Spec.EffectiveMaxPreemptions(),
			Workers:         t.Spec.Workers,
			ReadyWorkers:    ready,
			GPUsPerWorker:   t.Spec.Resources.GPUsPerWorker,
			GPUType:         t.Spec.GPUType,
			Image:           t.Spec.Image,
			BilledSeconds:   t.Status.BilledSeconds,
			AccumulatedCost: t.Status.AccumulatedCost.String(),
			PricePerWorker:  t.Spec.PricePerWorkerHour.String(),
			Nodes:           nodes,
			ExcludedNodes:   t.Status.ExcludedNodes,
			CreatedAt:       t.CreationTimestamp.UTC().Format(time.RFC3339),
			StartingStep:    t.Status.StartingStep,
		}
		if t.Status.StartedAt != nil {
			v.StartedAt = t.Status.StartedAt.UTC().Format(time.RFC3339)
		}
		if t.Status.CompletionTime != nil {
			v.CompletionTime = t.Status.CompletionTime.UTC().Format(time.RFC3339)
		}
		if n := len(t.Status.Segments); n > 0 {
			v.LastSegmentEnd = t.Status.Segments[n-1].EndReason
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].CreatedAt > views[j].CreatedAt })

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(map[string]any{
		"generatedAt": time.Now().UTC().Format(time.RFC3339),
		"tasks":       views,
	})
}
