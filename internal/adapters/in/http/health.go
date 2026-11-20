// Package http provides HTTP server adapters including health probes and grpc-gateway reverse proxy.
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/ports"
)

// HealthHandler manages liveness and readiness HTTP probe endpoints.
type HealthHandler struct {
	repo  ports.TelemetryRepository
	cache ports.IdempotencyCache
}

// NewHealthHandler creates a new HealthHandler instance.
func NewHealthHandler(repo ports.TelemetryRepository, cache ports.IdempotencyCache) *HealthHandler {
	return &HealthHandler{
		repo:  repo,
		cache: cache,
	}
}

// LivenessCheck handles /healthz probe requests, confirming the process is alive.
func (h *HealthHandler) LivenessCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "UP",
	})
}

// ReadinessCheck handles /readyz probe requests, checking downstream database and cache availability.
func (h *HealthHandler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	status := map[string]string{
		"status": "UP",
	}

	var hasError bool

	if h.repo != nil {
		if err := h.repo.Ping(ctx); err != nil {
			status["postgres"] = err.Error()
			hasError = true
		} else {
			status["postgres"] = "UP"
		}
	}

	if h.cache != nil {
		if err := h.cache.Ping(ctx); err != nil {
			status["redis"] = err.Error()
			hasError = true
		} else {
			status["redis"] = "UP"
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if hasError {
		status["status"] = "DOWN"
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	_ = json.NewEncoder(w).Encode(status)
}
