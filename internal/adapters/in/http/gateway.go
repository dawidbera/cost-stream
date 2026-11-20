// Package http provides HTTP server adapters including health probes and grpc-gateway reverse proxy.
package http

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
)

// Server encapsulates the HTTP REST reverse proxy and operational endpoints.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer configures and returns an HTTP Server routing health probes, Prometheus metrics, and gRPC-Gateway REST endpoints.
func NewServer(
	ctx context.Context,
	addr string,
	grpcServer coststreamv1.TelemetryServiceServer,
	healthHandler *HealthHandler,
	swaggerFilePath string,
	logger *slog.Logger,
) (*Server, error) {
	if logger == nil {
		logger = slog.Default()
	}

	gwMux := runtime.NewServeMux()
	if err := coststreamv1.RegisterTelemetryServiceHandlerServer(ctx, gwMux, grpcServer); err != nil {
		return nil, fmt.Errorf("failed to register gRPC-gateway server: %w", err)
	}

	mux := http.NewServeMux()

	// Operational and observability endpoints
	if healthHandler != nil {
		mux.HandleFunc("/healthz", healthHandler.LivenessCheck)
		mux.HandleFunc("/readyz", healthHandler.ReadinessCheck)
	}
	mux.Handle("/metrics", promhttp.Handler())

	// OpenAPI swagger documentation endpoint
	if swaggerFilePath != "" {
		mux.HandleFunc("/swagger/telemetry.swagger.json", func(w http.ResponseWriter, r *http.Request) {
			data, err := os.ReadFile(swaggerFilePath)
			if err != nil {
				http.Error(w, "swagger spec not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(data)
		})
	}

	// Route all API requests through the gRPC-Gateway reverse proxy
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			gwMux.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"cost-stream","status":"running"}`))
			return
		}
		http.NotFound(w, r)
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return &Server{
		httpServer: srv,
		logger:     logger,
	}, nil
}

// ServeHTTP implements http.Handler delegating to the underlying HTTP server router.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.httpServer.Handler.ServeHTTP(w, r)
}

// Serve accepts incoming HTTP connections on the provided net.Listener.
func (s *Server) Serve(lis net.Listener) error {
	s.logger.Info("Starting HTTP REST gateway server on listener", "addr", lis.Addr().String())
	if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// Start begins listening for incoming HTTP connections in a background goroutine.
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP REST gateway server", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// Shutdown gracefully terminates the HTTP server within the provided context deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP REST gateway server...")
	return s.httpServer.Shutdown(ctx)
}
