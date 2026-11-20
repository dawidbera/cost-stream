package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	grpcAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/grpc"
	httpAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/http"
	"github.com/dawidbera/cost-stream/internal/core/domain"
)

type mockRepo struct {
	pingErr error
	summary *domain.CostSummary
}

func (m *mockRepo) BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error {
	return nil
}

func (m *mockRepo) AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	if m.summary != nil {
		return m.summary, nil
	}
	return &domain.CostSummary{
		TenantID:   tenantID,
		CostCenter: costCenter,
	}, nil
}

func (m *mockRepo) Ping(ctx context.Context) error {
	return m.pingErr
}

type mockCache struct {
	pingErr error
}

func (m *mockCache) SetIfNotExist(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return true, nil
}

func (m *mockCache) Ping(ctx context.Context) error {
	return m.pingErr
}

type mockIngestionUseCase struct{}

func (m *mockIngestionUseCase) IngestRecord(ctx context.Context, record domain.TelemetryRecord) error {
	return nil
}

func (m *mockIngestionUseCase) IngestBatch(ctx context.Context, idempotencyKey string, records []domain.TelemetryRecord) (int, error) {
	return len(records), nil
}

type mockQueryUseCase struct{}

func (m *mockQueryUseCase) GetCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	return &domain.CostSummary{
		TenantID:      tenantID,
		CostCenter:    costCenter,
		TotalCostUSD:  42.50,
		TotalCPUHours: 12.0,
		TotalGBHours:  64.0,
		TotalSamples:  24,
	}, nil
}

// TestHealthHandler tests liveness and readiness probe responses.
func TestHealthHandler(t *testing.T) {
	repo := &mockRepo{}
	cache := &mockCache{}
	health := httpAdapter.NewHealthHandler(repo, cache)

	// Liveness check
	reqLiveness := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recLiveness := httptest.NewRecorder()
	health.LivenessCheck(recLiveness, reqLiveness)

	if recLiveness.Code != http.StatusOK {
		t.Fatalf("expected 200 for healthz, got %d", recLiveness.Code)
	}

	// Readiness check: healthy
	reqReadiness := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReadiness := httptest.NewRecorder()
	health.ReadinessCheck(recReadiness, reqReadiness)

	if recReadiness.Code != http.StatusOK {
		t.Fatalf("expected 200 for readyz when healthy, got %d", recReadiness.Code)
	}

	// Readiness check: unhealthy downstream
	repo.pingErr = errors.New("database connection refused")
	recUnhealthy := httptest.NewRecorder()
	health.ReadinessCheck(recUnhealthy, reqReadiness)

	if recUnhealthy.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for readyz when repo is down, got %d", recUnhealthy.Code)
	}
}

// TestHTTPGateway_Routing tests HTTP routes including health, metrics, root, and REST endpoints.
func TestHTTPGateway_Routing(t *testing.T) {
	ctx := context.Background()
	repo := &mockRepo{}
	cache := &mockCache{}
	health := httpAdapter.NewHealthHandler(repo, cache)
	grpcHandler := grpcAdapter.NewHandler(&mockIngestionUseCase{}, &mockQueryUseCase{})
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	gwServer, err := httpAdapter.NewServer(ctx, ":8080", grpcHandler, health, "", discardLogger)
	if err != nil {
		t.Fatalf("failed to create gateway server: %v", err)
	}

	// Test 1: /healthz
	reqHealth := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recHealth := httptest.NewRecorder()
	gwServer.ServeHTTP(recHealth, reqHealth)
	if recHealth.Code != http.StatusOK {
		t.Fatalf("expected 200 for /healthz, got %d", recHealth.Code)
	}

	// Test 2: /readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	gwServer.ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("expected 200 for /readyz, got %d", recReady.Code)
	}

	// Test 3: /metrics
	reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recMetrics := httptest.NewRecorder()
	gwServer.ServeHTTP(recMetrics, reqMetrics)
	if recMetrics.Code != http.StatusOK {
		t.Fatalf("expected 200 for /metrics, got %d", recMetrics.Code)
	}

	// Test 4: Root path /
	reqRoot := httptest.NewRequest(http.MethodGet, "/", nil)
	recRoot := httptest.NewRecorder()
	gwServer.ServeHTTP(recRoot, reqRoot)
	if recRoot.Code != http.StatusOK {
		t.Fatalf("expected 200 for /, got %d", recRoot.Code)
	}

	// Test 5: REST POST /v1/telemetry/batch
	batchReq := coststreamv1.BatchIngestRequest{
		IdempotencyKey: "test-idem-web",
		Records: []*coststreamv1.TelemetryRecord{
			{
				EventId:                "web-evt-1",
				TenantId:               "tenant-web",
				ClusterId:              "k8s-us-west",
				Namespace:              "analytics",
				WorkloadName:           "processor",
				CostCenter:             "data-eng",
				CpuCoresUsed:           4.0,
				MemoryBytesUsed:        2048 * 1024 * 1024,
				EstimatedHourlyCostUsd: 0.45,
			},
		},
	}
	body, err := json.Marshal(&batchReq)
	if err != nil {
		t.Fatalf("json marshal error: %v", err)
	}

	reqPost := httptest.NewRequest(http.MethodPost, "/v1/telemetry/batch", bytes.NewReader(body))
	reqPost.Header.Set("Content-Type", "application/json")
	recPost := httptest.NewRecorder()
	gwServer.ServeHTTP(recPost, reqPost)

	if recPost.Code != http.StatusOK {
		t.Fatalf("expected 200 for POST /v1/telemetry/batch, got %d: %s", recPost.Code, recPost.Body.String())
	}

	// Test 6: REST GET /v1/costs/summary
	reqGet := httptest.NewRequest(http.MethodGet, "/v1/costs/summary?tenant_id=tenant-web&cost_center=data-eng", nil)
	recGet := httptest.NewRecorder()
	gwServer.ServeHTTP(recGet, reqGet)

	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 for GET /v1/costs/summary, got %d: %s", recGet.Code, recGet.Body.String())
	}

	// Test 7: 404 for unknown path
	req404 := httptest.NewRequest(http.MethodGet, "/not-found-endpoint", nil)
	rec404 := httptest.NewRecorder()
	gwServer.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown path, got %d", rec404.Code)
	}
}
