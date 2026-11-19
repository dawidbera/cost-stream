package services_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/services"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

type mockIdempotencyCache struct {
	seenKeys map[string]bool
	failErr  error
}

func (m *mockIdempotencyCache) SetIfNotExist(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if m.failErr != nil {
		return false, m.failErr
	}
	if m.seenKeys[key] {
		return false, nil
	}
	m.seenKeys[key] = true
	return true, nil
}

func (m *mockIdempotencyCache) Ping(ctx context.Context) error {
	return nil
}

type mockRepository struct {
	records []domain.TelemetryRecord
	summary *domain.CostSummary
	err     error
}

func (m *mockRepository) BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error {
	if m.err != nil {
		return m.err
	}
	m.records = append(m.records, records...)
	return nil
}

func (m *mockRepository) AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.summary, nil
}

func (m *mockRepository) Ping(ctx context.Context) error {
	return nil
}

// TestIngestionService_IngestRecord tests validation and ingestion of single telemetry records.
func TestIngestionService_IngestRecord(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	poolCfg := workerpool.Config{
		WorkerCount:   1,
		QueueCapacity: 10,
		MaxBatchSize:  5,
		FlushInterval: time.Second,
		FlushTimeout:  time.Second,
	}
	pool := workerpool.New(poolCfg, func(ctx context.Context, batch []domain.TelemetryRecord) error {
		return nil
	}, discardLogger)
	pool.Start()
	defer func() {
		_ = pool.Stop(context.Background())
	}()

	svc := services.NewIngestionService(pool, nil, discardLogger, time.Hour)

	// Valid record
	err := svc.IngestRecord(context.Background(), domain.TelemetryRecord{
		EventID:   "evt-1",
		TenantID:  "tenant-1",
		ClusterID: "cluster-1",
	})
	if err != nil {
		t.Fatalf("expected valid record to succeed, got %v", err)
	}

	// Invalid record
	err = svc.IngestRecord(context.Background(), domain.TelemetryRecord{
		EventID: "", // missing
	})
	if !errors.Is(err, domain.ErrInvalidRecord) {
		t.Fatalf("expected ErrInvalidRecord, got %v", err)
	}
}

// TestIngestionService_Idempotency tests that duplicate batches with identical idempotency keys are rejected.
func TestIngestionService_Idempotency(t *testing.T) {
	discardLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cache := &mockIdempotencyCache{seenKeys: make(map[string]bool)}

	poolCfg := workerpool.Config{
		WorkerCount:   1,
		QueueCapacity: 10,
		MaxBatchSize:  5,
		FlushInterval: time.Second,
		FlushTimeout:  time.Second,
	}
	pool := workerpool.New(poolCfg, func(ctx context.Context, batch []domain.TelemetryRecord) error {
		return nil
	}, discardLogger)
	pool.Start()
	defer func() {
		_ = pool.Stop(context.Background())
	}()

	svc := services.NewIngestionService(pool, cache, discardLogger, time.Hour)

	records := []domain.TelemetryRecord{
		{EventID: "evt-1", TenantID: "t-1", ClusterID: "c-1"},
	}

	// First submission should succeed
	accepted, err := svc.IngestBatch(context.Background(), "idem-key-100", records)
	if err != nil || accepted != 1 {
		t.Fatalf("first batch expected 1 accepted, got count %d err %v", accepted, err)
	}

	// Second submission with identical key should return ErrDuplicateBatch
	_, err = svc.IngestBatch(context.Background(), "idem-key-100", records)
	if !errors.Is(err, domain.ErrDuplicateBatch) {
		t.Fatalf("expected ErrDuplicateBatch on duplicate key, got %v", err)
	}
}

// TestQueryService_GetCostSummary tests cost summary retrieval and validation.
func TestQueryService_GetCostSummary(t *testing.T) {
	expectedSummary := &domain.CostSummary{
		TenantID:      "tenant-1",
		CostCenter:    "finops",
		TotalCostUSD:  123.45,
		TotalCPUHours: 50.0,
		TotalGBHours:  200.0,
		TotalSamples:  10,
	}

	repo := &mockRepository{summary: expectedSummary}
	svc := services.NewQueryService(repo)

	// Valid query
	summary, err := svc.GetCostSummary(context.Background(), "tenant-1", "finops", time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatalf("expected successful query, got err %v", err)
	}
	if summary.TotalCostUSD != 123.45 {
		t.Fatalf("expected 123.45 total cost, got %f", summary.TotalCostUSD)
	}

	// Missing tenant ID
	_, err = svc.GetCostSummary(context.Background(), "", "finops", time.Now(), time.Now())
	if !errors.Is(err, domain.ErrTenantNotFound) {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}

	// Invalid time range
	_, err = svc.GetCostSummary(context.Background(), "tenant-1", "finops", time.Now(), time.Now().Add(-time.Hour))
	if err == nil {
		t.Fatalf("expected error when to_time is before from_time, got nil")
	}
}
