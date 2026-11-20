package tests_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	grpcAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/grpc"
	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/services"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

type noopRepo struct{}

func (n *noopRepo) BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error {
	return nil
}

func (n *noopRepo) AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	return &domain.CostSummary{}, nil
}

func (n *noopRepo) Ping(ctx context.Context) error {
	return nil
}

// BenchmarkWorkerPool measures throughput of enqueueing records into the worker pool.
func BenchmarkWorkerPool(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := workerpool.Config{
		WorkerCount:   8,
		QueueCapacity: 500000,
		MaxBatchSize:  500,
		FlushInterval: 100 * time.Millisecond,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, func(ctx context.Context, batch []domain.TelemetryRecord) error {
		return nil
	}, logger)
	pool.Start()
	defer func() {
		_ = pool.Stop(context.Background())
	}()

	rec := domain.TelemetryRecord{
		EventID:                "bench-evt",
		TenantID:               "bench-tenant",
		ClusterID:              "bench-k8s",
		Namespace:              "bench-ns",
		WorkloadName:           "bench-workload",
		CostCenter:             "finops",
		CPUCoresUsed:           2.5,
		MemoryBytesUsed:        1024 * 1024 * 1024,
		EstimatedHourlyCostUSD: 0.15,
		Timestamp:              time.Now(),
	}

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.Submit(ctx, rec)
		}
	})
}

// BenchmarkIngestBatch measures throughput of processing protobuf BatchIngestRequest through the gRPC handler.
func BenchmarkIngestBatch(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := workerpool.Config{
		WorkerCount:   8,
		QueueCapacity: 500000,
		MaxBatchSize:  500,
		FlushInterval: 100 * time.Millisecond,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(cfg, func(ctx context.Context, batch []domain.TelemetryRecord) error {
		return nil
	}, logger)
	pool.Start()
	defer func() {
		_ = pool.Stop(context.Background())
	}()

	ingestionSvc := services.NewIngestionService(pool, nil, logger, time.Hour)
	querySvc := services.NewQueryService(&noopRepo{})
	handler := grpcAdapter.NewHandler(ingestionSvc, querySvc)

	const batchSize = 100
	records := make([]*coststreamv1.TelemetryRecord, batchSize)
	for i := 0; i < batchSize; i++ {
		records[i] = &coststreamv1.TelemetryRecord{
			EventId:                fmt.Sprintf("bench-evt-%d", i),
			TenantId:               "bench-tenant",
			ClusterId:              "bench-cluster",
			Namespace:              "bench-ns",
			WorkloadName:           "bench-workload",
			CostCenter:             "bench-cc",
			CpuCoresUsed:           1.0,
			MemoryBytesUsed:        512 * 1024 * 1024,
			EstimatedHourlyCostUsd: 0.05,
		}
	}

	req := &coststreamv1.BatchIngestRequest{
		IdempotencyKey: "",
		Records:        records,
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := handler.IngestBatch(ctx, req)
		if err != nil {
			b.Fatalf("IngestBatch failed: %v", err)
		}
	}
}
