package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	grpcAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/grpc"
	httpAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/http"
	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/services"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

type inMemoryRepository struct {
	mu      sync.Mutex
	records []domain.TelemetryRecord
}

func (r *inMemoryRepository) BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, records...)
	return nil
}

func (r *inMemoryRepository) AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var totalCost, totalCPU, totalMemory float64
	var count int64

	for _, rec := range r.records {
		if rec.TenantID == tenantID && rec.CostCenter == costCenter {
			totalCost += rec.EstimatedHourlyCostUSD
			totalCPU += rec.CPUCoresUsed
			totalMemory += float64(rec.MemoryBytesUsed)
			count++
		}
	}

	return &domain.CostSummary{
		TenantID:      tenantID,
		CostCenter:    costCenter,
		TotalCostUSD:  totalCost,
		TotalCPUHours: totalCPU,
		TotalGBHours:  totalMemory / 1073741824.0,
		TotalSamples:  count,
	}, nil
}

func (r *inMemoryRepository) Ping(ctx context.Context) error {
	return nil
}

type inMemoryCache struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (c *inMemoryCache) SetIfNotExist(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys[key] {
		return false, nil
	}
	c.keys[key] = true
	return true, nil
}

func (c *inMemoryCache) Ping(ctx context.Context) error {
	return nil
}

// TestEndToEnd_SystemFlow tests the complete lifecycle: gRPC streaming, unary batching, HTTP REST gateway, and aggregation.
func TestEndToEnd_SystemFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	repo := &inMemoryRepository{}
	cache := &inMemoryCache{keys: make(map[string]bool)}

	var flushCounter atomic.Int64
	poolCfg := workerpool.Config{
		WorkerCount:   4,
		QueueCapacity: 5000,
		MaxBatchSize:  10,
		FlushInterval: 50 * time.Millisecond,
		FlushTimeout:  2 * time.Second,
	}

	pool := workerpool.New(poolCfg, func(flushCtx context.Context, batch []domain.TelemetryRecord) error {
		flushCounter.Add(int64(len(batch)))
		return repo.BulkInsertRecords(flushCtx, batch)
	}, logger)
	pool.Start()
	defer func() {
		_ = pool.Stop(context.Background())
	}()

	ingestionSvc := services.NewIngestionService(pool, cache, logger, time.Hour)
	querySvc := services.NewQueryService(repo)
	grpcHandler := grpcAdapter.NewHandler(ingestionSvc, querySvc)

	// 1. Start gRPC Server on dynamic port
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen gRPC: %v", err)
	}
	defer grpcLis.Close()

	grpcServer := grpc.NewServer()
	coststreamv1.RegisterTelemetryServiceServer(grpcServer, grpcHandler)

	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.GracefulStop()

	// 2. Start HTTP Gateway on dynamic port
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen HTTP: %v", err)
	}
	defer httpLis.Close()

	healthHandler := httpAdapter.NewHealthHandler(repo, cache)
	httpGw, err := httpAdapter.NewServer(ctx, httpLis.Addr().String(), grpcHandler, healthHandler, "", logger)
	if err != nil {
		t.Fatalf("failed to create http gateway: %v", err)
	}

	go func() {
		_ = httpGw.Serve(httpLis)
	}()
	defer func() {
		_ = httpGw.Shutdown(ctx)
	}()

	// 3. Connect gRPC Client
	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	grpcClient := coststreamv1.NewTelemetryServiceClient(conn)

	// 4. Test Unary IngestBatch via gRPC
	batchResp, err := grpcClient.IngestBatch(ctx, &coststreamv1.BatchIngestRequest{
		IdempotencyKey: "e2e-batch-1",
		Records: []*coststreamv1.TelemetryRecord{
			{
				EventId:                "e2e-rec-1",
				TenantId:               "tenant-e2e",
				ClusterId:              "k8s-us",
				Namespace:              "prod",
				WorkloadName:           "order-api",
				CostCenter:             "finops-e2e",
				CpuCoresUsed:           2.0,
				MemoryBytesUsed:        1024 * 1024 * 1024,
				EstimatedHourlyCostUsd: 0.20,
				Timestamp:              timestamppb.Now(),
			},
		},
	})
	if err != nil {
		t.Fatalf("IngestBatch gRPC failed: %v", err)
	}
	if batchResp.AcceptedCount != 1 {
		t.Fatalf("expected 1 accepted record, got %d", batchResp.AcceptedCount)
	}

	// 5. Test Client-Streaming StreamTelemetry via gRPC
	stream, err := grpcClient.StreamTelemetry(ctx)
	if err != nil {
		t.Fatalf("StreamTelemetry gRPC open failed: %v", err)
	}

	for i := 2; i <= 4; i++ {
		sendErr := stream.Send(&coststreamv1.TelemetryRecord{
			EventId:                fmt.Sprintf("e2e-rec-%d", i),
			TenantId:               "tenant-e2e",
			ClusterId:              "k8s-us",
			Namespace:              "prod",
			WorkloadName:           "order-api",
			CostCenter:             "finops-e2e",
			CpuCoresUsed:           1.0,
			MemoryBytesUsed:        512 * 1024 * 1024,
			EstimatedHourlyCostUsd: 0.10,
			Timestamp:              timestamppb.Now(),
		})
		if sendErr != nil {
			t.Fatalf("stream send failed: %v", sendErr)
		}
	}

	streamSummary, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("stream CloseAndRecv failed: %v", err)
	}
	if streamSummary.TotalIngested != 3 {
		t.Fatalf("expected 3 streamed records, got %d", streamSummary.TotalIngested)
	}

	// 6. Test HTTP REST Batch Ingestion via reverse proxy
	httpClient := &http.Client{Timeout: 5 * time.Second}
	httpURL := fmt.Sprintf("http://%s", httpLis.Addr().String())

	httpBatchReq := coststreamv1.BatchIngestRequest{
		IdempotencyKey: "e2e-batch-http",
		Records: []*coststreamv1.TelemetryRecord{
			{
				EventId:                "e2e-rec-5",
				TenantId:               "tenant-e2e",
				ClusterId:              "k8s-us",
				Namespace:              "prod",
				WorkloadName:           "order-api",
				CostCenter:             "finops-e2e",
				CpuCoresUsed:           1.0,
				MemoryBytesUsed:        512 * 1024 * 1024,
				EstimatedHourlyCostUsd: 0.10,
			},
		},
	}
	bodyBytes, _ := json.Marshal(&httpBatchReq)
	postResp, err := httpClient.Post(httpURL+"/v1/telemetry/batch", "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("HTTP POST batch failed: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 from REST batch, got %d", postResp.StatusCode)
	}

	// 7. Verify Health and Metrics HTTP endpoints
	healthResp, err := httpClient.Get(httpURL + "/healthz")
	if err != nil || healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /healthz, got %v", healthResp.StatusCode)
	}
	healthResp.Body.Close()

	readyResp, err := httpClient.Get(httpURL + "/readyz")
	if err != nil || readyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /readyz, got %v", readyResp.StatusCode)
	}
	readyResp.Body.Close()

	metricsResp, err := httpClient.Get(httpURL + "/metrics")
	if err != nil || metricsResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /metrics, got %v", metricsResp.StatusCode)
	}
	metricsResp.Body.Close()

	// Wait for worker pool flush interval
	time.Sleep(200 * time.Millisecond)

	// 8. Test Query via gRPC
	queryResp, err := grpcClient.QueryCostSummary(ctx, &coststreamv1.QueryCostRequest{
		TenantId:   "tenant-e2e",
		CostCenter: "finops-e2e",
	})
	if err != nil {
		t.Fatalf("QueryCostSummary failed: %v", err)
	}

	// Total expected: 1 (from batch) + 3 (from stream) + 1 (from http) = 5 samples
	if queryResp.TotalSamples != 5 {
		t.Fatalf("expected 5 aggregated samples, got %d", queryResp.TotalSamples)
	}
	// Total cost: 0.20 + 3*0.10 + 0.10 = 0.60 USD
	expectedCost := 0.60
	if fmt.Sprintf("%.2f", queryResp.TotalCostUsd) != fmt.Sprintf("%.2f", expectedCost) {
		t.Fatalf("expected total cost %.2f, got %.2f", expectedCost, queryResp.TotalCostUsd)
	}
}
