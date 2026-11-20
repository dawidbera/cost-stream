package grpc_test

import (
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	grpcAdapter "github.com/dawidbera/cost-stream/internal/adapters/in/grpc"
	"github.com/dawidbera/cost-stream/internal/core/domain"
)

type mockIngestionUseCase struct {
	ingestedRecords []domain.TelemetryRecord
	failWithErr     error
	acceptedCount   int
}

func (m *mockIngestionUseCase) IngestRecord(ctx context.Context, record domain.TelemetryRecord) error {
	if m.failWithErr != nil {
		return m.failWithErr
	}
	m.ingestedRecords = append(m.ingestedRecords, record)
	return nil
}

func (m *mockIngestionUseCase) IngestBatch(ctx context.Context, idempotencyKey string, records []domain.TelemetryRecord) (int, error) {
	if m.failWithErr != nil {
		return 0, m.failWithErr
	}
	m.ingestedRecords = append(m.ingestedRecords, records...)
	if m.acceptedCount > 0 {
		return m.acceptedCount, nil
	}
	return len(records), nil
}

type mockQueryUseCase struct {
	summary *domain.CostSummary
	err     error
}

func (m *mockQueryUseCase) GetCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.summary, nil
}

type mockStreamTelemetryServer struct {
	grpc.ServerStream
	records      []*coststreamv1.TelemetryRecord
	currentIndex int
	closedWith   *coststreamv1.StreamSummaryResponse
}

func (m *mockStreamTelemetryServer) Context() context.Context {
	return context.Background()
}

func (m *mockStreamTelemetryServer) Recv() (*coststreamv1.TelemetryRecord, error) {
	if m.currentIndex >= len(m.records) {
		return nil, io.EOF
	}
	rec := m.records[m.currentIndex]
	m.currentIndex++
	return rec, nil
}

func (m *mockStreamTelemetryServer) SendAndClose(resp *coststreamv1.StreamSummaryResponse) error {
	m.closedWith = resp
	return nil
}

func (m *mockStreamTelemetryServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockStreamTelemetryServer) SendHeader(metadata.MD) error { return nil }
func (m *mockStreamTelemetryServer) SetTrailer(metadata.MD)       {}

// TestHandler_IngestBatch tests gRPC batch ingestion handling and status code mapping.
func TestHandler_IngestBatch(t *testing.T) {
	mockIngestion := &mockIngestionUseCase{}
	mockQuery := &mockQueryUseCase{}
	handler := grpcAdapter.NewHandler(mockIngestion, mockQuery)

	ctx := context.Background()

	// Nil request test
	_, err := handler.IngestBatch(ctx, nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}

	// Successful batch request
	req := &coststreamv1.BatchIngestRequest{
		IdempotencyKey: "test-idem-key",
		Records: []*coststreamv1.TelemetryRecord{
			{
				EventId:                "evt-1",
				TenantId:               "tenant-1",
				ClusterId:              "k8s-prod",
				Namespace:              "default",
				WorkloadName:           "api-gateway",
				CostCenter:             "finops",
				CpuCoresUsed:           2.0,
				MemoryBytesUsed:        1024 * 1024 * 1024,
				EstimatedHourlyCostUsd: 0.20,
				Timestamp:              timestamppb.Now(),
			},
		},
	}

	resp, err := handler.IngestBatch(ctx, req)
	if err != nil {
		t.Fatalf("expected successful batch ingestion, got %v", err)
	}
	if resp.AcceptedCount != 1 || resp.Status != "SUCCESS" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// Idempotency conflict mapping
	mockIngestion.failWithErr = domain.ErrDuplicateBatch
	_, err = handler.IngestBatch(ctx, req)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("expected AlreadyExists, got code %v, err %v", status.Code(err), err)
	}

	// Buffer saturation mapping
	mockIngestion.failWithErr = domain.ErrBufferSaturated
	_, err = handler.IngestBatch(ctx, req)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got code %v, err %v", status.Code(err), err)
	}
}

// TestHandler_StreamTelemetry tests client streaming ingestion through StreamTelemetry.
func TestHandler_StreamTelemetry(t *testing.T) {
	mockIngestion := &mockIngestionUseCase{}
	mockQuery := &mockQueryUseCase{}
	handler := grpcAdapter.NewHandler(mockIngestion, mockQuery)

	stream := &mockStreamTelemetryServer{
		records: []*coststreamv1.TelemetryRecord{
			{
				EventId:   "stream-1",
				TenantId:  "t-1",
				ClusterId: "c-1",
			},
			{
				EventId:   "stream-2",
				TenantId:  "t-1",
				ClusterId: "c-1",
			},
		},
	}

	err := handler.StreamTelemetry(stream)
	if err != nil {
		t.Fatalf("expected stream to succeed, got %v", err)
	}
	if stream.closedWith == nil || stream.closedWith.TotalIngested != 2 {
		t.Fatalf("expected 2 total ingested in summary, got %+v", stream.closedWith)
	}
}

// TestHandler_QueryCostSummary tests cost querying through gRPC.
func TestHandler_QueryCostSummary(t *testing.T) {
	mockIngestion := &mockIngestionUseCase{}
	mockQuery := &mockQueryUseCase{
		summary: &domain.CostSummary{
			TenantID:      "tenant-test",
			CostCenter:    "infra",
			TotalCostUSD:  99.99,
			TotalCPUHours: 10.0,
			TotalGBHours:  50.0,
			TotalSamples:  100,
		},
	}
	handler := grpcAdapter.NewHandler(mockIngestion, mockQuery)

	ctx := context.Background()

	// Nil request
	_, err := handler.QueryCostSummary(ctx, nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}

	// Valid request
	resp, err := handler.QueryCostSummary(ctx, &coststreamv1.QueryCostRequest{
		TenantId:   "tenant-test",
		CostCenter: "infra",
		FromTime:   timestamppb.New(time.Now().Add(-24 * time.Hour)),
		ToTime:     timestamppb.Now(),
	})
	if err != nil {
		t.Fatalf("expected query to succeed, got %v", err)
	}
	if resp.TotalCostUsd != 99.99 || resp.TotalSamples != 100 {
		t.Fatalf("unexpected query response: %+v", resp)
	}

	// Tenant not found mapping
	mockQuery.err = domain.ErrTenantNotFound
	_, err = handler.QueryCostSummary(ctx, &coststreamv1.QueryCostRequest{
		TenantId:   "nonexistent",
		CostCenter: "infra",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got %v", err)
	}
}
