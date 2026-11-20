// Package grpc provides the gRPC transport adapter implementation for TelemetryService.
package grpc

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	coststreamv1 "github.com/dawidbera/cost-stream/api/proto/coststream/v1"
	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/ports"
)

var _ coststreamv1.TelemetryServiceServer = (*Handler)(nil)

// Handler implements the gRPC TelemetryServiceServer interface.
type Handler struct {
	coststreamv1.UnimplementedTelemetryServiceServer
	ingestionUseCase ports.IngestionUseCase
	queryUseCase     ports.QueryUseCase
}

// NewHandler constructs a new gRPC handler instance.
func NewHandler(ingestionUseCase ports.IngestionUseCase, queryUseCase ports.QueryUseCase) *Handler {
	return &Handler{
		ingestionUseCase: ingestionUseCase,
		queryUseCase:     queryUseCase,
	}
}

// IngestBatch handles unary batch ingestion requests.
func (h *Handler) IngestBatch(ctx context.Context, req *coststreamv1.BatchIngestRequest) (*coststreamv1.BatchIngestResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	records := make([]domain.TelemetryRecord, len(req.Records))
	for i, r := range req.Records {
		records[i] = protoRecordToDomain(r)
	}

	accepted, err := h.ingestionUseCase.IngestBatch(ctx, req.IdempotencyKey, records)
	if err != nil {
		if errors.Is(err, domain.ErrDuplicateBatch) {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		if errors.Is(err, domain.ErrBufferSaturated) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "batch ingestion error: %v", err)
	}

	rejected := int32(len(records) - accepted)
	statusStr := "SUCCESS"
	if rejected > 0 {
		statusStr = "PARTIAL"
	}

	return &coststreamv1.BatchIngestResponse{
		AcceptedCount: int32(accepted),
		RejectedCount: rejected,
		Status:        statusStr,
	}, nil
}

// StreamTelemetry handles client-streaming ingestion for high-frequency agents.
func (h *Handler) StreamTelemetry(stream coststreamv1.TelemetryService_StreamTelemetryServer) error {
	sessionStart := time.Now()
	var totalIngested int64

	ctx := stream.Context()

	for {
		protoRec, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			sessionEnd := time.Now()
			return stream.SendAndClose(&coststreamv1.StreamSummaryResponse{
				TotalIngested: totalIngested,
				SessionStart:  timestamppb.New(sessionStart),
				SessionEnd:    timestamppb.New(sessionEnd),
			})
		}
		if err != nil {
			return status.Errorf(codes.Unknown, "error reading telemetry stream: %v", err)
		}

		domainRec := protoRecordToDomain(protoRec)
		if err := h.ingestionUseCase.IngestRecord(ctx, domainRec); err != nil {
			if errors.Is(err, domain.ErrBufferSaturated) {
				return status.Error(codes.ResourceExhausted, "ingestion buffer saturated")
			}
			return status.Errorf(codes.InvalidArgument, "invalid telemetry record: %v", err)
		}

		totalIngested++
	}
}

// QueryCostSummary aggregates and returns cost statistics over a requested time range.
func (h *Handler) QueryCostSummary(ctx context.Context, req *coststreamv1.QueryCostRequest) (*coststreamv1.QueryCostResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "query request cannot be nil")
	}

	var fromTime, toTime time.Time
	if req.FromTime != nil {
		fromTime = req.FromTime.AsTime()
	}
	if req.ToTime != nil {
		toTime = req.ToTime.AsTime()
	}

	summary, err := h.queryUseCase.GetCostSummary(ctx, req.TenantId, req.CostCenter, fromTime, toTime)
	if err != nil {
		if errors.Is(err, domain.ErrTenantNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "query failed: %v", err)
	}

	return &coststreamv1.QueryCostResponse{
		TenantId:      summary.TenantID,
		CostCenter:    summary.CostCenter,
		TotalCostUsd:  summary.TotalCostUSD,
		TotalCpuHours: summary.TotalCPUHours,
		TotalGbHours:  summary.TotalGBHours,
		TotalSamples:  summary.TotalSamples,
	}, nil
}

// protoRecordToDomain converts a Protobuf TelemetryRecord into a core domain.TelemetryRecord.
func protoRecordToDomain(pb *coststreamv1.TelemetryRecord) domain.TelemetryRecord {
	if pb == nil {
		return domain.TelemetryRecord{}
	}

	var recordedAt time.Time
	if pb.Timestamp != nil {
		recordedAt = pb.Timestamp.AsTime()
	} else {
		recordedAt = time.Now()
	}

	return domain.TelemetryRecord{
		EventID:                pb.EventId,
		TenantID:               pb.TenantId,
		ClusterID:              pb.ClusterId,
		Namespace:              pb.Namespace,
		WorkloadName:           pb.WorkloadName,
		CostCenter:             pb.CostCenter,
		CPUCoresUsed:           pb.CpuCoresUsed,
		MemoryBytesUsed:        pb.MemoryBytesUsed,
		EstimatedHourlyCostUSD: pb.EstimatedHourlyCostUsd,
		Timestamp:              recordedAt,
		Labels:                 pb.Labels,
	}
}
