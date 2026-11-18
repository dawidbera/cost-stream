// Package ports defines input (driving) and output (driven) interface boundaries for the Clean Architecture.
package ports

import (
	"context"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
)

// IngestionUseCase defines the primary driving use case interface for ingesting single or batched telemetry records.
type IngestionUseCase interface {
	// IngestRecord accepts a single telemetry record and queues it for asynchronous batch processing.
	IngestRecord(ctx context.Context, record domain.TelemetryRecord) error

	// IngestBatch processes a batch of records under an optional idempotency key.
	// Returns the count of accepted records, or an error if validation fails, the batch is duplicate, or the buffer is saturated.
	IngestBatch(ctx context.Context, idempotencyKey string, records []domain.TelemetryRecord) (int, error)
}

// QueryUseCase defines the driving use case interface for querying aggregated telemetry and cost data.
type QueryUseCase interface {
	// GetCostSummary computes aggregated expenditure and resource usage for a tenant within a time interval.
	GetCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error)
}
