// Package services implements application use cases connecting driving ports to domain logic and driven adapters.
package services

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/ports"
	"github.com/dawidbera/cost-stream/internal/platform/telemetry"
	"github.com/dawidbera/cost-stream/internal/platform/workerpool"
)

var _ ports.IngestionUseCase = (*IngestionService)(nil)

// IngestionService implements the ports.IngestionUseCase interface.
type IngestionService struct {
	pool             *workerpool.Pool
	idempotencyCache ports.IdempotencyCache
	logger           *slog.Logger
	idempotencyTTL   time.Duration
}

// NewIngestionService constructs a new IngestionService instance.
func NewIngestionService(
	pool *workerpool.Pool,
	idempotencyCache ports.IdempotencyCache,
	logger *slog.Logger,
	idempotencyTTL time.Duration,
) *IngestionService {
	if idempotencyTTL <= 0 {
		idempotencyTTL = 24 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &IngestionService{
		pool:             pool,
		idempotencyCache: idempotencyCache,
		logger:           logger,
		idempotencyTTL:   idempotencyTTL,
	}
}

// IngestRecord validates a single telemetry record and submits it to the concurrent worker pool.
func (s *IngestionService) IngestRecord(ctx context.Context, record domain.TelemetryRecord) error {
	if err := record.Validate(); err != nil {
		telemetry.RecordIngestion("rejected", record.TenantID, 1)
		return fmt.Errorf("%w: %v", domain.ErrInvalidRecord, err)
	}

	if err := s.pool.Submit(ctx, record); err != nil {
		telemetry.RecordIngestion("rejected", record.TenantID, 1)
		return err
	}

	telemetry.RecordIngestion("accepted", record.TenantID, 1)
	return nil
}

// IngestBatch processes a batch of records under an optional idempotency key.
// Returns the count of accepted records.
func (s *IngestionService) IngestBatch(ctx context.Context, idempotencyKey string, records []domain.TelemetryRecord) (int, error) {
	if idempotencyKey != "" && s.idempotencyCache != nil {
		ok, err := s.idempotencyCache.SetIfNotExist(ctx, idempotencyKey, s.idempotencyTTL)
		if err != nil {
			s.logger.Warn("Failed to check idempotency cache, proceeding with ingestion",
				"idempotency_key", idempotencyKey,
				"error", err,
			)
		} else if !ok {
			return 0, domain.ErrDuplicateBatch
		}
	}

	validRecords := make([]domain.TelemetryRecord, 0, len(records))
	rejectedCount := 0

	for _, rec := range records {
		if err := rec.Validate(); err != nil {
			rejectedCount++
			telemetry.RecordIngestion("rejected", rec.TenantID, 1)
			continue
		}
		validRecords = append(validRecords, rec)
	}

	acceptedCount, err := s.pool.SubmitBatch(ctx, validRecords)
	for _, rec := range validRecords[:acceptedCount] {
		telemetry.RecordIngestion("accepted", rec.TenantID, 1)
	}

	if acceptedCount < len(validRecords) {
		unaccepted := len(validRecords) - acceptedCount
		for _, rec := range validRecords[acceptedCount:] {
			telemetry.RecordIngestion("rejected", rec.TenantID, 1)
		}
		s.logger.Warn("Batch partially rejected due to buffer saturation",
			"submitted", len(records),
			"accepted", acceptedCount,
			"rejected", rejectedCount+unaccepted,
		)
	}

	return acceptedCount, err
}
