// Package ports defines input (driving) and output (driven) interface boundaries for the Clean Architecture.
package ports

import (
	"context"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
)

// TelemetryRepository defines the driven persistence port for writing and querying telemetry records.
type TelemetryRepository interface {
	// BulkInsertRecords performs high-throughput multi-row insertion of telemetry records into the database.
	BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error

	// AggregateCostSummary computes aggregate metrics for a given tenant, cost center, and time window.
	AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error)

	// Ping checks the health and responsiveness of the underlying storage backend.
	Ping(ctx context.Context) error
}

// IdempotencyCache defines the driven caching port used for batch deduplication and preventing duplicate ingestion.
type IdempotencyCache interface {
	// SetIfNotExist attempts to store the key with a specified TTL. Returns true if the key was newly set.
	SetIfNotExist(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Ping checks the health and responsiveness of the underlying cache backend.
	Ping(ctx context.Context) error
}
