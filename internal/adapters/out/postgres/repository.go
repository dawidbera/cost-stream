// Package postgres provides PostgreSQL persistence adapter implementations using pgxpool and sqlc.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dawidbera/cost-stream/internal/adapters/out/postgres/db"
	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/ports"
)

var _ ports.TelemetryRepository = (*Repository)(nil)

// Repository implements ports.TelemetryRepository using PostgreSQL, pgxpool, and sqlc.
type Repository struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

// NewRepository creates a new PostgreSQL telemetry repository adapter instance.
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool:    pool,
		queries: db.New(pool),
	}
}

// BulkInsertRecords performs high-performance bulk insertion using PostgreSQL COPY protocol.
// If records is empty, it returns nil immediately without executing a database command.
func (r *Repository) BulkInsertRecords(ctx context.Context, records []domain.TelemetryRecord) error {
	if len(records) == 0 {
		return nil
	}

	params := make([]db.CopyTelemetryRecordsParams, 0, len(records))
	for _, rec := range records {
		labelsJSON, err := json.Marshal(rec.Labels)
		if err != nil {
			labelsJSON = []byte("{}")
		}

		params = append(params, db.CopyTelemetryRecordsParams{
			EventID:                rec.EventID,
			TenantID:               rec.TenantID,
			ClusterID:              rec.ClusterID,
			Namespace:              rec.Namespace,
			WorkloadName:           rec.WorkloadName,
			CostCenter:             rec.CostCenter,
			CpuCoresUsed:           rec.CPUCoresUsed,
			MemoryBytesUsed:        rec.MemoryBytesUsed,
			EstimatedHourlyCostUsd: rec.EstimatedHourlyCostUSD,
			RecordedAt: pgtype.Timestamptz{
				Time:  rec.Timestamp,
				Valid: !rec.Timestamp.IsZero(),
			},
			Labels: labelsJSON,
		})
	}

	_, err := r.queries.CopyTelemetryRecords(ctx, params)
	if err != nil {
		return fmt.Errorf("bulk copy telemetry records failed: %w", err)
	}

	return nil
}

// AggregateCostSummary computes aggregate financial and compute metrics for a tenant and cost center over a time window.
// If no records match, an initialized CostSummary with zero values is returned.
func (r *Repository) AggregateCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	arg := db.GetAggregatedCostSummaryParams{
		TenantID:   tenantID,
		CostCenter: costCenter,
		RecordedAt: pgtype.Timestamptz{
			Time:  from,
			Valid: true,
		},
		RecordedAt_2: pgtype.Timestamptz{
			Time:  to,
			Valid: true,
		},
	}

	row, err := r.queries.GetAggregatedCostSummary(ctx, arg)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return &domain.CostSummary{
				TenantID:      tenantID,
				CostCenter:    costCenter,
				TotalCostUSD:  0,
				TotalCPUHours: 0,
				TotalGBHours:  0,
				TotalSamples:  0,
			}, nil
		}
		return nil, fmt.Errorf("get aggregated cost summary failed: %w", err)
	}

	return &domain.CostSummary{
		TenantID:      row.TenantID,
		CostCenter:    row.CostCenter,
		TotalCostUSD:  row.TotalCostUsd,
		TotalCPUHours: row.TotalCpuHours,
		TotalGBHours:  row.TotalGbHours,
		TotalSamples:  row.TotalSamples,
	}, nil
}

// Ping checks if the PostgreSQL connection pool is alive and able to acquire a connection.
func (r *Repository) Ping(ctx context.Context) error {
	if r.pool == nil {
		return errors.New("postgres connection pool is nil")
	}
	return r.pool.Ping(ctx)
}
