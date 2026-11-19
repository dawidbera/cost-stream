// Package services implements application use cases connecting driving ports to domain logic and driven adapters.
package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dawidbera/cost-stream/internal/core/domain"
	"github.com/dawidbera/cost-stream/internal/core/ports"
)

var _ ports.QueryUseCase = (*QueryService)(nil)

// QueryService implements the ports.QueryUseCase interface.
type QueryService struct {
	repo ports.TelemetryRepository
}

// NewQueryService constructs a new QueryService instance.
func NewQueryService(repo ports.TelemetryRepository) *QueryService {
	return &QueryService{
		repo: repo,
	}
}

// GetCostSummary retrieves aggregated cost metrics for a given tenant and cost center over a time window.
func (s *QueryService) GetCostSummary(ctx context.Context, tenantID, costCenter string, from, to time.Time) (*domain.CostSummary, error) {
	if tenantID == "" {
		return nil, domain.ErrTenantNotFound
	}
	if costCenter == "" {
		return nil, errors.New("cost_center is required")
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return nil, errors.New("to_time must not be before from_time")
	}

	summary, err := s.repo.AggregateCostSummary(ctx, tenantID, costCenter, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to aggregate cost summary: %w", err)
	}

	return summary, nil
}
