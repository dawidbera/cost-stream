package postgres_test

import (
	"context"
	"testing"

	postgresAdapter "github.com/dawidbera/cost-stream/internal/adapters/out/postgres"
	"github.com/dawidbera/cost-stream/internal/core/domain"
)

// TestRepository_EmptyRecords tests that passing an empty slice of records returns nil without executing DB queries.
func TestRepository_EmptyRecords(t *testing.T) {
	repo := postgresAdapter.NewRepository(nil)

	err := repo.BulkInsertRecords(context.Background(), []domain.TelemetryRecord{})
	if err != nil {
		t.Fatalf("expected nil error for empty bulk insert, got %v", err)
	}

	err = repo.Ping(context.Background())
	if err == nil {
		t.Fatalf("expected error for nil pool ping, got nil")
	}
}
