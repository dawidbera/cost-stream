package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	postgresAdapter "github.com/dawidbera/cost-stream/internal/adapters/out/postgres"
	"github.com/dawidbera/cost-stream/internal/core/domain"
)

// TestPostgresRepository_Integration tests database persistence and aggregation against a real PostgreSQL container.
func TestPostgresRepository_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	pgContainer, err := tcpostgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		tcpostgres.WithDatabase("test_coststream"),
		tcpostgres.WithUsername("test_user"),
		tcpostgres.WithPassword("test_pass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Skipf("skipping test: unable to start postgres container (docker unavailable?): %v", err)
		return
	}
	defer func() {
		_ = pgContainer.Terminate(ctx)
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	// Execute migration
	migrationPath := filepath.Join("..", "..", "internal", "adapters", "out", "postgres", "migrations", "000001_init_schema.up.sql")
	migrationSQL, err := os.ReadFile(migrationPath)
	if err != nil {
		t.Fatalf("failed to read migration file: %v", err)
	}

	if _, execErr := pool.Exec(ctx, string(migrationSQL)); execErr != nil {
		t.Fatalf("failed to apply migration schema: %v", execErr)
	}

	repo := postgresAdapter.NewRepository(pool)

	// Verify Ping
	if pingErr := repo.Ping(ctx); pingErr != nil {
		t.Fatalf("expected ping to succeed, got %v", pingErr)
	}

	// Test BulkInsertRecords
	records := []domain.TelemetryRecord{
		{
			EventID:                "tc-evt-1",
			TenantID:               "tenant-tc",
			ClusterID:              "k8s-cluster-1",
			Namespace:              "finops",
			WorkloadName:           "aggregator",
			CostCenter:             "analytics",
			CPUCoresUsed:           3.5,
			MemoryBytesUsed:        4 * 1024 * 1024 * 1024,
			EstimatedHourlyCostUSD: 0.35,
			Timestamp:              time.Now().Add(-1 * time.Hour),
			Labels:                 map[string]string{"env": "test"},
		},
		{
			EventID:                "tc-evt-2",
			TenantID:               "tenant-tc",
			ClusterID:              "k8s-cluster-1",
			Namespace:              "finops",
			WorkloadName:           "aggregator",
			CostCenter:             "analytics",
			CPUCoresUsed:           1.5,
			MemoryBytesUsed:        2 * 1024 * 1024 * 1024,
			EstimatedHourlyCostUSD: 0.15,
			Timestamp:              time.Now().Add(-30 * time.Minute),
			Labels:                 map[string]string{"env": "test"},
		},
	}

	if insertErr := repo.BulkInsertRecords(ctx, records); insertErr != nil {
		t.Fatalf("BulkInsertRecords failed: %v", insertErr)
	}

	// Test AggregateCostSummary
	summary, err := repo.AggregateCostSummary(ctx, "tenant-tc", "analytics", time.Now().Add(-2*time.Hour), time.Now())
	if err != nil {
		t.Fatalf("AggregateCostSummary failed: %v", err)
	}

	if summary.TotalSamples != 2 {
		t.Fatalf("expected 2 samples, got %d", summary.TotalSamples)
	}
	if summary.TotalCostUSD != 0.50 {
		t.Fatalf("expected 0.50 total cost, got %f", summary.TotalCostUSD)
	}
	if summary.TotalCPUHours != 5.0 {
		t.Fatalf("expected 5.0 CPU hours, got %f", summary.TotalCPUHours)
	}
	if summary.TotalGBHours != 6.0 {
		t.Fatalf("expected 6.0 GB hours, got %f", summary.TotalGBHours)
	}

	// Test non-matching query returns zeroed summary
	emptySummary, err := repo.AggregateCostSummary(ctx, "nonexistent", "analytics", time.Now().Add(-2*time.Hour), time.Now())
	if err != nil {
		t.Fatalf("expected empty summary query to succeed without error, got %v", err)
	}
	if emptySummary.TotalSamples != 0 || emptySummary.TotalCostUSD != 0 {
		t.Fatalf("expected zeroed summary for nonexistent tenant, got %+v", emptySummary)
	}
}
