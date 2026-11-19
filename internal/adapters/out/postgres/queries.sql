-- name: InsertTelemetryRecord :one
-- Inserts a single telemetry record and returns the generated database ID.
INSERT INTO telemetry_records (
    event_id, tenant_id, cluster_id, namespace, workload_name,
    cost_center, cpu_cores_used, memory_bytes_used, estimated_hourly_cost_usd,
    recorded_at, labels
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
) RETURNING id;

-- name: CopyTelemetryRecords :copyfrom
-- High-throughput bulk insertion using PostgreSQL COPY protocol.
INSERT INTO telemetry_records (
    event_id, tenant_id, cluster_id, namespace, workload_name,
    cost_center, cpu_cores_used, memory_bytes_used, estimated_hourly_cost_usd,
    recorded_at, labels
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);

-- name: GetAggregatedCostSummary :one
-- Computes aggregated cost and resource statistics for a given tenant and cost center over a time window.
SELECT 
    tenant_id,
    cost_center,
    COALESCE(SUM(estimated_hourly_cost_usd), 0.0)::DOUBLE PRECISION AS total_cost_usd,
    COALESCE(SUM(cpu_cores_used), 0.0)::DOUBLE PRECISION AS total_cpu_hours,
    COALESCE(SUM(memory_bytes_used) / 1073741824.0, 0.0)::DOUBLE PRECISION AS total_gb_hours,
    COUNT(*)::BIGINT AS total_samples
FROM telemetry_records
WHERE tenant_id = $1 
  AND cost_center = $2 
  AND recorded_at >= $3 
  AND recorded_at <= $4
GROUP BY tenant_id, cost_center;

-- name: GetRecordByEventID :one
-- Retrieves a single telemetry record by its unique event ID.
SELECT 
    id, event_id, tenant_id, cluster_id, namespace, workload_name,
    cost_center, cpu_cores_used, memory_bytes_used, estimated_hourly_cost_usd,
    recorded_at, labels, created_at
FROM telemetry_records
WHERE event_id = $1;
