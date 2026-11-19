-- Schema initialization migration for telemetry records table.
-- Tracks resource consumption metrics and estimated hourly cost for FinOps telemetry.

CREATE TABLE IF NOT EXISTS telemetry_records (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    tenant_id VARCHAR(64) NOT NULL,
    cluster_id VARCHAR(64) NOT NULL,
    namespace VARCHAR(128) NOT NULL,
    workload_name VARCHAR(128) NOT NULL,
    cost_center VARCHAR(64) NOT NULL,
    cpu_cores_used DOUBLE PRECISION NOT NULL,
    memory_bytes_used BIGINT NOT NULL,
    estimated_hourly_cost_usd DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Compound index optimizing cost aggregation queries filtered by tenant, cost center, and time range.
CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_time 
    ON telemetry_records (tenant_id, cost_center, recorded_at);

-- Secondary index optimizing filtering by cluster and namespace.
CREATE INDEX IF NOT EXISTS idx_telemetry_cluster_ns 
    ON telemetry_records (cluster_id, namespace);
