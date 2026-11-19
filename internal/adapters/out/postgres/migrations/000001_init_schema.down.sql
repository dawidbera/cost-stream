-- Revert schema initialization migration.

DROP INDEX IF EXISTS idx_telemetry_cluster_ns;
DROP INDEX IF EXISTS idx_telemetry_tenant_time;
DROP TABLE IF EXISTS telemetry_records;
