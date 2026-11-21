#!/usr/bin/env bash
# Load testing script for cost-stream gRPC endpoints using ghz.
# Utilizes gRPC server reflection for automated schema resolution.

set -euo pipefail

TARGET_HOST="${GRPC_HOST:-localhost:50051}"
CALL_METHOD="coststream.v1.TelemetryService.IngestBatch"
TOTAL_REQUESTS="${TOTAL_REQUESTS:-10000}"
CONCURRENCY="${CONCURRENCY:-50}"

echo "Starting gRPC load test against ${TARGET_HOST} (${TOTAL_REQUESTS} requests, concurrency: ${CONCURRENCY})..."

if command -v ghz >/dev/null 2>&1; then
    ghz --insecure \
        --call="${CALL_METHOD}" \
        -c "${CONCURRENCY}" \
        -n "${TOTAL_REQUESTS}" \
        -d '{"idempotency_key":"","records":[{"event_id":"load-evt","tenant_id":"tenant-load","cluster_id":"c1","namespace":"default","workload_name":"w1","cost_center":"finops","cpu_cores_used":2.0,"memory_bytes_used":1073741824,"estimated_hourly_cost_usd":0.20}]}' \
        "${TARGET_HOST}"
else
    echo "ghz is not in PATH. Install via: go install github.com/bojand/ghz/cmd/ghz@latest"
    echo "Running built-in benchmark instead:"
    go test -bench=BenchmarkIngestBatch -benchmem ./tests
fi
