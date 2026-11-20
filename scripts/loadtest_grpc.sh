#!/usr/bin/env bash
# Load testing script for cost-stream gRPC endpoints using ghz or in-process load simulation.

set -euo pipefail

TARGET_HOST="${GRPC_HOST:-localhost:50051}"
PROTO_PATH="api/proto/coststream/v1/telemetry.proto"
CALL_METHOD="coststream.v1.TelemetryService.IngestBatch"
TOTAL_REQUESTS="${TOTAL_REQUESTS:-10000}"
CONCURRENCY="${CONCURRENCY:-50}"

echo "Starting gRPC load test against ${TARGET_HOST}..."

if command -v ghz >/dev/null 2>&1; then
    ghz --insecure \
        --proto="${PROTO_PATH}" \
        --call="${CALL_METHOD}" \
        -c "${CONCURRENCY}" \
        -n "${TOTAL_REQUESTS}" \
        -d '{"idempotency_key":"","records":[{"event_id":"load-evt","tenant_id":"tenant-load","cluster_id":"c1","namespace":"default","workload_name":"w1","cost_center":"finops","cpu_cores_used":2.0,"memory_bytes_used":1073741824,"estimated_hourly_cost_usd":0.20}]}' \
        "${TARGET_HOST}"
else
    echo "ghz is not installed. You can install it via: go install github.com/bojand/ghz/cmd/ghz@latest"
    echo "Running built-in benchmark instead:"
    go test -bench=BenchmarkIngestBatch -benchmem ./tests
fi
