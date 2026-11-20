#!/usr/bin/env bash
# Load testing script for cost-stream HTTP REST endpoints using k6, oha, or curl.

set -euo pipefail

TARGET_URL="${HTTP_URL:-http://localhost:8080/v1/telemetry/batch}"

echo "Starting HTTP REST load test against ${TARGET_URL}..."

PAYLOAD='{"idempotency_key":"","records":[{"event_id":"http-load-1","tenant_id":"tenant-load","cluster_id":"c1","namespace":"default","workload_name":"w1","cost_center":"finops","cpu_cores_used":1.5,"memory_bytes_used":536870912,"estimated_hourly_cost_usd":0.15}]}'

if command -v k6 >/dev/null 2>&1; then
    k6 run - <<EOF
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 20,
  duration: '10s',
};

export default function () {
  const res = http.post('${TARGET_URL}', '${PAYLOAD}', {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'status is 200': (r) => r.status === 200 });
}
EOF
elif command -v oha >/dev/null 2>&1; then
    oha -z 10s -c 20 -m POST -H "Content-Type: application/json" -d "${PAYLOAD}" "${TARGET_URL}"
else
    echo "Neither k6 nor oha found. Testing with single curl call..."
    curl -s -X POST -H "Content-Type: application/json" -d "${PAYLOAD}" "${TARGET_URL}"
    echo ""
fi
