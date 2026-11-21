# VHS tape configuration for cost-stream demonstration
# Generates a terminal recording demonstrating compilation, startup, ingestion, idempotency, and query aggregation.

Output assets/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1100
Set Height 640
Set Padding 18
Set WindowBar Colorful
Set TypingSpeed 25ms
Set Theme "Catppuccin Mocha"

# 1. Compile static binary
Type "make build"
Sleep 500ms
Enter
Sleep 1.5s

# 2. Start cost-stream service in background
Type "HTTP_PORT=8085 ./bin/server &"
Sleep 500ms
Enter
Sleep 1.5s

# 3. Check health and readiness probe
Type "curl -s http://localhost:8085/readyz | jq ."
Sleep 500ms
Enter
Sleep 1.5s

# 4. Ingest telemetry micro-batch from sample JSON
Type "curl -s -X POST http://localhost:8085/v1/telemetry/batch -H 'Content-Type: application/json' -d @assets/sample_batch.json | jq ."
Sleep 500ms
Enter
Sleep 2s

# 5. Verify Redis idempotency (reject duplicate batch with 409 Conflict)
Type "curl -s -i -X POST http://localhost:8085/v1/telemetry/batch -H 'Content-Type: application/json' -d @assets/sample_batch.json | head -n 5"
Sleep 500ms
Enter
Sleep 2s

# 6. Query aggregated cost metrics over time window
Type "curl -s 'http://localhost:8085/v1/costs/summary?tenant_id=acme-corp&cost_center=payments&from_time=2026-01-01T00:00:00Z&to_time=2026-12-31T23:59:59Z' | jq ."
Sleep 500ms
Enter
Sleep 2.5s

# 7. Check Prometheus metrics endpoint
Type "curl -s http://localhost:8085/metrics | grep 'coststream_telemetry_ingested_total' | head -n 2"
Sleep 500ms
Enter
Sleep 2s

# 8. Graceful shutdown
Type "kill %1"
Sleep 500ms
Enter
Sleep 1s
