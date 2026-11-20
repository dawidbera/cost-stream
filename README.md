# cost-stream

Cloud Cost & Telemetry Aggregation Engine written in idiomatic Go.

`cost-stream` is a high-throughput FinOps microservice designed to ingest, buffer, and aggregate high-frequency container utilization and cloud cost telemetry from Kubernetes edge agents and CI/CD pipelines.

---

## Architecture Overview

The system strictly adheres to **Clean / Hexagonal Architecture (Ports & Adapters)**. The domain logic and use cases remain decoupled from transport frameworks and persistence databases.

```mermaid
flowchart TD
    subgraph Clients["Clients & Edge Agents"]
        Agent["K8s DaemonSet / Edge Agent (gRPC Stream)"]
        WebClient["FinOps Web Dashboard / CI (REST / HTTP)"]
    end

    subgraph Service["cost-stream Microservice"]
        subgraph Ingress["Transport Adapters (Driving)"]
            GRPC["gRPC Server (Protobuf v3)"]
            Gateway["gRPC-Gateway (Reverse Proxy HTTP -> gRPC)"]
        end

        subgraph Core["Domain & Application Core"]
            ServiceLayer["Telemetry Service (Use Cases)"]
            IngestBuffer["Ring Buffer / Ingestion Channel"]
            WorkerPool["Batch Worker Pool (Goroutines)"]
        end

        subgraph Egress["Infrastructure Adapters (Driven)"]
            Deduplicator["Redis Idempotency & Cache Adapter"]
            Repo["PostgreSQL Adapter (sqlc + pgxpool)"]
            Telemetry["OpenTelemetry + Prometheus Metrics"]
        end
    end

    subgraph Data["Storage & Cache"]
        PG[("PostgreSQL 16 (Timescale / Partitioned)")]
        RedisDB[("Redis 7 (Deduplication & Fast Cache)")]
    end

    Agent -->|"Bi-directional gRPC Stream"| GRPC
    WebClient -->|"JSON / HTTPS"| Gateway
    Gateway -->|"In-process Loopback"| GRPC
    GRPC --> ServiceLayer
    ServiceLayer -->|"Non-blocking Push"| IngestBuffer
    IngestBuffer --> WorkerPool
    WorkerPool -->|"Check Key"| Deduplicator
    Deduplicator --> RedisDB
    WorkerPool -->|"Bulk Copy / Multi-Row Insert"| Repo
    Repo --> PG
    ServiceLayer -.-> Telemetry
```

### Ingestion Sequence & Lifecycle

```mermaid
sequenceDiagram
    autonumber
    actor Client as Edge Agent / Client
    participant GW as HTTP Gateway / gRPC
    participant Service as Ingestion Service
    participant Cache as Redis Idempotency
    participant Pool as Worker Pool Buffer
    participant Worker as Background Workers
    participant PG as PostgreSQL (COPY FROM)

    Client->>GW: POST /v1/telemetry/batch (or StreamTelemetry)
    GW->>Service: IngestBatch(records, idempotencyKey)
    alt Idempotency Key Present
        Service->>Cache: SetIfNotExist(idempotencyKey, TTL)
        alt Duplicate Key Detected
            Cache-->>Service: false
            Service-->>GW: ErrDuplicateBatch (HTTP 409 / gRPC AlreadyExists)
            GW-->>Client: Error: duplicate batch
        end
    end
    Service->>Pool: SubmitBatch(validRecords)
    alt Buffer Saturated
        Pool-->>Service: ErrBufferSaturated
        Service-->>GW: ResourceExhausted (HTTP 503)
    else Buffer Accepted
        Pool-->>Service: Accepted
        Service-->>GW: Success Response
        GW-->>Client: BatchIngestResponse (AcceptedCount)
    end
    Note over Pool,Worker: Dual-trigger: MaxBatchSize (500) OR FlushInterval (200ms)
    Worker->>PG: CopyTelemetryRecords (PostgreSQL COPY Protocol)
    PG-->>Worker: Batch committed
```

---

## Directory Structure

```text
cost-stream/
├── cmd/
│   └── server/
│       └── main.go                    # Application entrypoint & dependency wiring
├── api/
│   ├── proto/
│   │   └── coststream/
│   │       └── v1/
│   │           ├── telemetry.proto    # Protobuf v3 service contract
│   │           ├── telemetry.pb.go    # Generated Go Protobuf structs
│   │           ├── telemetry_grpc.pb.go # Generated gRPC client/server interfaces
│   │           └── telemetry.pb.gw.go # Generated reverse proxy HTTP handlers
│   └── openapi/                       # Generated OpenAPI v2 Swagger specs
├── internal/
│   ├── core/
│   │   ├── domain/                    # Zero-dependency business entities & errors
│   │   │   ├── telemetry.go
│   │   │   ├── errors.go
│   │   │   └── telemetry_test.go
│   │   ├── ports/                     # Driving & driven interface boundaries
│   │   │   ├── input.go               # IngestionUseCase & QueryUseCase
│   │   │   └── output.go              # TelemetryRepository & IdempotencyCache
│   │   └── services/                  # Application use case implementations
│   │       ├── ingestion_service.go
│   │       ├── query_service.go
│   │       └── services_test.go
│   ├── adapters/
│   │   ├── in/
│   │   │   ├── grpc/                  # gRPC server handlers & interceptors
│   │   │   │   ├── handler.go
│   │   │   │   ├── interceptors.go
│   │   │   │   └── handler_test.go
│   │   │   └── http/                  # HTTP reverse proxy & operational endpoints
│   │   │       ├── gateway.go
│   │   │       ├── health.go
│   │   │       └── gateway_test.go
│   │   └── out/
│   │       ├── postgres/              # PostgreSQL adapter with sqlc & pgxpool
│   │       │   ├── db/                # sqlc generated queries & models
│   │       │   ├── migrations/        # SQL schema migrations
│   │       │   ├── queries.sql        # Raw SQL queries
│   │       │   ├── repository.go      # Adapter implementing ports.TelemetryRepository
│   │       │   └── repository_test.go
│   │       └── redis/                 # Redis idempotency adapter
│   │           ├── client.go
│   │           └── client_test.go
│   └── platform/
│       ├── config/                    # Environment variable configuration
│       │   └── config.go
│       ├── logger/                    # log/slog structured JSON logging
│       │   └── logger.go
│       ├── telemetry/                 # Prometheus metrics & OpenTelemetry
│       │   └── telemetry.go
│       └── workerpool/                # Concurrent micro-batching worker pool
│           ├── pool.go
│           └── pool_test.go
├── tests/
│   ├── integration/                   # E2E & testcontainers PostgreSQL tests
│   │   ├── e2e_test.go
│   │   └── repository_test.go
│   └── benchmark_test.go              # Concurrency and ingestion benchmarks
├── scripts/
│   ├── loadtest_grpc.sh               # gRPC load generation script
│   └── loadtest_http.sh               # HTTP REST load generation script
├── Dockerfile                         # Multi-stage distroless production image
├── docker-compose.yml                 # Local development stack (PostgreSQL + Redis)
├── buf.yaml                           # Buf linting and module configuration
├── buf.gen.yaml                       # Buf code generation template
├── sqlc.yaml                          # sqlc query compiler configuration
├── .golangci.yml                      # Strict static code analysis configuration
└── Makefile                           # Build automation targets
```

---

## API Specifications

### Protocol Buffers & gRPC (`coststream.v1.TelemetryService`)

| RPC Method | Input Type | Output Type | Description |
|:---|:---|:---|:---|
| `IngestBatch` | `BatchIngestRequest` | `BatchIngestResponse` | Unary high-throughput ingestion with idempotency deduplication |
| `StreamTelemetry` | `stream TelemetryRecord` | `StreamSummaryResponse` | Client-streaming ingestion for real-time edge agents |
| `QueryCostSummary` | `QueryCostRequest` | `QueryCostResponse` | Aggregated cost and compute metrics across time intervals |

### HTTP REST Transcoding (via `grpc-gateway`)

| Method | Endpoint | Description |
|:---|:---|:---|
| `POST` | `/v1/telemetry/batch` | Ingest a batch of telemetry records in JSON format |
| `GET` | `/v1/costs/summary` | Query cost metrics filtered by `tenant_id`, `cost_center`, `from_time`, and `to_time` |
| `GET` | `/healthz` | Liveness health probe (returns `{"status":"UP"}`) |
| `GET` | `/readyz` | Readiness probe verifying PostgreSQL and Redis connections |
| `GET` | `/metrics` | Prometheus scraper endpoint |
| `GET` | `/swagger/telemetry.swagger.json` | OpenAPI v2 specification document |

---

## Concurrency & High-Throughput Batching

1. **Non-Blocking Ingestion Channel:** Incoming requests push records into a bounded channel (default capacity `50,000`). If capacity is exceeded, `ErrBufferSaturated` is returned immediately to signal backpressure (`HTTP 503 / gRPC ResourceExhausted`).
2. **Dedicated Worker Goroutines:** A configurable worker pool (default `runtime.NumCPU() * 2`) consumes from the channel without lock contention.
3. **Dual-Trigger Flushing:**
   - **Threshold trigger:** Flushes immediately when `MAX_BATCH_SIZE` (default 500 records) is accumulated.
   - **Ticker trigger:** Flushes every `FLUSH_INTERVAL` (default 200ms) to ensure low latency under light traffic.
4. **PostgreSQL COPY Protocol:** Uses PostgreSQL `COPY FROM` via `pgxpool` for bulk writes, bypassing single-row `INSERT` overhead.
5. **Clean Graceful Shutdown:** Stops ingress listeners, closes the channel, drains buffered records to PostgreSQL with a 10-second deadline, and closes database connection pools.

---

## Configuration Reference

All settings are configured via environment variables:

| Variable | Default Value | Description |
|:---|:---|:---|
| `GRPC_PORT` | `50051` | gRPC listening port |
| `HTTP_PORT` | `8080` | HTTP REST gateway & observability port |
| `LOG_LEVEL` | `INFO` | Structured logging verbosity (`DEBUG`, `INFO`, `WARN`, `ERROR`) |
| `SHUTDOWN_TIMEOUT` | `10s` | Maximum time allowed to drain queues on termination |
| `DATABASE_URL` | `postgres://coststream:coststream_password@localhost:5432/coststream?sslmode=disable` | PostgreSQL connection string |
| `DATABASE_MAX_CONNS` | `25` | PostgreSQL maximum connection pool size |
| `REDIS_ADDR` | `localhost:6379` | Redis network address |
| `IDEMPOTENCY_TTL` | `24h` | Cache retention for batch idempotency keys |
| `WORKER_COUNT` | `NumCPU * 2` | Number of background batching workers |
| `QUEUE_CAPACITY` | `50000` | Ingestion buffer channel capacity |
| `MAX_BATCH_SIZE` | `500` | Micro-batch size threshold |
| `FLUSH_INTERVAL` | `200ms` | Maximum interval between batch writes |

---

## Quickstart & Local Development

### 1. Start Infrastructure Dependencies

```bash
docker-compose up -d
```

### 2. Run Database Migrations & Code Generation

```bash
make generate
```

### 3. Build & Run Application

```bash
make build
./bin/server
```

### 4. Run Test Suites & Benchmarks

```bash
# Run unit & concurrency race detector tests
make test-race

# Run benchmarks
go test -bench=. -benchmem ./tests

# Run linter
make lint
```

---

## Performance Benchmarks

Measured on an Intel Core i7 machine (Linux 6.8):

| Benchmark | Operations | Latency | Memory Allocation | Allocs / Op |
|:---|:---:|:---:|:---:|:---:|
| `BenchmarkWorkerPool` | 10,823,308 | 134.6 ns/op | 0 B/op | 0 allocs/op |
| `BenchmarkIngestBatch` (100 records) | 26,815 | 49.8 µs/op | 32.9 KB/op | 4 allocs/op |

> [!NOTE]
> The worker pool handles over 7.4 million submissions per second with zero runtime heap allocations during channel enqueueing.

---

## Technical Lessons Learned (Gotchas)

1. **PostgreSQL COPY FROM Protocol vs Multi-Row INSERT:**
   Standard multi-row parameterized `INSERT INTO ... VALUES (...), (...)` requires dynamic query generation and parameter limits (PostgreSQL has a 65,535 parameter limit). Using `sqlc` with `:copyfrom` generates a native `pgx.CopyFromSource` implementation, achieving maximum multi-row write throughput while eliminating SQL parsing overhead.

2. **In-Process Loopback for gRPC-Gateway:**
   Instead of configuring the `grpc-gateway` to establish a network loopback TCP connection to `127.0.0.1:50051`, `RegisterTelemetryServiceHandlerServer` was employed. This executes gRPC handlers in-process, saving CPU cycles, eliminating TCP socket exhaustion, and halving request latency.

3. **Protobuf Mutex Copying Warning (`copylocks`):**
   Generated Protobuf structs contain internal state with synchronization primitives (`protoimpl.MessageState`). Passing Protobuf messages by value to functions like `json.Marshal(req)` triggers `copylocks` linter warnings. Always pass pointers (`json.Marshal(&req)`) to preserve memory safety.
