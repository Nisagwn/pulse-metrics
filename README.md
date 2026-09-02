# PulseMetrics - APM Platform

A production-grade Application Performance Monitoring (APM) platform built from scratch using Go, Kafka, ScyllaDB, and React.

**Status:** Phase 2 complete - metrics + distributed tracing, service map, dashboard

---

## Overview

PulseMetrics is a distributed systems monitoring platform that collects metrics, traces, and logs from microservices. It provides real-time dashboards, anomaly detection, and service topology visualization.

### Core Components

```
Service 1      Service 2      Service N
    ↓              ↓              ↓
  Agent          Agent          Agent  (collect metrics/traces/logs)
    ↓              ↓              ↓
    └──────────→ Kafka ←─────────┘  (buffer, partition)
                    ↓
              Collector (Go)  (consume, validate, aggregate)
                    ↓
              ScyllaDB  (time-series storage)
                    ↓
         React Dashboard  (visualize, query, alert)
```

---

## Tech Stack

| Component | Technology | Why |
|-----------|-----------|-----|
| Agent | Go + OpenTelemetry | Lightweight, high-performance |
| Collector | Go + gRPC | Scalable, type-safe |
| Storage | ScyllaDB | High-throughput time-series |
| Message Queue | Kafka | Decouples agents, high availability |
| Dashboard | React + TypeScript | Modern UX, real-time updates |
| Monitoring | Prometheus + Grafana | Observe the observers |

---

## Project Structure

```
pulse-metrics/
├── cmd/
│   ├── agent/              # Agent executable
│   │   └── main.go
│   ├── collector/          # Collector executable
│   │   └── main.go
│   ├── dashboard-api/      # HTTP/JSON API + embedded React dashboard
│   │   ├── main.go
│   │   ├── traces.go
│   │   └── web/index.html
│   └── demo/               # Four traced microservices + load generator
├── internal/
│   ├── agent/              # Agent implementation
│   │   └── agent.go
│   ├── collector/          # Collector implementation
│   │   ├── collector.go    #   Kafka -> ScyllaDB ingest
│   │   └── query.go        #   gRPC MetricsService (read path)
│   ├── health/             # Shared /healthz + /readyz server
│   ├── tracing/            # Trace SDK: spans, W3C context, HTTP instrumentation
│   │   ├── context.go      #   TraceID/SpanID, traceparent parse & format
│   │   ├── tracer.go       #   Tracer, Span, samplers
│   │   ├── exporter.go     #   Batching Kafka exporter
│   │   └── httptrace.go    #   Middleware (server) + Transport (client)
│   └── proto/              # Generated protobuf files (auto-generated)
├── proto/                  # Protocol Buffer definitions
│   ├── metrics.proto
│   ├── traces.proto
│   └── logs.proto
├── scripts/                # init-scylla.cql (schema reference)
├── test/                   # Integration tests (build tag: integration)
├── config/                 # Configuration files
│   ├── prometheus.yml
│   └── grafana-datasources.yml
├── docker-compose.yml      # Local dev environment
└── README.md
```

---

## Quick Start (Local Development)

### Prerequisites

- Docker & Docker Compose
- Go 1.21+
- Protocol Buffers compiler (protoc)

### 1. Start Infrastructure

```bash
# Clone repo
git clone https://github.com/nisah/pulse-metrics.git
cd pulse-metrics

# Start Kafka, ScyllaDB, Prometheus, Grafana
docker-compose up -d

# Verify services are healthy
docker-compose ps

# Expected output:
# STATUS: Up (healthy)
```

### 2. Generate Protocol Buffers

```bash
# Install protoc compiler (if needed)
# macOS: brew install protobuf
# Linux: apt-get install protobuf-compiler

# Generate Go code from proto files
protoc --go_out=internal/proto --go_opt=paths=source_relative \
       --go-grpc_out=internal/proto --go-grpc_opt=paths=source_relative \
       proto/*.proto
```

### 3. Build Agent

```bash
cd cmd/agent
go build -o ../../bin/agent .
cd ../..

# Verify
./bin/agent --help
```

### 4. Build Collector

```bash
cd cmd/collector
go build -o ../../bin/collector .
cd ../..

# Verify
./bin/collector --help
```

### 5. Run Collector

```bash
./bin/collector --kafka localhost:9092 --scylla localhost:9042 --debug
```

Expected output:
```
Starting metrics collector on gRPC port 50051
Collector started
Keyspace and tables ensured
```

### 6. Run Agent (in another terminal)

```bash
./bin/agent --service test-app --kafka localhost:9092 --debug --interval 5s
```

Expected output:
```
Starting APM agent for service=test-app instance=default
Metrics published: count=4 payload_size=123
```

### 6b. Run Dashboard API (third terminal)

```bash
./bin/dashboard-api --addr :8080 --collector localhost:50051
# then open http://localhost:8080
```

### 6c. Check Health

Every service exposes liveness and readiness endpoints:

| Service       | Health address        |
|---------------|-----------------------|
| agent         | `:8081`               |
| collector     | `:8082`               |
| dashboard-api | `:8080` (same port)   |

```bash
curl localhost:8082/readyz
# {"status":"ready","uptime":"36s","checks":{"scylladb":"ok"},"version":"week1"}
```

`/healthz` is liveness — it stays 200 even when a dependency is down, so a brief
ScyllaDB outage does not send every process into a restart loop. `/readyz` runs
the dependency checks and returns 503 when one fails.

### 6d. Query the API

```bash
curl "localhost:8080/api/v1/series"
curl "localhost:8080/api/v1/query?service=demo-app&metric=process.memory.heap.alloc&range=1h"
curl "localhost:8080/api/v1/query?service=demo-app&metric=process.memory.heap.alloc&range=1h&agg=p95"
```

Aggregations: `avg`, `sum`, `min`, `max`, `count`, `last`, `p50`, `p95`, `p99`.
Omit `agg` for raw points.

### 6e. See distributed tracing

```bash
./bin/demo --kafka localhost:9092 --rps 4
```

This starts four instrumented services in one process - `gateway` calls `orders`,
which calls `payments` and `inventory` in parallel - plus a load generator. Some
requests fail and some are slow on purpose.

```bash
curl "localhost:8080/api/v1/operations"
curl "localhost:8080/api/v1/traces?service=gateway&range=15m&limit=5"
curl "localhost:8080/api/v1/trace?id=<trace_id>"
curl "localhost:8080/api/v1/topology?range=1h"
```

Open http://localhost:8080 and switch to the **Trace'ler** tab for the waterfall
viewer, or **Servis haritasi** for the dependency graph.

#### Instrumenting your own service

```go
exporter := tracing.NewBatchExporter(tracing.BatchExporterConfig{
    KafkaBrokers: []string{"localhost:9092"},
    ServiceName:  "my-service",
})
tracer := tracing.NewTracer("my-service", exporter,
    tracing.WithSampler(tracing.NewRatioSampler(0.1)))
defer tracer.Shutdown(context.Background())

// Incoming requests: one SERVER span each, parent taken from traceparent.
http.ListenAndServe(":8080", tracer.Middleware(mux))

// Outgoing requests: one CLIENT span each, traceparent injected.
// Passing ctx is required - that is where the parent span lives.
client := tracer.Client()
req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
resp, err := client.Do(req)
```

### 7. Verify Data in ScyllaDB

```bash
# Connect to ScyllaDB
docker exec -it pulse-scylladb cqlsh

# Query metrics
> USE pulse;
> SELECT * FROM metrics LIMIT 10;
```

### 8. View in Grafana

- Open http://localhost:3000
- Login: admin / admin
- Add Prometheus datasource (http://prometheus:9090)
- Create dashboard to visualize metrics

---

## Development Workflow

### Making Changes to Protocol Buffers

1. Edit `proto/*.proto`
2. Regenerate Go code:
   ```bash
   make proto  # (TBD: add Makefile)
   ```
3. Rebuild binaries
4. Restart services

### Testing Locally

```bash
# Unit tests - no infrastructure needed
go test -race ./...

# End-to-end: agent -> Kafka -> collector -> ScyllaDB -> gRPC query
# Requires "docker compose up -d" first.
go test -tags integration -v -timeout 15m ./test/...
```

Integration tests create their own Kafka topic, consumer group and service name
per run, so they are safe to run while a collector is already running locally.

### Debugging

Agent & Collector support `--debug` flag for verbose logging:
```bash
./bin/agent --debug
./bin/collector --debug
```

---

## Phase 1 Deliverables (Weeks 1-4) — complete

- [x] Architecture design & documentation
- [x] Docker Compose development environment
- [x] Protocol Buffer schemas (metrics, traces, logs)
- [x] Go project structure
- [x] Agent: collects system metrics → Kafka
- [x] Collector: Kafka consumer → ScyllaDB
- [x] gRPC query service (`MetricsService`: Query, ListSeries, Health)
- [x] Dashboard API + simple React dashboard
- [x] Health check endpoints (`/healthz`, `/readyz` on every service)
- [x] Graceful shutdown on SIGINT/SIGTERM
- [x] Unit tests + end-to-end integration tests

---

## Phase 2: Distributed Tracing (Weeks 5-10) - complete

- [x] Trace SDK (`internal/tracing`) - spans, samplers, batching exporter
- [x] Trace context propagation (W3C `traceparent` / `tracestate`)
- [x] HTTP middleware + client transport for auto-instrumentation
- [x] Trace schema in ScyllaDB (`spans`, `trace_index`, `service_ops`)
- [x] Trace reconstruction (parent-child spans, single-partition read)
- [x] gRPC `TraceService`: QueryTraces, GetTrace, GetTopology, ListOperations
- [x] Dashboard: trace search, waterfall viewer, service dependency map
- [x] `cmd/demo`: four traced microservices with a load generator

### A note on "OpenTelemetry SDK integration"

The original plan said "integrate the OpenTelemetry SDK". This project already
had its own complete `traces.proto` (Span, SpanKind, SpanStatus, Event, Link,
ServiceTopology) modelled on OTLP. Pulling in the upstream Go SDK would have made
that schema dead code and added a large dependency tree.

Instead the SDK here is native, and interoperability is achieved where it actually
matters: **W3C Trace Context**. A service instrumented with OpenTelemetry and a
service instrumented with this SDK propagate the same `traceparent` header, so
they join the same trace. Swapping in the upstream SDK later stays a contained
change because the wire format is the standard one.

---

## Phase 3: Advanced Features (Weeks 11-18)

- [ ] Anomaly detection (statistical baselines)
- [ ] Alert rules engine
- [ ] Log aggregation & search
- [ ] Service topology auto-discovery
- [ ] Performance optimization (sampling, compression)

---

## Phase 4: Production Readiness (Weeks 19-22)

- [ ] Multi-instance collector deployment
- [ ] ScyllaDB replication & failover
- [ ] Comprehensive documentation
- [ ] PulseCity integration example
- [ ] Public GitHub release

---

## Configuration

### Environment Variables

```bash
# Agent
PULSE_SERVICE_NAME=my-service
PULSE_INSTANCE_ID=host-123
PULSE_KAFKA_BROKERS=localhost:9092
PULSE_COLLECT_INTERVAL=10s

# Collector
PULSE_SCYLLA_ADDR=localhost:9042
PULSE_KAFKA_BROKERS=localhost:9092
PULSE_GRPC_PORT=50051
```

### Config Files

- `config/prometheus.yml` — Prometheus scrape config
- `config/grafana-datasources.yml` — Grafana datasource setup

---

## API References (TBD)

### Agent SDK (Go)

```go
import "github.com/nisah/pulse-metrics/pkg/sdk"

agent := sdk.NewAgent("my-service")
agent.RecordMetric("request.latency", 150.5)
agent.RecordEvent("user.login", map[string]string{"user_id": "123"})
```

### Collector gRPC (implemented)

Served by the collector on `:50051`; `dashboard-api` is its client.

```protobuf
service MetricsService {
  rpc Query(MetricsQueryRequest) returns (MetricsQueryResponse);
  rpc ListSeries(ListSeriesRequest) returns (ListSeriesResponse);
  rpc Health(HealthRequest) returns (HealthResponse);
}
```

```protobuf
service TraceService {
  rpc QueryTraces(TraceQueryRequest) returns (TraceQueryResponse);
  rpc GetTrace(GetTraceRequest) returns (Trace);
  rpc GetTopology(TopologyRequest) returns (ServiceTopology);
  rpc ListOperations(ListOperationsRequest) returns (ListOperationsResponse);
}
```

---

## Monitoring PulseMetrics Itself

Monitor the collector and agent using Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'pulse-agent'
    static_configs:
      - targets: ['localhost:8080']  # Agent prometheus endpoint (TBD)
  
  - job_name: 'pulse-collector'
    static_configs:
      - targets: ['localhost:8081']  # Collector prometheus endpoint (TBD)
```

---

## Troubleshooting

### Agent can't connect to Kafka

```bash
# Check Kafka is running
docker-compose ps kafka

# Verify broker is listening
docker-compose logs kafka | grep "started"

# Test connectivity
docker-compose exec kafka kafka-broker-api-versions --bootstrap-servers localhost:9092
```

### Collector can't connect to ScyllaDB

```bash
# Check ScyllaDB is running
docker-compose ps scylladb

# Verify schema was created
docker exec -it pulse-scylladb cqlsh -e "DESCRIBE KEYSPACES;"
```

### Metrics not appearing in dashboard

1. Verify agent is running and sending to Kafka:
   ```bash
   docker-compose logs -f kafka  # Watch for messages
   ```

2. Verify collector is consuming:
   ```bash
   docker-compose logs -f collector  # Check for "Metrics stored"
   ```

3. Check ScyllaDB has data:
   ```bash
   docker exec -it pulse-scylladb cqlsh -e "SELECT COUNT(*) FROM pulse.metrics;"
   ```

---

## Contributing

Phases are divided by feature area. Each phase builds on the previous:

1. **Phase 1 (Complete):** Metrics collection
2. **Phase 2:** Distributed tracing
3. **Phase 3:** Analytics & alerting
4. **Phase 4:** Production hardening

See `APM_PROJECT_PLAN.md` for detailed roadmap.

---

## License

MIT (placeholder)

---

## Next Steps

Phases 1 and 2 are done. Known work carried into Phase 3:

- **`metrics` partitions are still unbounded.** `trace_index` uses an hourly
  `time_bucket` in its partition key; `pulse.metrics` does not. Add one before
  running this against real traffic volume.
- **Topology is computed at query time** from a bounded sample of traces
  (`GetTopology`, `sample_limit`). Honest at demo scale, too slow at production
  scale - the edges want a stream processor writing them ahead of time.
- **Operation names come from `r.URL.Path`.** A path like `/orders/12345` creates
  a distinct operation per id (cardinality explosion). Use the router route
  template instead once there is a router.
- **The dashboard is a single embedded HTML file** using React via CDN, no build
  step. Vite + TypeScript is worthwhile once it grows.
- **Span events are stored as a JSON string column.** Fine while events are rare;
  revisit if they become a primary query dimension.
