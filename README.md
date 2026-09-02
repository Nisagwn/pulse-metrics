# PulseMetrics - APM Platform

A production-grade Application Performance Monitoring (APM) platform built from scratch using Go, Kafka, ScyllaDB, and React.

**Status:** Phase 1 complete - metrics pipeline, query API, health checks, dashboard

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
│   └── dashboard-api/      # HTTP/JSON API + embedded React dashboard
│       ├── main.go
│       └── web/index.html
├── internal/
│   ├── agent/              # Agent implementation
│   │   └── agent.go
│   ├── collector/          # Collector implementation
│   │   ├── collector.go    #   Kafka -> ScyllaDB ingest
│   │   └── query.go        #   gRPC MetricsService (read path)
│   ├── health/             # Shared /healthz + /readyz server
│   ├── traces/             # Traces processing (TBD)
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

## Phase 2: Distributed Tracing (Weeks 5-10)

- [ ] OpenTelemetry trace SDK integration
- [ ] Trace context propagation (W3C standard)
- [ ] HTTP middleware for auto-instrumentation
- [ ] Trace table schema in ScyllaDB
- [ ] Trace reconstruction (parent-child spans)
- [ ] React UI: trace viewer, timeline, service dependency map

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

`GetTopology` arrives with Phase 2 (distributed tracing).

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

- [x] Week 1: Architecture & setup
- [x] Week 2: Health check endpoints
- [x] Week 3: Agent + basic metrics
- [x] Week 4: Collector + ScyllaDB storage + query API
- [x] Week 5: Dashboard MVP

Phase 1 is done. Phase 2 starts with the trace SDK: `proto/traces.proto`
already defines `Span`, `SpanKind` and `ServiceTopology`; the Go side is next.

Known Phase 2 groundwork:
- The dashboard is a single embedded HTML file using React via CDN (no build
  step). Migrating it to Vite + TypeScript is worthwhile once it grows.
- `metrics` partitions grow without bound over 30 days. Add a time bucket to
  the partition key before running this with real traffic volume.
