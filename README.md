# PulseMetrics - APM Platform

A production-grade Application Performance Monitoring (APM) platform built from scratch using Go, Kafka, ScyllaDB, and React.

**Status:** Phase 4 complete - production ready: horizontally scalable, self-observable, migratable

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
│   │   ├── query.go        #   gRPC MetricsService (read path)
│   │   ├── traces.go       #   span ingest + service edges
│   │   ├── tracequery.go   #   gRPC TraceService + topology
│   │   ├── logs.go         #   log ingest
│   │   ├── logquery.go     #   gRPC LogService + pattern detection
│   │   └── alerts.go       #   alert engine + shared state (LWT) + gRPC
│   ├── health/             # Shared /healthz + /readyz server
│   ├── buildinfo/          # Version stamped in via -ldflags
│   ├── config/             # Env config + startup validation
│   ├── obs/                # Self-metrics (Prometheus)
│   ├── logging/            # Log SDK: trace-correlated structured logging
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

### 6f. Logs, alerts and anomaly detection

The demo services write trace-correlated logs. Try:

```bash
curl "localhost:8080/api/v1/log-services"
curl "localhost:8080/api/v1/logs?service=payments&range=15m&levels=ERROR,WARN"

# Every log line the request produced, across all services:
curl "localhost:8080/api/v1/trace-logs?id=<trace_id>"

# Repeated patterns, variables masked, ranked by error correlation:
curl "localhost:8080/api/v1/log-patterns?service=payments&range=30m"
```

Create an alert rule and evaluate it immediately:

```bash
curl -X POST localhost:8080/api/v1/rules -H "Content-Type: application/json" -d '{
  "name":"Goroutine sayisi yuksek",
  "service":"demo-app",
  "metric":"process.runtime.goroutines",
  "condition":"max > 3",
  "durationSeconds":600,
  "severity":"WARNING"
}'

curl -X POST localhost:8080/api/v1/evaluate      # dont wait for the 30s tick
curl "localhost:8080/api/v1/alerts?range=24h"
```

Add `"webhookUrl":"https://..."` to get an HTTP POST when the rule fires and
again when it resolves. Re-evaluating while the state is unchanged produces
nothing - the engine tracks which rules are currently firing so a breach is
reported once, not every tick.

### 6g. Scale out and watch it stay correct

```bash
make topics        # Kafka topics -> 3 partitions (consumer parallelism ceiling)

# Two collectors, same consumer group
PULSE_INSTANCE_ID=collector-1 ./bin/collector -port 50051 -health :8082
PULSE_INSTANCE_ID=collector-2 ./bin/collector -port 50053 -health :8084

# Partitions really are split between them:
docker exec pulse-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --group pulse-collector

# Each collector's own metrics:
make metrics
curl -s localhost:8084/metrics | grep pulse_alert_transitions
```

Grafana (http://localhost:3000, admin/admin) now has a **PulseMetrics ->
Collector** dashboard with ingest rate, Kafka lag, error breakdown, ingest
p95, alert transitions and Go runtime metrics.

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

## Phase 3: Advanced Features (Weeks 11-18) - complete

- [x] Log aggregation & search (`internal/logging`, `pulse.logs`, `pulse.trace_logs`)
- [x] **Trace-log correlation** - logs carry `trace_id` automatically from context
- [x] Log pattern detection (variable masking + error correlation)
- [x] Alert rules engine with state tracking, webhooks and resolve notifications
- [x] Anomaly detection via `zscore` conditions (statistical baselines)
- [x] Service topology auto-discovery **at ingest time** (no more query-time sampling)
- [x] Dashboard: Logs and Alerts tabs, per-trace log view

### The three pillars, joined

Phase 1 gave metrics ("payments is slow"). Phase 2 gave traces ("*this* request
spent 312 ms in payments"). Phase 3 gives logs and, more importantly, **joins
them**: a log written inside a span automatically carries that span's `trace_id`,
so one id pulls the whole causal chain across every service:

```
gateway   INFO   checkout istegi alindi
payments  ERROR  kart reddedildi: yetersiz bakiye, tutar 187.28
orders    ERROR  odeme adimi basarisiz
gateway   ERROR  checkout basarisiz
```

In the dashboard, clicking a `trace_id` in the Logs tab opens that trace's
waterfall, and every trace shows its own logs underneath.

### Topology: from query-time sampling to ingest-time counting

Phase 2 computed the service graph by sampling recent traces and walking
parent-child links at query time. That was honest at demo scale and slow at any
other scale.

The fix was at the source. The SDK now carries the caller's service name in the
W3C `tracestate` header (the field the standard reserves for exactly this), so a
SERVER span arrives already knowing who called it, in `peer.service`. The
collector writes the edge during ingest, with **no extra read**.

Result on the demo workload: `GetTopology` went from sampling ~27 calls per edge
to counting all 180, and got faster doing it.

### Alert conditions

A rule condition is deliberately a tiny closed language - three parts:

```
<aggregation> <operator> <number>

  aggregation : avg | sum | min | max | last | count | p50 | p95 | p99 | zscore
  operator    : > | >= | < | <=

  "p95 > 500"      classic threshold
  "zscore > 3"     statistical anomaly
```

A full expression parser (parentheses, AND/OR, arithmetic) would be complexity
this project does not need yet, and evaluating user-supplied text would be a
security hole. Three parts covers the need and is trivial to validate - invalid
conditions are rejected when the rule is created, not silently never fired.

`zscore` is the anomaly detector: it compares the recent window against a
baseline 12x longer, excluding the recent window itself, and reports how many
standard deviations away it is. Use it for metrics where no fixed threshold
works - a service handling 200 req/s by day and 20 by night cannot have one.

---

## Phase 4: Production Readiness (Weeks 19-22) - complete

- [x] **Multi-instance collector deployment** - shared alert state via LWT
- [x] **Schema migration** - `metrics` finally has an hourly `time_bucket`
- [x] ScyllaDB replication & consistency configurable (`NetworkTopologyStrategy`, `LOCAL_QUORUM`)
- [x] **Self-observability** - Prometheus `/metrics` on every binary + provisioned Grafana dashboard
- [x] Cardinality guard on operation names (the `r.URL.Path` debt)
- [x] Environment-based config with startup validation
- [x] Containerization (`Dockerfile`, `docker-compose.apps.yml` with two collectors)
- [x] Operations runbook (`docs/OPERATIONS.md`)

### Running two collectors

The headline of Phase 4. Two collectors in the same consumer group split
Kafka partitions between them - neither processes the same message twice:

```bash
PULSE_INSTANCE_ID=collector-1 ./bin/collector -port 50051 -health :8082
PULSE_INSTANCE_ID=collector-2 ./bin/collector -port 50053 -health :8084
```

Ingest was already safe to scale this way. **Alerting was not.** In Phase 3
"which rules are currently firing" lived in each process's memory: two
collectors meant two memories, and every alert fired twice. Horizontal
scaling did not break the system in an obvious way - it did something more
insidious.

Phase 4 moves that state into `pulse.alert_state` and performs the
transition with a **lightweight transaction** (CQL's `IF` clause):

```sql
UPDATE alert_state SET firing = true, ... WHERE rule_id = ? IF firing = false
```

A plain `UPDATE` is last-write-wins, so both collectors would succeed and
both would notify. With `IF`, Scylla runs Paxos underneath and applies the
update *only* if the current value is still what we expected. Exactly one
collector wins; the loser gets `[applied]=false` and stays quiet.

No leader election needed - the transition itself provides the mutual
exclusion, and a leader would have been one more moving part to keep alive.

LWT is expensive (four round trips plus consensus), so it runs **only on an
actual state change**. Ordinary evaluation rounds do a cheap `SELECT` first
and never reach Paxos when nothing changed.

```bash
curl -s localhost:8082/metrics | grep pulse_alert_transitions
curl -s localhost:8084/metrics | grep pulse_alert_transitions
# same transition: result="won" on one, result="lost" on the other
```

**Partition count is the ceiling.** At most *partition count* consumers in a
group can do work at once; with one partition the second collector just sits
idle. The default is now 3 (`make topics` raises existing topics).

### The schema migration

Cassandra and Scylla cannot change a partition key. Columns can be added,
TTLs changed - but not the key that decides which node holds the data,
because that would mean redistributing everything. So this:

```
Phase 1-3: PRIMARY KEY ((service_name, metric_name), timestamp, instance_id)
Phase 4:   PRIMARY KEY ((service_name, metric_name, time_bucket), timestamp, instance_id)
```

is not an `ALTER`, it is a **move**. `cmd/pulse-migrate` does it, preserving
each row's remaining TTL (`TTL(value)` read, `USING TTL` written) - without
that, a 29-day-old measurement would get a fresh 30 days and the table would
keep growing when it should shrink.

`CREATE TABLE IF NOT EXISTS` silently does nothing to an existing table, so
the collector now **verifies the schema at startup** and refuses to run
against the old one. The alternative is worse: a collector that reports
healthy, keeps consuming, and drops every message it cannot write. Silent
data loss always costs more than a loud startup failure.

### Self-observability

The most annoying failure of a monitoring system is stopping quietly: the
graph flatlines and nobody can tell "no traffic" from "collector died".

PulseMetrics does not monitor itself - that would be circular. Whatever
reports the collector's death has to be a *different* system, and the
Prometheus already sitting in `docker-compose.yml` is exactly that. Every
binary exposes `/metrics` next to `/healthz` and `/readyz`, and Grafana
loads a dashboard from `config/dashboards/` - so it survives the container
being recreated.

The metric to watch is `pulse_collector_kafka_lag`. Computing it correctly
took a second attempt: kafka-go's `Reader.Lag()` returns `-1` for consumer
groups by design, so the real lag is derived from `ListOffsets` (where each
partition ends) minus `OffsetFetch` (where the group has committed).

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

Phases 1-4 are done. Paid off in Phase 4: the unbounded `metrics` partition,
the in-memory alert state, and the `r.URL.Path` cardinality risk.

What is still open, honestly:

- **Log search filters in Go, not in the database.** ScyllaDB has no full-text
  index; queries narrow by partition and time range first, then filter in
  memory. Correct and verifiable at this scale, wrong at large scale - that job
  belongs to a search index (OpenSearch, Quickwit, Loki).
- **No authentication anywhere.** The gRPC API, the dashboard and the alert
  rule endpoints are all open. Fine on localhost, not deployable to a shared
  network as-is. mTLS between components and an auth proxy in front of the
  dashboard is the smallest honest fix.
- **The dashboard is a single embedded HTML file** using React via CDN, no build
  step. Vite + TypeScript is worthwhile once it grows.
- **Span events are stored as a JSON string column.** Fine while events are rare;
  revisit if they become a primary query dimension.
- **A stale `alert_state` row can silence a rule.** If a collector sets
  `firing=true` and dies before notifying, the rule stays "firing" and the next
  real breach is not reported. A staleness check on `updated_ms` would close it.
- **The migration tool stops the world.** Correct at this size, wrong at
  production volume - `docs/OPERATIONS.md` describes the dual-write alternative
  that needs no downtime and no copying.
