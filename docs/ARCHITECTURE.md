# PulseMetrics Architecture

## High-Level System Design

```
┌────────────────────────────────────────────────────────────────────┐
│                         Monitored Services                         │
├─────────────────────┬──────────────────────┬──────────────────────┤
│   Service A         │      Service B       │      Service N       │
│  (Go, Python, etc)  │   (Go, Python, etc)  │  (Go, Python, etc)   │
│        ↓            │          ↓           │          ↓           │
│ ┌──────────────┐   │ ┌──────────────┐    │ ┌──────────────┐     │
│ │ PulseAgent   │   │ │ PulseAgent   │    │ │ PulseAgent   │     │
│ │ - Metrics    │   │ │ - Metrics    │    │ │ - Metrics    │     │
│ │ - Traces     │   │ │ - Traces     │    │ │ - Traces     │     │
│ │ - Logs       │   │ │ - Logs       │    │ │ - Logs       │     │
│ └──────────────┘   │ └──────────────┘    │ └──────────────┘     │
└────────────────────┴──────────────────────┴──────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │      Kafka          │
                    │  (Topic: pulse)     │
                    │  - Partitioned      │
                    │  - Replicated       │
                    │  - Persistent       │
                    └─────────────────────┘
                              ↓
                    ┌─────────────────────┐
                    │    Collector        │
                    │  (Go Microservice)  │
                    │  - Consumes Kafka   │
                    │  - Validates data   │
                    │  - Aggregates       │
                    │  - Deduplicates     │
                    └─────────────────────┘
                              ↓
            ┌─────────────────┴────────────────┐
            ↓                                   ↓
    ┌──────────────────┐          ┌────────────────────────┐
    │   ScyllaDB       │          │   Redis Cache          │
    │ (Time-Series DB) │          │ (Session State)        │
    │  - Metrics       │          │ - Query results        │
    │  - Traces        │          │ - Alerts               │
    │  - Logs          │          │ - Notifications        │
    │  - Indexed       │          └────────────────────────┘
    └──────────────────┘
            ↓
    ┌──────────────────────────┐
    │  React Dashboard         │
    │  - Real-time UI          │
    │  - Query builder         │
    │  - Alerting              │
    │  - Service topology      │
    └──────────────────────────┘
```

---

## Component Breakdown

### 1. Agent (PulseAgent)

**Purpose:** Collect metrics, traces, and logs from application runtime.

**Responsibilities:**
- Automatically instrument code (via OpenTelemetry SDK)
- Collect system metrics (CPU, memory, disk, goroutines)
- Capture distributed traces (W3C trace context)
- Batch and buffer telemetry data
- Handle network failures gracefully
- Minimize performance overhead

**Key Features:**
- **Language Agnostic:** Supports Go, Python, Java, Node.js (via OpenTelemetry)
- **Low Overhead:** < 5% CPU, < 50MB memory per service
- **Batching:** Reduces network calls (batch size: configurable)
- **Sampling:** Optional sampling for high-volume services
- **Compression:** Snappy compression for Kafka payload

**Technology:**
- OpenTelemetry SDK (standard telemetry collection)
- Kafka producer (reliable delivery)
- Protobuf serialization (efficient wire format)

### 2. Kafka (Message Broker)

**Purpose:** Decouple agents from collectors, provide high-throughput buffer.

**Topics:**
```
pulse-metrics  → Agent metrics (1000+ msgs/sec)
pulse-traces   → Distributed traces (100+ msgs/sec)
pulse-logs     → Application logs (10k+ msgs/sec)
```

**Configuration:**
- **Replication Factor:** 3 (in production)
- **Partitions:** 12 (allows parallel processing)
- **Retention:** 7 days (rolling window)
- **Compression:** Snappy (reduces storage, network)

**Why Kafka?**
- Handles spikes in telemetry (agents burst, collector processes steadily)
- Reliable delivery (agents don't wait for collector)
- Replay capability (debug past data)
- Scales horizontally (more partitions, more consumers)

### 3. Collector

**Purpose:** Consume telemetry from Kafka, validate, aggregate, store in database.

**Responsibilities:**
- Consume from Kafka topics (consumer group: pulse-collector)
- Parse & validate protobuf messages
- Detect & handle duplicates (via trace-id / metric-id)
- Aggregate metrics (time-window bucketing)
- Write to ScyllaDB (batch inserts for performance)
- Expose Prometheus metrics (self-monitoring)

**Scaling:**
- Stateless design → horizontal scaling
- Each consumer reads different partition
- No shared state → fault tolerance
- Restart any instance without data loss

**Error Handling:**
- Parse errors → log & skip
- Database errors → retry with backoff
- Service unavailable → pause consumer

### 4. ScyllaDB (Time-Series Database)

**Purpose:** Durable storage for metrics, traces, logs.

**Schema Design:**

#### Metrics Table
```sql
CREATE TABLE metrics (
  service_name TEXT,
  metric_name TEXT,
  timestamp BIGINT,
  tags MAP<TEXT, TEXT>,
  value DOUBLE,
  PRIMARY KEY ((service_name, metric_name), timestamp)
)
```

**Why this design?**
- Partition by (service, metric) → balanced load
- Sort by timestamp DESC → recent data queried first
- TTL (30 days) → automatic data cleanup
- Allows queries: "Get metric X for service Y over time range"

#### Traces Table
```sql
CREATE TABLE spans (
  trace_id TEXT,
  span_id TEXT,
  service_name TEXT,
  operation_name TEXT,
  start_time BIGINT,
  duration_micros BIGINT,
  parent_span_id TEXT,
  status TEXT,
  tags MAP<TEXT, TEXT>,
  PRIMARY KEY ((trace_id), span_id)
)
```

**Why?**
- Partition by trace_id → all spans of a trace on same node
- Allows reconstructing complete trace with single partition read
- Efficient for "show me this entire trace" queries

#### Logs Table
```sql
CREATE TABLE logs (
  service_name TEXT,
  timestamp BIGINT,
  level TEXT,
  message TEXT,
  trace_id TEXT,
  span_id TEXT,
  attributes MAP<TEXT, TEXT>,
  PRIMARY KEY ((service_name), timestamp)
)
```

**Replication & HA:**
- Replication Factor: 3 (survive 2-node failure)
- Consistency Level: Quorum (read/write majority)
- Snapshotting: Automated backups

### 5. Dashboard (React Frontend)

**Purpose:** Query and visualize telemetry data.

**Features (Phase by Phase):**

**Phase 1 (Week 5):**
- Service selector
- Metric picker (dropdown)
- Time-range selector (last 1h, 6h, 24h)
- Line chart (Recharts)
- Auto-refresh

**Phase 2 (Week 10):**
- Distributed trace viewer
- Flame graph (latency breakdown by service)
- Service dependency graph
- Timeline waterfall view

**Phase 3 (Week 18):**
- Custom dashboard builder
- Alert rules UI
- Anomaly visualization
- Log search & correlation

**Technology:**
- React + TypeScript
- Recharts (time-series charts)
- D3 (dependency graphs)
- WebGL (3D service topology - optional)

---

## Data Flow

### Metrics Example

```
Service A: app.recordMetric("request.latency", 150)
                    ↓
            Agent batches in-memory
                    ↓
            [MetricsPayload] {
              service_name: "api",
              metrics: [
                {name: "request.latency", value: 150, timestamp: 1234567890}
              ]
            }
                    ↓
            Serialize to protobuf (50 bytes)
                    ↓
            Kafka Producer: send to topic "pulse-metrics"
                    ↓
            Kafka Broker: partition by service_name hash
                    ↓
            Collector Consumer Group: reads partition
                    ↓
            Collector: validates, parses protobuf
                    ↓
            ScyllaDB: INSERT INTO metrics (...)
                    ↓
            Dashboard: SELECT * FROM metrics WHERE ... (via gRPC)
                    ↓
            User sees: graph of request latency over time
```

### Traces Example

```
Service A receives HTTP request
                    ↓
            [Span created]
                TraceID: abc123
                SpanID: span1
                Service: api
                Operation: POST /users
                    ↓
            Service A calls Service B (HTTP)
                    ↓
            [Trace context propagated]
                Headers: traceparent=abc123;span1
                    ↓
            Service B receives request
                    ↓
            [Child Span created]
                TraceID: abc123 (inherited)
                SpanID: span2
                ParentSpanID: span1
                Service: database
                Operation: query_user
                    ↓
            All spans sent to Kafka at end of request
                    ↓
            Collector: groups spans by trace_id
                    ↓
            ScyllaDB: stores all spans
                    ↓
            Dashboard: reconstructs trace (span2 parent of span1)
                    ↓
            User sees: waterfall showing A → B interaction, total latency
```

---

## Performance Characteristics

### Agent
- **CPU Overhead:** < 2%
- **Memory Overhead:** < 50MB
- **Batching Delay:** 10s (configurable)
- **Max Throughput:** 10k+ metrics/sec per instance

### Collector
- **Throughput:** 100k+ metrics/sec
- **Latency:** < 100ms end-to-end (agent → storage)
- **Memory:** 500MB-1GB per instance
- **Replication:** Stateless (scale horizontally)

### Database
- **Write Latency:** < 10ms (local)
- **Read Latency:** < 100ms (with caching)
- **Compression Ratio:** 5:1 (metric payloads)
- **Storage:** ~100MB per 1M metrics (with compression)

---

## Reliability & Fault Tolerance

### Agent Failures
- **On Kafka Down:** Buffer in-memory (10k messages), retry on recovery
- **On Network Partition:** Retry with exponential backoff
- **On Service Crash:** In-flight data lost (acceptable for metrics)

### Collector Failures
- **Multi-instance:** No shared state, any instance can fail
- **Kafka broker down:** Pause, wait for broker recovery
- **Database down:** Retry with backoff, metrics not lost (buffered in Kafka)

### Database Failures
- **Node failure:** Quorum-based reads/writes ensure consistency
- **Partition failure:** Automatic re-replication (RF=3)
- **Data loss:** 7-day Kafka retention allows replay

---

## Scalability

### Horizontal Scaling

**Agents:** Simply deploy to more services (stateless)

**Collector:**
```
N agents → 1 Kafka topic (partitioned) → N collectors
Each collector reads subset of partitions
Total throughput = N × (single collector throughput)
```

**Database (ScyllaDB):**
```
Add node → data re-balances → throughput increases
Scales linearly with node count
```

### Vertical Scaling

**Agent:** Already minimal

**Collector:** Increase memory (better batching), CPUs (more go-routines)

**Database:** Standard Cassandra/ScyllaDB tuning

---

## Monitoring PulseMetrics Itself

The system uses its own agents to monitor itself:

```
Prometheus
    ↓
Collector (exports metrics)
    ↓
PulseMetrics Agent monitors Collector
    ↓
    Exports to Prometheus
    ↓
Grafana dashboard shows "Collector health"
```

**Metrics:**
- `collector_messages_consumed` (throughput)
- `collector_processing_latency` (health)
- `database_write_latency` (bottleneck detection)
- `kafka_consumer_lag` (backlog)

---

## Future Enhancements

### Phase 2-3 (Planned)
- **Anomaly Detection:** Statistical baselines, Z-score detection
- **Alerting:** Rules engine, multi-channel notifications
- **Log Aggregation:** Full-text search, pattern detection
- **Correlation:** Link logs → traces → metrics

### Phase 4+ (Optional)
- **Sampling:** Smart sampling for high-volume services
- **Distributed Tracing:** Jaeger compatibility
- **Service Mesh Integration:** Istio/Linkerd sidecar injection
- **Multi-Tenancy:** Data isolation per tenant
- **Custom Dashboards:** Drag-drop builder
- **3D Visualization:** WebGL service topology

---

## Deployment Architecture (Production)

```
┌─────────────────────────────────────────────────────────┐
│                  Kubernetes Cluster                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Kafka 1    │  │   Kafka 2    │  │   Kafka 3    │ │
│  │  (Broker 1)  │  │  (Broker 2)  │  │  (Broker 3)  │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  Replication Factor: 3, Partitions: 12                 │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Collector 1  │  │ Collector 2  │  │ Collector 3  │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  Stateless, scales horizontally                        │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ScyllaDB Node1│  │ScyllaDB Node2│  │ScyllaDB Node3│ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│  Replication Factor: 3, RF=2 for performance           │
│                                                         │
│  ┌────────────────────────────────────────────────────┐│
│  │  Dashboard (React + Node.js API Gateway)          ││
│  └────────────────────────────────────────────────────┘│
│                                                         │
└─────────────────────────────────────────────────────────┘
```

---

## References

- OpenTelemetry: https://opentelemetry.io
- Kafka: https://kafka.apache.org
- ScyllaDB: https://scylladb.com
- Protobuf: https://developers.google.com/protocol-buffers
- React: https://react.dev
