# Quick Start (5 Minutes)

## Prerequisites
- Docker & Docker Compose
- Go 1.21+
- `protoc` compiler: `brew install protobuf` (macOS), `apt-get install protobuf-compiler` (Linux),
  or download a release zip from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases) (Windows)
- protoc Go plugins:
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```
  Make sure `$(go env GOPATH)/bin` is on your `PATH`.

## Go

```bash
# 1. Navigate to project
cd pulse-metrics

# 2. Start Docker services
docker compose up -d

# 3. Wait ~15 seconds for Kafka and ScyllaDB to come up
docker compose ps

# 4. Generate protobuf + gRPC code
protoc -I=proto --go_out=internal/proto --go_opt=paths=source_relative \
       --go-grpc_out=internal/proto --go-grpc_opt=paths=source_relative \
       metrics.proto logs.proto traces.proto

# 5. Build binaries
go build -o bin/collector      ./cmd/collector
go build -o bin/agent          ./cmd/agent
go build -o bin/dashboard-api  ./cmd/dashboard-api
go build -o bin/demo           ./cmd/demo

# 6. Run collector (Terminal 1)
./bin/collector --debug

# 7. Run agent (Terminal 2)
./bin/agent --debug --service demo-app --instance node-1 --interval 5s

# 8. Run dashboard API (Terminal 3)
./bin/dashboard-api --addr :8080 --collector localhost:50051

# 9. Run the traced demo microservices (Terminal 4)
./bin/demo --kafka localhost:9092 --rps 4

# 10. Open the dashboard
#     http://localhost:8080
```

The collector creates the `pulse` keyspace and `metrics` table on startup,
so no manual schema step is needed.

## Or Use Make

```bash
make dev             # Steps 2-5
make run-collector   # Step 6
make run-agent       # Step 7
make run-dashboard   # Step 8
make run-demo        # Step 9
```

## Verify It Works

✓ Agent logs: `Metrics published {"count": 4}`
✓ Collector logs: `Metrics stored {"service": "demo-app", "instance": "node-1", "count": 4}`
✓ Health: `curl localhost:8082/readyz` → `{"status":"ready","checks":{"scylladb":"ok"}}`
✓ API: `curl "localhost:8080/api/v1/query?service=demo-app&metric=process.runtime.goroutines&range=15m"`
✓ Database:
```bash
docker exec pulse-scylladb cqlsh -e "SELECT COUNT(*) FROM pulse.metrics;"
```

### Try two instances

Run a second agent to see per-instance separation on one chart:

```bash
./bin/agent --service demo-app --instance node-2 --health :8083 --interval 5s --debug
```

The dashboard now draws one line per instance. `instance_id` is part of the
clustering key, so two instances reporting in the same millisecond produce two
rows instead of overwriting each other.

## Ports

| Service       | Port    | What                         |
|---------------|---------|------------------------------|
| dashboard     | 8080    | React panel + JSON API       |
| agent health  | 8081    | `/healthz`, `/readyz`        |
| collector     | 8082    | `/healthz`, `/readyz`        |
| collector     | 50051   | gRPC Metrics/Trace/Log/Alert |
| Kafka         | 9092    |                              |
| ScyllaDB      | 9042    | CQL                          |
| Prometheus    | 9090    |                              |
| Grafana       | 3000    | admin / admin (bos - asagi bak) |
| demo gateway  | 9101    | /checkout                    |
| demo orders   | 9102    | /orders                      |
| demo payments | 9103    | /charge                      |
| demo inventory| 9104    | /reserve                     |

### Grafana hakkinda

Kullanici adi ve parola `admin` / `admin` (docker-compose.yml icinde tanimli).
Ama Grafana'da PulseMetrics verisi **yok**: Prometheus bu servisleri kazimiyor
ve ScyllaDB icin bir Grafana datasource'u tanimli degil. Metrik ve trace'ler
icin http://localhost:8080 adresindeki kendi panelini kullan.

## Uc ayak birlikte

```bash
# 1) Hatali bir log bul
curl "localhost:8080/api/v1/logs?service=payments&range=15m&levels=ERROR&limit=1"

# 2) Cikan trace_id ile o istegin TUM servislerdeki loglarini al
curl "localhost:8080/api/v1/trace-logs?id=<trace_id>"

# 3) Ayni trace'in span'lerini gor: nerede yavasladi?
curl "localhost:8080/api/v1/trace?id=<trace_id>"
```

Panelde ayni sey iki tiklama: Loglar sekmesinde bir trace_id'ye tikla,
trace'in waterfall'i ve altinda o trace'in loglari acilir.

## Tests

```bash
go test -race ./...                                   # unit, no infra needed
go test -tags integration -v -timeout 15m ./test/...  # needs docker compose up
```

## Next Steps

See README.md for the development workflow and docs/ARCHITECTURE.md for system design.
