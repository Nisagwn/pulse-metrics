# Quick Start (5 Minutes)

## Prerequisites
- Docker & Docker Compose
- Go 1.21+
- `protoc` compiler: `brew install protobuf` (macOS) or `apt-get install protobuf-compiler` (Linux)

## Go

```bash
# 1. Clone or navigate to project
cd pulse-metrics

# 2. Start Docker services
docker-compose up -d

# 3. Wait 10 seconds for services to start
sleep 10

# 4. Generate protobuf code
protoc --go_out=internal/proto --go_opt=paths=source_relative \
       --go-grpc_out=internal/proto --go-grpc_opt=paths=source_relative \
       proto/*.proto

# 5. Build binaries
go build -o bin/collector ./cmd/collector
go build -o bin/agent ./cmd/agent

# 6. Run collector (Terminal 1)
./bin/collector --debug

# 7. Run agent (Terminal 2)
./bin/agent --debug --service demo-app

# 8. Check data in database (Terminal 3)
docker exec -it pulse-scylladb cqlsh -e "SELECT COUNT(*) FROM pulse.metrics;"
```

## Or Use Make

```bash
make dev          # Does steps 2-5 automatically
make run-collector  # Step 6
make run-agent    # Step 7
```

## Verify It Works

✓ Agent logs: `Metrics published: count=...`  
✓ Collector logs: `Metrics stored: count=...`  
✓ Database: `SELECT COUNT(*) FROM pulse.metrics;` shows growing number  

## View in Grafana

- http://localhost:3000 (admin / admin)
- Add Prometheus datasource: http://prometheus:9090
- Create dashboard to visualize metrics

## Next Steps

See README.md for development workflow and ARCHITECTURE.md for system design.
