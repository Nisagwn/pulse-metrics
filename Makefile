.PHONY: help build proto docker logs clean test test-integration test-all health run-demo alerts evaluate

# Colors for output
BLUE := \033[0;36m
GREEN := \033[0;32m
NC := \033[0m # No Color

help:
	@echo "$(BLUE)PulseMetrics - APM Platform$(NC)"
	@echo "Available commands:"
	@echo "  $(GREEN)make docker-up$(NC)        Start Docker Compose services"
	@echo "  $(GREEN)make docker-down$(NC)      Stop Docker Compose services"
	@echo "  $(GREEN)make docker-logs$(NC)      View Docker Compose logs"
	@echo "  $(GREEN)make proto$(NC)            Generate protobuf code"
	@echo "  $(GREEN)make build-agent$(NC)      Build agent binary"
	@echo "  $(GREEN)make build-collector$(NC)  Build collector binary"
	@echo "  $(GREEN)make build-dashboard$(NC)  Build dashboard API binary"
	@echo "  $(GREEN)make build-demo$(NC)       Build traced demo microservices"
	@echo "  $(GREEN)make build-all$(NC)        Build all binaries"
	@echo "  $(GREEN)make test$(NC)             Run unit tests"
	@echo "  $(GREEN)make test-integration$(NC) Run integration tests (needs Docker)"
	@echo "  $(GREEN)make clean$(NC)            Clean build artifacts"
	@echo "  $(GREEN)make dev$(NC)              Run complete dev setup"

# Docker Compose commands
docker-up:
	@echo "$(BLUE)Starting Docker Compose...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "  Kafka: localhost:9092 (topics: pulse-metrics, pulse-traces, pulse-logs)"
	@echo "  ScyllaDB: localhost:9042"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana: http://localhost:3000 (admin/admin)"

docker-down:
	@echo "$(BLUE)Stopping Docker Compose...$(NC)"
	docker-compose down
	@echo "$(GREEN)Services stopped!$(NC)"

docker-logs:
	docker-compose logs -f

docker-status:
	docker-compose ps

# Protocol Buffers
proto:
	@echo "$(BLUE)Generating protobuf code...$(NC)"
	@mkdir -p internal/proto
	protoc -I=proto --go_out=internal/proto --go_opt=paths=source_relative \
	       metrics.proto logs.proto traces.proto
	@echo "$(GREEN)Protobuf code generated!$(NC)"

# Build commands
build-agent: proto
	@echo "$(BLUE)Building agent...$(NC)"
	cd cmd/agent && go build -o ../../bin/agent .
	@echo "$(GREEN)Agent built: ./bin/agent$(NC)"

build-collector: proto
	@echo "$(BLUE)Building collector...$(NC)"
	cd cmd/collector && go build -o ../../bin/collector .
	@echo "$(GREEN)Collector built: ./bin/collector$(NC)"

build-dashboard: proto
	@echo "$(BLUE)Building dashboard-api...$(NC)"
	go build -o bin/dashboard-api ./cmd/dashboard-api
	@echo "$(GREEN)Dashboard API built: ./bin/dashboard-api$(NC)"

build-demo: proto
	@echo "$(BLUE)Building demo microservices...$(NC)"
	go build -o bin/demo ./cmd/demo
	@echo "$(GREEN)Demo built: ./bin/demo$(NC)"

build-all: build-agent build-collector build-dashboard build-demo
	@echo "$(GREEN)All binaries built!$(NC)"

# Testing
test:
	@echo "$(BLUE)Running unit tests...$(NC)"
	go test -race ./...

test-integration:
	@echo "$(BLUE)Running integration tests (Kafka + ScyllaDB gerekli)...$(NC)"
	go test -tags integration -v -timeout 15m ./test/...

test-all: test test-integration

test-coverage:
	go test -cover ./...

# Development
dev: docker-up proto build-all
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo "Next steps:"
	@echo "  1. Run collector: ./bin/collector --debug"
	@echo "  2. Run agent (in another terminal): ./bin/agent --debug"
	@echo "  3. Run dashboard (third terminal): make run-dashboard"
	@echo "  4. Run demo (fourth terminal): make run-demo"
	@echo "  5. Open http://localhost:8080"

run-collector:
	@echo "$(BLUE)Starting collector...$(NC)"
	./bin/collector --kafka localhost:9092 --scylla localhost:9042 --debug

run-agent:
	@echo "$(BLUE)Starting agent...$(NC)"
	./bin/agent --service test-app --kafka localhost:9092 --debug --interval 5s

run-demo:
	@echo "$(BLUE)Starting traced demo microservices...$(NC)"
	./bin/demo --kafka localhost:9092 --rps 4

run-dashboard:
	@echo "$(BLUE)Starting dashboard API on http://localhost:8080 ...$(NC)"
	./bin/dashboard-api --addr :8080 --collector localhost:50051

health:
	@echo "$(BLUE)Health checks:$(NC)"
	@curl -s localhost:8081/readyz && echo ""
	@curl -s localhost:8082/readyz && echo ""
	@curl -s localhost:8080/readyz && echo ""

alerts:
	@echo "$(BLUE)Alarm kurallari ve son alarmlar:$(NC)"
	@curl -s localhost:8080/api/v1/rules && echo ""
	@curl -s "localhost:8080/api/v1/alerts?range=24h" && echo ""

evaluate:
	@echo "$(BLUE)Kurallari simdi degerlendir:$(NC)"
	@curl -s -X POST localhost:8080/api/v1/evaluate && echo ""

# Cleaning
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -f bin/agent bin/collector bin/dashboard-api bin/demo
	rm -rf internal/proto/*.pb.go
	@echo "$(GREEN)Cleaned!$(NC)"

clean-docker:
	docker-compose down -v
	@echo "$(GREEN)Docker volumes cleaned!$(NC)"

# Utilities
verify-proto-compiler:
	@which protoc > /dev/null || (echo "$(RED)protoc not found. Install with: brew install protobuf$(NC)" && exit 1)
	@echo "$(GREEN)protoc is installed: $(shell protoc --version)$(NC)"

info:
	@echo "$(BLUE)PulseMetrics Development Info$(NC)"
	@echo "Project: APM Platform (6 months)"
	@echo "Phase: 1 - Foundation (Weeks 1-4)"
	@echo ""
	@echo "Tech Stack:"
	@echo "  Backend: Go + OpenTelemetry"
	@echo "  Message Queue: Kafka"
	@echo "  Database: ScyllaDB"
	@echo "  Dashboard: React (TBD)"
	@echo ""
	@echo "Documentation:"
	@echo "  - README.md: Quick start & overview"
	@echo "  - APM_PROJECT_PLAN.md: Full 6-month roadmap"

.DEFAULT_GOAL := help
