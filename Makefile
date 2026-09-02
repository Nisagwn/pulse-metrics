.PHONY: help build proto docker logs clean test

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
	@echo "  $(GREEN)make build-all$(NC)        Build all binaries"
	@echo "  $(GREEN)make test$(NC)             Run tests"
	@echo "  $(GREEN)make clean$(NC)            Clean build artifacts"
	@echo "  $(GREEN)make dev$(NC)              Run complete dev setup"

# Docker Compose commands
docker-up:
	@echo "$(BLUE)Starting Docker Compose...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Services started!$(NC)"
	@echo "  Kafka: localhost:9092"
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

build-all: build-agent build-collector
	@echo "$(GREEN)All binaries built!$(NC)"

# Testing
test:
	@echo "$(BLUE)Running tests...$(NC)"
	go test -v ./...

test-coverage:
	go test -v -cover ./...

# Development
dev: docker-up proto build-all
	@echo "$(GREEN)Development environment ready!$(NC)"
	@echo "Next steps:"
	@echo "  1. Run collector: ./bin/collector --debug"
	@echo "  2. Run agent (in another terminal): ./bin/agent --debug"

run-collector:
	@echo "$(BLUE)Starting collector...$(NC)"
	./bin/collector --kafka localhost:9092 --scylla localhost:9042 --debug

run-agent:
	@echo "$(BLUE)Starting agent...$(NC)"
	./bin/agent --service test-app --kafka localhost:9092 --debug --interval 5s

# Cleaning
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -f bin/agent bin/collector
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
