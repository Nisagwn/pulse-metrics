.PHONY: help build proto docker logs clean test test-integration test-all health run-demo alerts evaluate \
        migrate migrate-dry topics metrics scale-up docker-images docker-apps version build-migrate

# Windows'ta uretilen ikiliye .exe uzantisi ver.
# Go -o ile verilen adi oldugu gibi kullanir; uzantisiz bir dosya
# Windows'ta calisir ama arac zincirleri (tasklist, Stop-Process,
# Explorer) onu program olarak tanimaz.
ifeq ($(OS),Windows_NT)
EXE := .exe
else
EXE :=
endif

# Surum bilgisi ikiliye -ldflags ile gomulur; /healthz ve
# Prometheus'taki pulse_build_info bunu gosterir.
VERSION ?= v0.4.0
COMMIT  := $(shell git rev-parse --short=12 HEAD 2>/dev/null)
LDFLAGS := -X github.com/nisah/pulse-metrics/internal/buildinfo.Version=$(VERSION) \
           -X github.com/nisah/pulse-metrics/internal/buildinfo.Commit=$(COMMIT)

# Renkler.
#
# Kacis dizisini printf ile URETIYORUZ. Dosyaya duz metin olarak \033
# yazmak yetmiyor: Makefile'daki @echo kabugun echo'suna gider ve Git
# Bash'in yerlesik echo'su ters bolu kacislarini -e olmadan yorumlamaz.
# Sonuc, her ciktinin basinda ham "\033[0;36m" gorunmesiydi.
ESC   := $(shell printf '\033')
BLUE  := $(ESC)[0;36m
GREEN := $(ESC)[0;32m
NC    := $(ESC)[0m

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
	@echo ""
	@echo "$(BLUE)Faz 4 - uretime hazirlik$(NC)"
	@echo "  $(GREEN)make migrate-dry$(NC)      Sema gocu: sadece plani goster"
	@echo "  $(GREEN)make migrate$(NC)          Sema gocu: calistir (collector'lari once durdur)"
	@echo "  $(GREEN)make topics$(NC)           Kafka topic'lerini 3 partition'a cikar"
	@echo "  $(GREEN)make metrics$(NC)          Collector'in kendi olculerini goster"
	@echo "  $(GREEN)make scale-up$(NC)         Ikinci bir collector baslat"
	@echo "  $(GREEN)make docker-images$(NC)    Konteyner imajlarini derle"
	@echo "  $(GREEN)make docker-apps$(NC)      Uygulamalari konteynerde calistir (2 collector)"

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
	go build -ldflags "$(LDFLAGS)" -o bin/agent$(EXE) ./cmd/agent
	@echo "$(GREEN)Agent built: ./bin/agent$(EXE)$(NC)"

build-collector: proto
	@echo "$(BLUE)Building collector...$(NC)"
	go build -ldflags "$(LDFLAGS)" -o bin/collector$(EXE) ./cmd/collector
	@echo "$(GREEN)Collector built: ./bin/collector$(EXE)$(NC)"

build-dashboard: proto
	@echo "$(BLUE)Building dashboard-api...$(NC)"
	go build -ldflags "$(LDFLAGS)" -o bin/dashboard-api$(EXE) ./cmd/dashboard-api
	@echo "$(GREEN)Dashboard API built: ./bin/dashboard-api$(EXE)$(NC)"

build-demo: proto
	@echo "$(BLUE)Building demo microservices...$(NC)"
	go build -ldflags "$(LDFLAGS)" -o bin/demo$(EXE) ./cmd/demo
	@echo "$(GREEN)Demo built: ./bin/demo$(EXE)$(NC)"

build-migrate: proto
	@echo "$(BLUE)Building pulse-migrate...$(NC)"
	go build -ldflags "$(LDFLAGS)" -o bin/pulse-migrate$(EXE) ./cmd/pulse-migrate
	@echo "$(GREEN)Migrate built: ./bin/pulse-migrate$(EXE)$(NC)"

build-all: build-agent build-collector build-dashboard build-demo build-migrate
	@echo "$(GREEN)All binaries built!$(NC)"

version: build-collector
	@./bin/collector$(EXE) -version

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
	./bin/collector$(EXE) --kafka localhost:9092 --scylla localhost:9042 --debug

run-agent:
	@echo "$(BLUE)Starting agent...$(NC)"
	./bin/agent$(EXE) --service test-app --kafka localhost:9092 --debug --interval 5s

run-demo:
	@echo "$(BLUE)Starting traced demo microservices...$(NC)"
	./bin/demo$(EXE) --kafka localhost:9092 --rps 4

run-dashboard:
	@echo "$(BLUE)Starting dashboard API on http://localhost:8080 ...$(NC)"
	./bin/dashboard-api$(EXE) --addr :8080 --collector localhost:50051

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

# --- Faz 4: uretime hazirlik -------------------------------------------------

migrate-dry: build-migrate
	@echo "$(BLUE)Sema gocu plani (hicbir sey degistirilmez):$(NC)"
	./bin/pulse-migrate$(EXE) -scylla localhost:9042

migrate: build-migrate
	@echo "$(BLUE)Sema gocu calistiriliyor...$(NC)"
	@echo "UYARI: collector'lari once durdur. Goc sirasinda tablo kisa sure yok olur."
	./bin/pulse-migrate$(EXE) -scylla localhost:9042 -confirm -keep-backup

topics:
	@echo "$(BLUE)Kafka topic'leri 3 partition'a cikariliyor...$(NC)"
	@echo "Bir consumer group'ta ayni anda en fazla PARTITION SAYISI kadar"
	@echo "tuketici is yapabilir; tek partition ile ikinci collector bos oturur."
	@for t in pulse-metrics pulse-traces pulse-logs; do \
		docker exec pulse-kafka kafka-topics --bootstrap-server localhost:9092 \
			--alter --topic $$t --partitions 3 2>/dev/null || true; \
	done
	@for t in pulse-metrics pulse-traces pulse-logs; do \
		echo -n "  $$t partition sayisi: "; \
		docker exec pulse-kafka kafka-topics --bootstrap-server localhost:9092 \
			--describe --topic $$t 2>/dev/null | grep -c "Partition:"; \
	done

metrics:
	@echo "$(BLUE)Collector'in kendi olculeri:$(NC)"
	@curl -s localhost:8082/metrics | grep -E "^pulse_" | head -30

scale-up: build-collector
	@echo "$(BLUE)Ikinci collector baslatiliyor (:50053, saglik :8084)$(NC)"
	@echo "Ayni consumer group'ta oldugu icin partition'lari bolusurler."
	PULSE_INSTANCE_ID=collector-2 ./bin/collector$(EXE) -port 50053 -health :8084

docker-images:
	@echo "$(BLUE)Konteyner imajlari derleniyor...$(NC)"
	@for c in collector agent dashboard-api demo; do \
		echo "  -> pulse/$$c"; \
		docker build -q --build-arg CMD_NAME=$$c --build-arg VERSION=$(VERSION) \
			--build-arg COMMIT=$(COMMIT) -t pulse/$$c:$(VERSION) -t pulse/$$c:latest . ; \
	done
	@echo "$(GREEN)Imajlar hazir$(NC)"

docker-apps:
	@echo "$(BLUE)Uygulamalar konteynerde baslatiliyor (2 collector)...$(NC)"
	docker compose -f docker-compose.apps.yml up -d --build
	@echo "$(GREEN)Panel: http://localhost:8080$(NC)"
	@echo "  collector-1 olculeri: http://localhost:8092/metrics"
	@echo "  collector-2 olculeri: http://localhost:8093/metrics"

docker-apps-down:
	docker compose -f docker-compose.apps.yml down

# Cleaning
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -f bin/agent$(EXE) bin/collector$(EXE) bin/dashboard-api$(EXE) bin/demo$(EXE) bin/pulse-migrate$(EXE)
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
	@echo "$(BLUE)PulseMetrics$(NC) $(VERSION) ($(COMMIT))"
	@echo "Faz: 4 - uretime hazirlik"
	@echo ""
	@echo "Tech Stack:"
	@echo "  Backend: Go (kendi trace/log SDK'lari)"
	@echo "  Message Queue: Kafka"
	@echo "  Database: ScyllaDB"
	@echo "  Oz-izleme: Prometheus + Grafana"
	@echo "  Dashboard: React (CDN, derleme adimi yok)"
	@echo ""
	@echo "Documentation:"
	@echo "  - README.md          genel bakis"
	@echo "  - QUICKSTART.md      hizli baslangic"
	@echo "  - docs/OPERATIONS.md isletim rehberi (Faz 4)"

.DEFAULT_GOAL := help
