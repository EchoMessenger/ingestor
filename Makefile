VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
IMAGE     ?= ghcr.io/echomessenger/ingestor
PLATFORM  ?= linux/amd64

.PHONY: deps tidy build run seed infra-up infra-down topics health stats lint docker-build docker-push

# ---- Go ----

deps:
	go mod download

tidy:
	go mod tidy

build:
	mkdir -p bin
	go build \
		-trimpath \
		-ldflags="-s -w -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)" \
		-o bin/ingestor \
		./cmd/main.go
	@echo "built bin/ingestor  version=$(VERSION)"

run:
	@test -f .env.local || (echo "missing .env.local — copy from .env.example" && exit 1)
	export $$(cat .env.local | grep -v '^#' | xargs) && go run ./cmd/main.go

# Положить тестовые сообщения в каждый топик
seed:
	export $$(cat .env.local | grep -v '^#' | xargs) && go run ./cmd/seed/main.go

lint:
	@which golangci-lint > /dev/null || (echo "install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

# ---- Docker ----

docker-build:
	docker build \
		--platform $(PLATFORM) \
		--build-arg VERSION=$(VERSION) \
		--build-arg BUILD_TIME=$(BUILD_TIME) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.
	@echo "built $(IMAGE):$(VERSION)"

docker-push:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest

# ---- Local infra ----

infra-up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@for i in $$(seq 1 30); do \
		docker compose ps --format json | grep -q '"Health":"healthy"' && break; \
		echo "  waiting... ($$i/30)"; \
		sleep 2; \
	done
	$(MAKE) topics
	@echo "Infrastructure ready"

infra-down:
	docker compose down -v

topics:
	@echo "Creating Kafka topics..."
	@for t in \
		"account-events:3" \
		"topic-events:3" \
		"subscription-events:5" \
		"message-events:10" \
		"search-queries:3" \
		"firehose.handshake:3" \
		"firehose.account-mgmt:3" \
		"firehose.auth:3" \
		"firehose.subscriptions:5" \
		"firehose.messages:10" \
		"firehose.queries:3" \
		"firehose.updates:3" \
		"firehose.deletions:3" \
		"firehose.notifications:5" \
		"dlq:3"; do \
		name=$$(echo $$t | cut -d: -f1); \
		parts=$$(echo $$t | cut -d: -f2); \
		docker compose exec -T kafka \
			kafka-topics --bootstrap-server localhost:9092 \
			--create --if-not-exists \
			--topic tinode.$$name \
			--partitions $$parts \
			--replication-factor 1 2>&1 | grep -v "already exists" || true; \
	done
	@echo "Topics ready"

# ---- Diagnostics ----

health:
	@curl -sf http://localhost:8081/healthz && echo " /healthz ok" || echo " /healthz FAILED"
	@curl -sf http://localhost:8081/readyz  && echo " /readyz ok"  || echo " /readyz not ready"

stats:
	@curl -s http://localhost:8081/stats | python3 -m json.tool

metrics:
	@curl -s http://localhost:8081/metrics

clickhouse:
	docker compose exec clickhouse clickhouse-client