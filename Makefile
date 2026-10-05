# Common tasks. Run "make help" to see them.

REPO_DIR := $(abspath .)
PG_PORT ?= 55432
TEST_DSN ?= postgres://postgres:postgres@localhost:$(PG_PORT)/postgres
BUILDER ?= $(shell go env GOPATH)/bin/builder

.PHONY: help test test-db db db-stop collector clean demo-up demo-load demo-down

help: ## Show this help
	@grep -E "^[a-z-]+:.*##" $(MAKEFILE_LIST) | sed "s/:.*##/:/"

test: ## Unit tests (no database needed)
	go test -race ./...
	cd processor/isowatchprocessor && go test -race ./...

db: ## Start PostgreSQL 18 in Docker for tests and the demo
	docker run -d --name isowatch-pg -e POSTGRES_PASSWORD=postgres -p $(PG_PORT):5432 postgres:18

db-stop: ## Stop and remove the test database
	docker rm -f isowatch-pg

test-db: ## All tests, including real PostgreSQL ones (run "make db" first)
	ISOWATCH_TEST_DSN=$(TEST_DSN) go test -race -count=1 ./...
	cd processor/isowatchprocessor && go test -race ./...

collector: ## Build the collector with the IsoWatch processor into collector/build
	@test -x $(BUILDER) || go install go.opentelemetry.io/collector/cmd/builder@v0.162.0
	sed "s|REPO_DIR|$(REPO_DIR)|g" collector/builder-config.yaml > collector/.builder.yaml
	cd collector && $(BUILDER) --config=.builder.yaml

demo-up: ## Start PostgreSQL, Jaeger, the collector and the demo in Docker
	docker compose up -d --build
	@echo "Jaeger UI: http://localhost:16686   demo: http://localhost:8090/state"

demo-load: ## Send buggy traffic to the demo (use MODE=-safe or MODE=-auto)
	docker compose run --rm load $(MODE)

demo-down: ## Stop the Docker demo
	docker compose down

clean: ## Remove build output
	rm -rf collector/build collector/.builder.yaml
