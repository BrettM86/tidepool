.PHONY: help build run test test-db-up test-db-down db-migrate db-migrate-down dev-up dev-down plc-up plc-down jetstream-up jetstream-down lint fmt fmt-check clean e2e e2e-up e2e-test e2e-logs e2e-down check-lexicons

.DEFAULT_GOAL := help

CYAN := \033[36m
RESET := \033[0m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m

COMPOSE := docker compose -f docker-compose.dev.yml
E2E_COMPOSE := docker compose -f docker-compose.e2e.yml

DEV_DATABASE_URL ?= postgres://tidepool:tidepool@localhost:5442/tidepool_dev?sslmode=disable
TEST_DATABASE_URL ?= postgres://tidepool_test:tidepool_test@localhost:5443/tidepool_test?sslmode=disable

##@ General

help: ## Show this help message
	@echo ""
	@echo "$(CYAN)Tidepool Development Commands$(RESET)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make $(CYAN)<target>$(RESET)\n"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  $(CYAN)%-15s$(RESET) %s\n", $$1, $$2 } \
		/^##@/ { printf "\n$(YELLOW)%s$(RESET)\n", substr($$0, 5) }' $(MAKEFILE_LIST)
	@echo ""

##@ Build & Run

build: ## Build the tidepool binary
	@echo "$(GREEN)Building tidepool...$(RESET)"
	@go build -o tidepool ./cmd/tidepool
	@echo "$(GREEN)✓ Build complete: ./tidepool$(RESET)"

run: ## Run tidepool against the dev database (requires: make dev-up)
	@go run ./cmd/tidepool

##@ Local Development

dev-up: ## Start the dev database (port 5442)
	@echo "$(GREEN)Starting Tidepool dev database...$(RESET)"
	@$(COMPOSE) up -d --wait postgres
	@echo "$(GREEN)✓ PostgreSQL (dev) on localhost:5442$(RESET)"

dev-down: ## Stop all dev services (including the test database, PLC, and Jetstream)
	@echo "$(YELLOW)Stopping Tidepool dev stack...$(RESET)"
	@$(COMPOSE) --profile test --profile plc --profile jetstream down --remove-orphans
	@echo "$(GREEN)✓ Stopped$(RESET)"

plc-up: ## Start the local PLC directory (port 3002; first run builds did-method-plc)
	@echo "$(GREEN)Starting local PLC directory (first run takes several minutes)...$(RESET)"
	@$(COMPOSE) --profile plc up -d --wait postgres-plc plc-directory
	@echo "$(GREEN)✓ PLC directory on http://localhost:3002$(RESET)"

plc-down: ## Stop the local PLC directory
	@$(COMPOSE) --profile plc stop plc-directory postgres-plc
	@echo "$(GREEN)✓ PLC directory stopped$(RESET)"

jetstream-up: ## Start Jetstream against the local bridge (run `make run` first; ws on :6018)
	@echo "$(GREEN)Starting Jetstream pointed at the local bridge firehose...$(RESET)"
	@$(COMPOSE) --profile jetstream up -d jetstream
	@echo "$(GREEN)✓ Jetstream on ws://localhost:6018/subscribe (metrics :6019)$(RESET)"

jetstream-down: ## Stop the Jetstream container
	@$(COMPOSE) --profile jetstream stop jetstream
	@echo "$(GREEN)✓ Jetstream stopped$(RESET)"

##@ Database Management

db-migrate: ## Apply migrations to the dev database (goose CLI)
	@echo "$(GREEN)Running migrations...$(RESET)"
	@goose -dir internal/db/migrations postgres "$(DEV_DATABASE_URL)" up
	@echo "$(GREEN)✓ Migrations complete$(RESET)"

db-migrate-down: ## Roll back the last migration on the dev database
	@echo "$(YELLOW)Rolling back last migration...$(RESET)"
	@goose -dir internal/db/migrations postgres "$(DEV_DATABASE_URL)" down
	@echo "$(GREEN)✓ Rollback complete$(RESET)"

##@ Testing

test-db-up: ## Start the test database (port 5443)
	@echo "$(GREEN)Starting test database...$(RESET)"
	@$(COMPOSE) --profile test up -d --wait postgres-test
	@echo "$(GREEN)✓ PostgreSQL (test) on localhost:5443$(RESET)"

test-db-down: ## Stop the test database
	@$(COMPOSE) --profile test stop postgres-test
	@echo "$(GREEN)✓ Test database stopped$(RESET)"

test: test-db-up ## Run the test suite against real postgres (migrations run in-process)
	@echo "$(GREEN)Running tests (store tests migrate the test database themselves)...$(RESET)"
	@TIDEPOOL_TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./...
	@echo "$(GREEN)✓ Tests complete$(RESET)"

##@ End-to-end (real Lemmy + PLC + Jetstream)

e2e: ## Full e2e run: build + start the stack, run the suite, tear down (-v)
	@echo "$(GREEN)Starting e2e stack (first run builds Lemmy from source — grab a coffee)...$(RESET)"
	@up_status=0; $(E2E_COMPOSE) up -d --build --wait --wait-timeout 600 || up_status=$$?; \
	if [ $$up_status -ne 0 ]; then \
		echo "$(RED)✗ e2e stack failed to come up (exit $$up_status); recent logs:$(RESET)" >&2; \
		$(E2E_COMPOSE) logs --tail=100 || true; \
		echo "$(YELLOW)Tearing down half-started e2e stack...$(RESET)"; \
		$(E2E_COMPOSE) down -v --remove-orphans || true; \
		exit $$up_status; \
	fi; \
	echo "$(GREEN)Stack healthy; running e2e suite...$(RESET)"; \
	status=0; go test -tags e2e -count=1 -timeout 20m ./tests/e2e/... || status=$$?; \
	echo "$(YELLOW)Tearing down e2e stack...$(RESET)"; \
	down_status=0; $(E2E_COMPOSE) down -v --remove-orphans || down_status=$$?; \
	if [ $$down_status -ne 0 ]; then \
		echo "$(RED)✗ WARNING: e2e teardown failed (exit $$down_status) — the stack may still be running; try 'make e2e-down'$(RESET)" >&2; \
	fi; \
	if [ $$status -ne 0 ]; then exit $$status; fi; \
	exit $$down_status

e2e-up: ## Start the e2e stack and leave it running (for iterating on tests)
	@$(E2E_COMPOSE) up -d --build --wait --wait-timeout 600
	@echo "$(GREEN)✓ e2e stack up: tidepool 127.0.0.1:8092, lemmy 127.0.0.1:8541, jetstream 127.0.0.1:6028$(RESET)"

e2e-test: ## Run the e2e suite against an already-running stack (make e2e-up)
	@go test -tags e2e -count=1 -v -timeout 20m ./tests/e2e/...

e2e-logs: ## Tail all e2e stack logs
	@$(E2E_COMPOSE) logs -f

e2e-down: ## Tear down the e2e stack and its volumes
	@$(E2E_COMPOSE) down -v --remove-orphans
	@echo "$(GREEN)✓ e2e stack removed$(RESET)"

##@ Code Quality

check-lexicons: ## Verify vendored lexicons match the manifest (and ~/Code/coves when present)
	@./scripts/check-lexicons.sh

fmt: ## Format all Go code
	@echo "$(GREEN)Formatting Go code...$(RESET)"
	@gofmt -w ./cmd ./internal
	@echo "$(GREEN)✓ Formatting complete$(RESET)"

fmt-check: ## Check formatting without writing
	@unformatted=$$(gofmt -l ./cmd ./internal); \
	if [ -n "$$unformatted" ]; then \
		echo "$(RED)✗ Unformatted files:$(RESET)"; \
		echo "$$unformatted"; \
		echo "$(YELLOW)Run 'make fmt' to fix$(RESET)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ All files formatted$(RESET)"

lint: fmt-check ## Run golangci-lint (includes format check)
	@echo "$(GREEN)Running linter...$(RESET)"
	@golangci-lint run ./...
	@echo "$(GREEN)✓ Linting complete$(RESET)"

##@ Cleanup

clean: ## Remove build artifacts
	@rm -f tidepool
	@go clean
	@echo "$(GREEN)✓ Clean complete$(RESET)"
