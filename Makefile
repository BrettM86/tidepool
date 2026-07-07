.PHONY: help build run test test-db-up test-db-down db-migrate db-migrate-down dev-up dev-down lint fmt fmt-check clean

.DEFAULT_GOAL := help

CYAN := \033[36m
RESET := \033[0m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m

COMPOSE := docker compose -f docker-compose.dev.yml

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

dev-down: ## Stop all dev services (including the test database)
	@echo "$(YELLOW)Stopping Tidepool dev stack...$(RESET)"
	@$(COMPOSE) --profile test down --remove-orphans
	@echo "$(GREEN)✓ Stopped$(RESET)"

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

##@ Code Quality

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
