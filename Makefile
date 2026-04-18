.PHONY: help up down seed run run-backfill run-full run-dry test lint clean

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start PostgreSQL + MongoDB containers
	docker compose up -d postgres mongodb
	@echo "Waiting for databases to be healthy..."
	@sleep 5
	@echo "Databases ready."

down: ## Stop all containers
	docker compose down

seed: ## Generate sample data in PostgreSQL
	python -m src.seed

run: ## Run pipeline in incremental mode (default)
	python -m src.pipeline

run-backfill: ## Run pipeline in backfill mode (use START_DATE and END_DATE env vars)
	python -m src.pipeline --mode backfill --start-date $(START_DATE) --end-date $(END_DATE)

run-full: ## Run pipeline in full rebuild mode
	python -m src.pipeline --mode full

run-dry: ## Dry run — extract + transform only, skip load
	python -m src.pipeline --dry-run

test: ## Run all tests
	pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report
	pytest tests/ -v --tb=short --cov=src --cov-report=term-missing

lint: ## Run linter and type checker
	ruff check src/ tests/
	mypy src/

clean: ## Remove generated files and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .mypy_cache -exec rm -rf {} + 2>/dev/null || true
