# Load .env variables
include .env
export $(shell sed 's/=.*//' .env)

# Default target
.PHONY: help
help:
	@echo "DuckLake Dev Commands:"
	@echo "  make dev       - Start Postgres + MinIO + create-bucket for local development"
	@echo "  make stop      - Stop all running containers"
	@echo "  make logs      - Tail logs from all services"
	@echo "  make reset     - Remove all volumes and containers (DANGEROUS!)"
	@echo "  make python    - Run ducknews_lake/main.py locally with uv"
	@echo "  make watch     - Run ducknews_lake/main.py locally with hot reload"

# Rebuild docker images
.PHONY: build
build:
	docker compose build

# Start infrastructure for local dev
.PHONY: dev
dev:
	docker compose up -d postgres minio create-bucket

# Stop all running containers
.PHONY: stop
stop:
	docker compose down

# Tail logs
.PHONY: logs
logs:
	docker compose logs -f

# Remove all data & containers
.PHONY: reset
reset:
	docker compose down -v

# Run the Python app locally
.PHONY: python
python:
	uv run python ducknews_lake/main.py

# Run the Python app with hot reload (requires watchfiles)
.PHONY: watch
watch:
	uv run watchfiles "python ducknews_lake/main.py" --recursive ducknews_lake/

# Run SQLMesh locally
.PHONY: sqlmesh
sqlmesh:
	uv run sqlmesh --config-file ducknews_lake/sqlmesh/config.yaml run