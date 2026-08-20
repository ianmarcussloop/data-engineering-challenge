# =============================================================================
# Makefile for Data Engineering Challenge
# 
# Usage:
#   make help              # Show this help
#   make init              # Full initialization (Docker + DB + Kafka + tests)
#   make init-light        # Initialization without sample data
#   make start             # Start Docker services only
#   make stop              # Stop Docker services
#   make db-init           # Initialize database
#   make kafka-init        # Initialize Kafka topics and schema
#   make test              # Run all tests
#   make test-unit         # Run unit tests only
#   make test-spark        # Run Spark tests
#   make test-kafka        # Run Kafka tests
#   make test-postgres     # Run PostgreSQL tests
#   make clean             # Stop and remove containers
#   make cleanup           # Clean up main project environment (truncate DB + empty Kafka)
#   make cleanup-test      # Clean up test environment
#   make cleanup-main      # Clean up main project environment
#   make cleanup-all       # Clean up both environments
# =============================================================================

.PHONY: help init init-light start stop db-init kafka-init test test-unit test-spark test-kafka test-postgres clean cleanup cleanup-test cleanup-main cleanup-all

# Project directory
PROJECT_DIR := $(shell pwd)
SCRIPTS_DIR := $(PROJECT_DIR)/scripts

# Default target
help:
	@echo "Data Engineering Challenge - Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Initialization:"
	@echo "  init              Full initialization (Docker + DB + Kafka + sample data)"
	@echo "  init-light        Initialization without sample data"
	@echo "  init-test         Full initialization + run all tests"
	@echo ""
	@echo "Docker:"
	@echo "  start             Start Docker services"
	@echo "  stop              Stop Docker services"
	@echo "  clean             Stop and remove containers"
	@echo ""
	@echo "Database:"
	@echo "  db-init           Initialize PostgreSQL database"
	@echo "  db-shell          Open PostgreSQL shell"
	@echo ""
	@echo "Kafka:"
	@echo "  kafka-init        Initialize Kafka topics and schema"
	@echo "  kafka-topics      List Kafka topics"
	@echo ""
	@echo "Tests:"
	@echo "  test              Run all tests"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-spark        Run Spark tests"
	@echo "  test-kafka        Run Kafka tests"
	@echo "  test-postgres     Run PostgreSQL tests"
	@echo "  test-e2e          Run end-to-end tests"
	@echo ""
	@echo "Data:"
	@echo "  produce-data      Produce sample data to Kafka"
	@echo "  consume-data      Consume messages from Kafka to PostgreSQL"
	@echo ""
	@echo "Cleanup:"
	@echo "  cleanup           Clean up main project environment (truncate DB + empty Kafka)"
	@echo "  cleanup-test      Clean up test environment"
	@echo "  cleanup-main      Clean up main project environment"
	@echo "  cleanup-all       Clean up both environments"

# =============================================================================
# Initialization
# =============================================================================

init:
	@echo "=== Running full initialization ==="
	$(MAKE) start
	@sleep 5
	$(MAKE) db-init
	$(MAKE) kafka-init
	$(MAKE) produce-data
	@echo "Waiting for data to be processed..."
	@sleep 10
	@echo "=== Initialization complete ==="

init-light:
	@echo "=== Running light initialization (no sample data) ==="
	$(MAKE) start
	@sleep 5
	$(MAKE) db-init
	$(MAKE) kafka-init
	@echo "=== Light initialization complete ==="

init-test:
	$(MAKE) init
	$(MAKE) test

# =============================================================================
# Docker
# =============================================================================

start:
	@echo "=== Starting Docker services ==="
	cd $(PROJECT_DIR) && docker compose up -d postgres zookeeper kafka schema-registry
	@echo "Waiting for services to start..."
	@sleep 30
	@echo "=== Services started ==="

stop:
	@echo "=== Stopping Docker services ==="
	cd $(PROJECT_DIR) && docker compose down

clean:
	@echo "=== Cleaning up ==="
	cd $(PROJECT_DIR) && docker compose down -v

# =============================================================================
# Cleanup (Data Cleanup - keeps containers running)
# =============================================================================

cleanup:
	@echo "=== Cleaning up main project environment ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --main

cleanup-test:
	@echo "=== Cleaning up test environment ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --test

cleanup-main:
	@echo "=== Cleaning up main project environment ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --main

cleanup-all:
	@echo "=== Cleaning up both environments ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --all

# =============================================================================
# Database
# =============================================================================

db-init:
	@echo "=== Initializing database ==="
	cd $(PROJECT_DIR) && uv run python postgres/scripts/init_db.py

_db-shell:
	@echo "=== Opening PostgreSQL shell ==="
	PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp

# =============================================================================
# Kafka
# =============================================================================

kafka-init:
	@echo "=== Initializing Kafka ==="
	cd $(PROJECT_DIR) && uv run python kafka/scripts/create_topics.py

kafka-topics:
	@echo "=== Kafka Topics ==="
	@cd $(PROJECT_DIR) && uv run python -c "from confluent_kafka.admin import AdminClient; a=AdminClient({'bootstrap.servers':'localhost:9092'}); print([t for t in a.list_topics(timeout=5).topics])"

# =============================================================================
# Data
# =============================================================================

produce-data:
	@echo "=== Producing sample data ==="
	cd $(PROJECT_DIR) && uv run python kafka/scripts/ocpp_producer.py

consume-data:
	@echo "=== Consuming data to PostgreSQL ==="
	@echo "Press Ctrl+C to stop"
	cd $(PROJECT_DIR) && uv run python postgres/scripts/kafka_to_postgres.py

# =============================================================================
# Tests
# =============================================================================

TEST_FILES := tests/ spark/scripts/test_spark_kafka_to_postgres.py

test:
	@echo "=== Running all tests ==="
	cd $(PROJECT_DIR) && uv run pytest $(TEST_FILES) -v --tb=short

test-unit:
	@echo "=== Running unit tests only ==="
	cd $(PROJECT_DIR) && uv run pytest \
		tests/spark/test_parsers.py \
		tests/spark/test_pipeline.py::TestSparkParsersIntegration \
		tests/kafka/test_ocpp_producer_detailed.py \
		tests/kafka/test_create_topics_detailed.py \
		tests/kafka/test_create_topics_script.py \
		tests/postgres/test_kafka_to_postgres.py \
		tests/postgres/test_init_db.py \
		tests/postgres/test_schema.py \
		spark/scripts/test_spark_kafka_to_postgres.py \
		-v

test-spark:
	@echo "=== Running Spark tests ==="
	cd $(PROJECT_DIR) && uv run pytest tests/spark/ spark/scripts/test_spark_kafka_to_postgres.py -v

test-kafka:
	@echo "=== Running Kafka tests ==="
	cd $(PROJECT_DIR) && uv run pytest tests/kafka/ -v

test-postgres:
	@echo "=== Running PostgreSQL tests ==="
	cd $(PROJECT_DIR) && uv run pytest tests/postgres/ -v

test-e2e:
	@echo "=== Running end-to-end tests ==="
	cd $(PROJECT_DIR) && uv run pytest tests/e2e/ -v

# =============================================================================
# Utility
# =============================================================================

check-services:
	@echo "=== Checking Services ==="
	@echo "PostgreSQL:"
	@PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "SELECT version()" || echo "  Not available"
	@echo "Kafka:"
	@cd $(PROJECT_DIR) && uv run python -c "from confluent_kafka.admin import AdminClient; a=AdminClient({'bootstrap.servers':'localhost:9092'}); print('  Topics:', list(a.list_topics(timeout=5).topics.keys()))" || echo "  Not available"
	@echo "Schema Registry:"
	@curl -s http://localhost:8081/subjects || echo "  Not available"

logs:
	@echo "=== Docker Container Logs ==="
	cd $(PROJECT_DIR) && docker compose logs -f

ps:
	@echo "=== Docker Container Status ==="
	cd $(PROJECT_DIR) && docker compose ps
