# =============================================================================
# Makefile for Data Engineering Challenge
# 
# Usage:
#   make help              # Show this help
#   make init              # Full initialization (Docker + DB + Kafka + sample data)
#   make init-light        # Initialization without sample data
#   make start             # Start Docker services only
#   make stop              # Stop Docker services
#   make db-init           # Initialize database
#   make kafka-init        # Initialize Kafka topics and schema
#   make test              # Run all tests in isolated test environment
#   make test-unit         # Run unit tests only
#   make test-spark        # Run Spark tests
#   make test-kafka        # Run Kafka tests
#   make test-postgres     # Run PostgreSQL tests
#   make test-e2e          # Run end-to-end tests
#   make clean             # Stop and remove containers
#   make cleanup           # Clean up main project environment (truncate DB + empty Kafka)
#   make cleanup-main      # Clean up main project environment
#   make cleanup-all       # Clean up both main and shared test environments
#   make start-test        # Start isolated test Docker environment
#   make stop-test         # Stop isolated test Docker environment
#   make clean-test        # Clean up isolated test Docker environment
# =============================================================================

.PHONY: help init init-light start stop start-test stop-test db-init kafka-init test test-unit test-spark test-kafka test-postgres test-e2e test-db-init test-kafka-init produce-data-test consume-data-test consume-active clean cleanup cleanup-main cleanup-all

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
	@echo "  start             Start Docker services (production)"
	@echo "  stop              Stop Docker services"
	@echo "  start-test        Start test Docker services (isolated)"
	@echo "  stop-test         Stop test Docker services"
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
	@echo "  test              Run all tests in isolated test environment"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-spark        Run Spark tests"
	@echo "  test-kafka        Run Kafka tests"
	@echo "  test-postgres     Run PostgreSQL tests"
	@echo "  test-e2e          Run end-to-end tests"
	@echo ""
	@echo "Data:"
	@echo "  produce-data      Produce sample data to Kafka"
	@echo "  consume-data      Consume messages from Kafka to PostgreSQL"
	@echo "  consume-active    Consume ocpp.active messages to PostgreSQL"
	@echo ""
	@echo "Cleanup:"
	@echo "  cleanup           Clean up main project environment (truncate DB + empty Kafka)"
	@echo "  cleanup-main      Clean up main project environment"
	@echo "  cleanup-all       Clean up both main and shared test environments"
	@echo "  clean-test        Clean up isolated test Docker environment"

# =============================================================================
# Initialization
# =============================================================================

init:
	@echo "=== Clean up environment before start ==="
	$(MAKE) clean
	@echo "=== Running full initialization ==="
	$(MAKE) start
	@sleep 5
	$(MAKE) db-init
	$(MAKE) kafka-init
	$(MAKE) produce-data
	$(MAKE) consume-data
	$(MAKE) consume-active
	@echo "Waiting for data to be processed..."
	@sleep 10
	@echo "=== Initialization complete ==="

init-light:
	@echo "=== Running light initialization (no sample data) ==="
	$(MAKE) clean
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
	@echo "=== Starting Docker services (production) ==="
	cd $(PROJECT_DIR) && docker compose --profile production up -d
	@echo "Waiting for services to start..."
	@sleep 10
	@echo "=== Services started ==="

stop:
	@echo "=== Stopping Docker services ==="
	cd $(PROJECT_DIR) && docker compose --profile production down

# =============================================================================
# Test Docker Environment (Isolated)
# =============================================================================

start-test:
	@echo "=== Starting test Docker services (isolated environment) ==="
	cd $(PROJECT_DIR) && docker compose --profile test up -d
	@echo "Waiting for test services to start..."
	@sleep 10
	@echo "=== Test services started ==="

stop-test:
	@echo "=== Stopping test Docker services ==="
	cd $(PROJECT_DIR) && docker compose --profile test down

clean-test:
	@echo "=== Cleaning up test Docker environment ==="
	cd $(PROJECT_DIR) && rm -rf spark-checkpoints-test/
	cd $(PROJECT_DIR) && docker compose --profile test down -v

clean:
	@echo "=== Cleaning up ==="
	cd $(PROJECT_DIR) && rm -rf spark-checkpoints/
	cd $(PROJECT_DIR) && docker compose --profile production down -v

# =============================================================================
# Cleanup (Data Cleanup - keeps containers running)
# =============================================================================

cleanup:
	@echo "=== Cleaning up main project environment ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --main

cleanup-main:
	@echo "=== Cleaning up main project environment ==="
	cd $(PROJECT_DIR) && ./scripts/cleanup.sh --main

cleanup-all:
	@echo "=== Cleaning up both main and shared test environments ==="
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
	cd $(PROJECT_DIR) && SPARK_LOCAL_IP=127.0.0.1 CHECKPOINT_DIR=./spark-checkpoints uv run python spark/scripts/spark_kafka_to_postgres.py

consume-active:
	@echo "=== Consuming ocpp.active data to PostgreSQL ==="
	@echo "Press Ctrl+C to stop"
	cd $(PROJECT_DIR) && uv run python postgres/scripts/kafka_active_to_postgres.py

# =============================================================================
# Tests
# =============================================================================

TEST_FILES := tests/ spark/scripts/test_spark_kafka_to_postgres.py

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
# Test Environment Initialization (mirrors init but for test env)
# =============================================================================

test-db-init:
	@echo "=== Initializing test database ==="
	cd $(PROJECT_DIR) && DATABASE_URL=postgresql://ev_user:ev_password@localhost:5433/ev_coorp_test uv run python postgres/scripts/init_db.py

test-kafka-init:
	@echo "=== Initializing test Kafka topics ==="
	cd $(PROJECT_DIR) && env KAFKA_BROKER=localhost:9093 SCHEMA_REGISTRY_URL=http://localhost:8082 uv run python kafka/scripts/create_topics.py

produce-data-test:
	@echo "=== Producing test data ==="
	cd $(PROJECT_DIR) && KAFKA_BROKER=localhost:9093 uv run python kafka/scripts/ocpp_producer.py

consume-data-test:
	@echo "=== Consuming test data to PostgreSQL ==="
	@echo "Press Ctrl+C to stop"
	cd $(PROJECT_DIR) && SPARK_LOCAL_IP=127.0.0.1 CHECKPOINT_DIR=./spark-checkpoints-test KAFKA_BROKER=localhost:9093 POSTGRES_URL=jdbc:postgresql://localhost:5433/ev_coorp_test POSTGRES_USER=ev_user POSTGRES_PASSWORD=ev_password uv run python spark/scripts/spark_kafka_to_postgres.py

consume-active-test:
	@echo "=== Consuming test ocpp.active data to PostgreSQL ==="
	@echo "Press Ctrl+C to stop"
	cd $(PROJECT_DIR) && KAFKA_BROKER=localhost:9093 KAFKA_TOPIC_ACTIVE=ocpp.active_test DATABASE_URL=postgresql://ev_user:ev_password@localhost:5433/ev_coorp_test uv run python postgres/scripts/kafka_active_to_postgres.py

# =============================================================================
# Isolated Test Environment
# =============================================================================

# Main test target - mirrors init but for test environment
init-test:
	@echo "=== Running tests in isolated Docker environment ==="
	$(MAKE) clean-test
	$(MAKE) start-test
	@sleep 10
	$(MAKE) test-db-init
	$(MAKE) test-kafka-init
	$(MAKE) produce-data-test
	$(MAKE) consume-data-test

test:
	@echo "=== Running all tests ==="
	cd $(PROJECT_DIR) && TEST_KAFKA_BROKER=localhost:9093 TEST_POSTGRES_URL=postgresql://ev_user:ev_password@localhost:5433/ev_coorp_test uv run pytest -v

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
