# Scripts for Data Engineering Challenge

This directory contains utility scripts for managing the Data Engineering Challenge infrastructure.

## Quick Start

### Option 1: Using Make (Recommended)

```bash
# Show available commands
make help

# Full initialization (Docker + DB + Kafka + sample data)
make init

# Full initialization + run all tests
make init-test

# Start services only
make start

# Stop services
make stop

# Run all tests (after initialization)
make test

# Run specific test categories
make test-spark
make test-kafka
make test-postgres
make test-e2e
```

### Option 2: Using Bash Script

```bash
# Show help
./scripts/init_all.sh --help

# Full initialization
./scripts/init_all.sh

# Initialization without sample data (faster)
./scripts/init_all.sh --light

# Full initialization + run tests
./scripts/init_all.sh --test

# Shutdown only
./scripts/init_all.sh --down
```

### Option 3: Using Python Script

```bash
# Full initialization
python scripts/init_all.py

# Initialization without sample data
python scripts/init_all.py --light

# Full initialization + run tests
python scripts/init_all.py --test

# Shutdown only
python scripts/init_all.py --down
```

## Manual Initialization

If you prefer to initialize components manually:

### 1. Start Docker Services

```bash
cd /path/to/data-engineering-challenge
docker compose down
docker compose up -d postgres zookeeper kafka schema-registry
```

Wait for services to be ready (approximately 30-60 seconds).

### 2. Initialize PostgreSQL Database

```bash
uv run python postgres/scripts/init_db.py
```

This creates:
- The `ocpp` schema
- The `ocpp.history` table
- The `ocpp.history_test` table (for tests)
- Indexes for performance

### 3. Initialize Kafka Topics and Schema

```bash
uv run python kafka/scripts/create_topics.py
```

This creates:
- `ocpp.messages` topic (for raw messages)
- `ocpp.active.raw` topic (compacted, for active sessions)
- `ocpp.active` topic (compacted, for session state)
- AVRO schema for message serialization

### 4. (Optional) Generate Sample Data

```bash
# Produce sample messages to Kafka
uv run python kafka/scripts/ocpp_producer.py

# Consume messages from Kafka to PostgreSQL (run in background)
uv run python postgres/scripts/kafka_to_postgres.py &
```

### 5. Run Tests

```bash
# Run all tests
uv run pytest tests/ spark/scripts/test_spark_kafka_to_postgres.py -v

# Run specific test modules
uv run pytest tests/spark/ -v
uv run pytest tests/kafka/ -v
uv run pytest tests/postgres/ -v
uv run pytest tests/e2e/ -v
```

## Troubleshooting

### Docker Services Not Starting

1. Check if Docker is running: `docker --version`
2. Check container status: `docker compose ps`
3. Check logs: `docker compose logs`
4. Try cleaning up: `docker compose down -v` then `docker compose up -d`

### PostgreSQL Connection Issues

1. Verify PostgreSQL is running: `docker compose ps | grep postgres`
2. Test connection: `PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "SELECT 1"`
3. If connection fails, wait a bit longer and retry

### Kafka Connection Issues

1. Verify Kafka is running: `docker compose ps | grep kafka`
2. Test connection: `uv run python -c "from confluent_kafka.admin import AdminClient; print(AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5))"`
3. Verify Zookeeper is running: `docker compose ps | grep zookeeper`

### Port Conflicts

If you get port conflict errors:
- PostgreSQL: 5432
- Zookeeper: 2181
- Kafka: 9092
- Schema Registry: 8081

Stop any existing services using these ports, or modify the `docker-compose.yml` to use different ports.

## Test Categories

### Unit Tests (No Infrastructure Required)
- `tests/spark/test_parsers.py` - Spark parsing functions
- `tests/spark/test_pipeline.py::TestSparkParsersIntegration` - Parser imports
- `tests/kafka/test_ocpp_producer_detailed.py` - Kafka producer parsing
- `tests/kafka/test_create_topics_detailed.py` - Topic configurations
- `tests/postgres/test_kafka_to_postgres.py` - PostgreSQL consumer functions
- `tests/postgres/test_init_db.py` - Database initialization functions
- `spark/scripts/test_spark_kafka_to_postgres.py` - Spark script unit tests

These tests can run without Docker services.

### Integration Tests (Require Infrastructure)
- `tests/spark/test_pipeline.py::TestKafkaProducers` - Kafka producer tests
- `tests/spark/test_pipeline.py::TestPostgresIntegration` - PostgreSQL table tests
- `tests/spark/test_data_integrity.py` - Data integrity tests
- `tests/postgres/test_schema.py` - PostgreSQL schema tests
- `tests/postgres/test_crud_sqlmodel.py` - CRUD operations
- `tests/postgres/test_production_schema.py` - Production schema tests
- `tests/kafka/test_consumer.py` - Kafka consumer tests
- `tests/kafka/test_producer_consumer.py` - Producer/consumer integration
- `tests/kafka/test_tombstones.py` - Tombstone handling
- `tests/kafka/test_topics.py` - Topic verification
- `tests/e2e/test_full_flow.py` - End-to-end flow tests

These tests require Docker services to be running.

## Cleanup

### Stopping Infrastructure

```bash
# Stop and remove containers
make clean

# Or manually
docker compose down -v
```

### Data Cleanup (Keep Containers Running)

The `cleanup.py` script cleans up data from PostgreSQL tables and Kafka topics **without stopping the containers**:

**Main Project Environment (default):**
- PostgreSQL tables: `ocpp.history`, `charger_session`
- Kafka topics: `ocpp.messages`, `ocpp.messages_test`, `ocpp.active.raw`, `ocpp.active`

**Test Environment:**
- PostgreSQL tables: `ocpp_history_test`
- Kafka topics: `ocpp.active_test`, `ocpp.active.raw_test`

**Usage:**
```bash
# Clean up main project environment (default)
python scripts/cleanup.py
python scripts/cleanup.py --main
./scripts/cleanup.sh

# Clean up test environment
python scripts/cleanup.py --test
./scripts/cleanup.sh --test

# Clean up both environments
python scripts/cleanup.py --all
./scripts/cleanup.sh --all

# Show help
python scripts/cleanup.py --help
./scripts/cleanup.sh --help
```

**Makefile targets:**
```bash
make cleanup           # Clean up main project environment
make cleanup-test      # Clean up test environment
make cleanup-main      # Clean up main project environment
make cleanup-all       # Clean up both environments
```

## Infrastructure Requirements

- Docker and Docker Compose
- Python 3.12+
- uv (Python package manager)
- netcat (for port checking: `nc`)

## Notes

- The initialization scripts start only the required services (PostgreSQL, Kafka, Zookeeper, Schema Registry)
- Grafana is defined in docker-compose.yml but not required for tests
- The Spark container is also defined but not started by default (tests use local Spark)
- Sample data generation can take several seconds
- For E2E tests to pass, data must be processed through the pipeline first
