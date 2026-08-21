# Tests

This directory contains tests following the TDD (Test-Driven Development) workflow from `plan.md`.

## Test Structure

```
tests/
├── __init__.py
├── README.md                    # This file
├── conftest.py                 # Pytest fixtures (auto-creates test infrastructure)
├── setup_test_infra.py          # Manual test infrastructure setup script
├── test_docker_setup.py         # Phase 0: Verify Docker infrastructure exists
├── e2e/
│   └── test_full_flow.py        # Phase 4: End-to-end session lifecycle tests
├── fixtures/
│   └── ocpp_messages.py         # Test message generators
├── kafka/
│   ├── __init__.py
│   ├── test_topics.py           # Phase 1: Kafka topic creation tests
│   └── test_tombstones.py       # Phase 1.2: Tombstone behavior tests
├── postgres/
│   ├── __init__.py
│   └── test_schema.py           # Phase 2: PostgreSQL schema tests
└── spark/
    ├── __init__.py
    ├── test_parsers.py           # Phase 3.1: Parsing function tests
    ├── test_pipeline.py          # Phase 3.2: Pipeline integration tests
    └── test_data_integrity.py    # Phase 3.3: Data integrity tests
```

## TDD Workflow

### 1. Write Tests First (RED)
All test files are written to **fail initially** when the required infrastructure doesn't exist.

### 2. Run Tests - They Fail (RED)
```bash
# Run all tests
pytest tests/ -v

# Run specific phase tests
pytest tests/kafka/test_topics.py -v           # Phase 1: Kafka topics
pytest tests/postgres/test_schema.py -v     # Phase 2: PostgreSQL schema
pytest tests/spark/test_parsers.py -v        # Phase 3.1: Parsers
pytest tests/spark/test_pipeline.py -v       # Phase 3.2: Pipeline
pytest tests/spark/test_data_integrity.py -v # Phase 3.3: Data integrity
pytest tests/e2e/test_full_flow.py -v        # Phase 4: E2E
```

### 3. Create Infrastructure (GREEN)
```bash
# Start Docker services
docker compose down
docker compose up -d --build

# Create Kafka topics (Phase 1)
uv run python kafka/scripts/create_topics.py

# Create PostgreSQL tables (Phase 2)
uv run python postgres/scripts/init_db.py

# Start Spark pipeline (Phase 3)
uv run python spark/scripts/spark_kafka_to_postgres.py &
```

### 4. Run Tests Again - They Pass (GREEN)
```bash
pytest tests/ -v
```

## Test Execution Order

Follow the phased approach from `plan.md`:

### Phase 0: Infrastructure Verification
```bash
pytest tests/test_docker_setup.py -v
```
Tests that Docker containers are running and test infrastructure (topics/tables with _test suffix) exists.

### Phase 1: Kafka Topics
```bash
pytest tests/kafka/test_topics.py -v
pytest tests/kafka/test_tombstones.py -v
```
Tests that test Kafka topics (`ocpp.active_test`, `ocpp.active.raw_test`) exist with correct configurations and tombstone behavior.

**Fails until**: Docker containers are running (conftest.py auto-creates test topics)

### Phase 2: PostgreSQL Schema
```bash
pytest tests/postgres/test_schema.py -v
```
Tests that test PostgreSQL table (`ocpp_history_test`) exists with all required fields and indexes.

**Fails until**: Docker containers are running (conftest.py auto-creates test table)

### Phase 3: Spark Pipeline
```bash
pytest tests/spark/test_parsers.py -v        # Parsing functions
pytest tests/spark/test_pipeline.py -v       # Pipeline integration
pytest tests/spark/test_data_integrity.py -v # Data validation
```
Tests parsing functions, pipeline branches, and data integrity.

**Note**: Parsing tests pass immediately. Integration and data integrity tests need data in test tables.

### Phase 4: End-to-End
```bash
pytest tests/e2e/test_full_flow.py -v
```
Tests complete session lifecycle from StartTransaction to StopTransaction using test infrastructure.

**Note**: E2E tests require the Spark pipeline to be running and processing messages.

## Test Infrastructure

### Test Infrastructure (Used by all test files)
All tests now use **_test** suffixed resources that are auto-created by `conftest.py`:
- Kafka topics: `ocpp.messages_test`, `ocpp.active_test`, `ocpp.active.raw_test`
- PostgreSQL table: `ocpp_history_test`

The test infrastructure is created automatically when running pytest, thanks to the `setup_test_infrastructure` fixture in `conftest.py` with `scope="session", autouse=True`.

**Note**: Tests will FAIL initially if Docker containers are not running, then PASS once `conftest.py` auto-creates the test infrastructure.

## Running Specific Tests

```bash
# Run all tests
pytest tests/ -v

# Run tests for a specific phase
pytest tests/kafka/ -v           # Phase 1: All Kafka tests
pytest tests/postgres/ -v      # Phase 2: All PostgreSQL tests
pytest tests/spark/ -v          # Phase 3: All Spark tests
pytest tests/e2e/ -v            # Phase 4: All E2E tests

# Run a specific test file
pytest tests/kafka/test_topics.py -v

# Run a specific test
pytest tests/kafka/test_topics.py::TestKafkaTopicCreation::test_ocpp_active_topic_exists -v
```

## Notes

- **All tests now use `_test` suffixed resources** (e.g., `ocpp.active_test`, `ocpp.active.raw_test`, `ocpp_history_test`) that are auto-created by `conftest.py`.
- Tests will FAIL initially if Docker containers are not running.
- Once Docker is running, `conftest.py` automatically creates all test infrastructure before tests execute.
- E2E tests have sleep timers to allow Spark streaming to process messages - these may need adjustment based on your system speed.
- All test topics now use the `_test` suffix (`ocpp.messages_test`, `ocpp.active_test`, `ocpp.active.raw_test`) for complete isolation from production topics.
