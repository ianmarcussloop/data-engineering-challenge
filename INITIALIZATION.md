# Initialization Guide

This guide explains how to initialize the Data Engineering Challenge infrastructure to get all tests passing.

## Overview

The project requires the following infrastructure to be running for all tests to pass:

1. **PostgreSQL** (port 5432) - Database for session history
2. **Kafka** (port 9092) - Message queue for OCPP messages
3. **Zookeeper** (port 2181) - Required by Kafka
4. **Schema Registry** (port 8081) - For Avro schema management

## Quick Start (Recommended)

### Using Make

```bash
# Initialize everything and run tests
make init-test
```

This will:
- Start Docker services
- Initialize the database
- Initialize Kafka topics
- Generate sample data
- Run all tests

### Using Script

```bash
# Initialize everything and run tests
./scripts/init_all.sh --test

# Or with Python
python scripts/init_all.py --test
```

## Step-by-Step Initialization

### 1. Start Docker Services

```bash
cd /Users/iansloop/data-engineering-challenge

# Stop any existing containers
docker compose down

# Start required services (takes ~30-60 seconds)
docker compose up -d postgres zookeeper kafka schema-registry

# Verify services are running
docker compose ps
```

**Expected output:**
```
NAME                  COMMAND                  SERVICE             STATUS              PORTS
kafka                 "/etc/confluent/docker…"   kafka              running             0.0.0.0:9092->9092/tcp, :::9092->9092/tcp, 0.0.0.0:29092->29092/tcp, :::29092->29092/tcp, 9093/tcp
ev_coorp_postgres     "docker-entrypoint.s…"   postgres           running             0.0.0.0:5432->5432/tcp, :::5432->5432/tcp
schema-registry       "/etc/confluent/docker…"   schema-registry    running             0.0.0.0:8081->8081/tcp, :::8081->8081/tcp
zookeeper             "/docker-entrypoint.…"   zookeeper          running             0.0.0.0:2181->2181/tcp, :::2181->2181/tcp
```

### 2. Initialize PostgreSQL Database

```bash
# Create tables, schema, and indexes
uv run python postgres/scripts/init_db.py
```

**Expected output:**
```
ocpp schema created successfully
ocpp.history table created successfully
Tables created successfully!
```

This creates:
- `ocpp` schema
- `ocpp.history` table (production)
- `ocpp.history_test` table (for tests)
- Indexes on key columns

### 3. Initialize Kafka Topics and Schema

```bash
# Create topics and register Avro schema
uv run python kafka/scripts/create_topics.py
```

**Expected output:**
```
🚀 Initializing Kafka topic and schema...
Topic ocpp.messages already exists.
Created Kafka topic: ocpp.active.raw
Created Kafka topic: ocpp.active
✅ Registered schema for ocpp.messages-value (ID: 1)
✨ Done!
```

This creates:
- `ocpp.messages` - Raw OCPP messages
- `ocpp.active.raw` - Normalized active session messages (compacted)
- `ocpp.active` - Session state (compacted)
- Avro schema for message serialization

### 4. (Optional) Generate Sample Data

For **data integrity tests** and **E2E tests** to pass, you need data in the system:

```bash
# Produce sample messages to Kafka
uv run python kafka/scripts/ocpp_producer.py
```

**Expected output:**
```
📂 Parsing .txt file and publishing to Kafka...
📋 Parsed 100 messages from ocpp-sample-data.txt.
✅ Published: {'chargerId': 'charger6', 'uniqueId': 'ef51a638-0e05-4a9d-be52-594ada28f153', 'message': '[2, "ef51a638-0e05-4a9d-be52-594ada28f153", "MeterValues", {...}]'}
...
✨ All messages published!
```

Then process the messages to PostgreSQL:

```bash
# Run consumer in background
uv run python postgres/scripts/kafka_to_postgres.py &
KAFKA_CONSUMER_PID=$!

# Wait for processing
sleep 10

# Stop consumer
kill $KAFKA_CONSUMER_PID
```

Alternatively, use the Make command:
```bash
make produce-data
# Then manually process, or use:
make consume-data  # (run in separate terminal, Ctrl+C to stop)
```

### 5. Run All Tests

```bash
# Run all tests
make test

# Or explicitly
uv run pytest tests/ spark/scripts/test_spark_kafka_to_postgres.py -v
```

## Expected Test Results

After full initialization with sample data:

| Category | Status | Count | Notes |
|----------|--------|-------|-------|
| Spark unit tests | ✅ PASS | ~65 | No infrastructure needed |
| Kafka unit tests | ✅ PASS | ~20 | No infrastructure needed |
| PostgreSQL unit tests | ✅ PASS | ~15 | No infrastructure needed |
| Spark integration tests | ✅ PASS | ~3 | Requires Kafka |
| PostgreSQL integration tests | ✅ PASS | ~5 | Requires PostgreSQL |
| Data integrity tests | ✅ PASS | ~4 | Requires data in DB |
| E2E tests | ✅ PASS | ~3 | Requires full pipeline |
| **Total** | **✅ PASS** | **~196** | All tests passing |

## Minimal Initialization (For Unit Tests Only)

If you only want to run unit tests (which don't require infrastructure):

```bash
# Just run unit tests (no Docker needed)
make test-unit

# Or explicitly
uv run pytest \
    tests/spark/test_parsers.py \
    tests/spark/test_pipeline.py::TestSparkParsersIntegration \
    tests/kafka/test_ocpp_producer_detailed.py \
    tests/kafka/test_create_topics_detailed.py \
    tests/postgres/test_kafka_to_postgres.py \
    tests/postgres/test_init_db.py \
    spark/scripts/test_spark_kafka_to_postgres.py \
    -v
```

This will run **~100+ unit tests** without any infrastructure.

## Cleanup

```bash
# Stop and remove all containers
make clean

# Or manually
docker compose down -v
```

## Troubleshooting

### Issue: PostgreSQL connection refused

**Solution:**
```bash
# Check if PostgreSQL container is running
docker compose ps

# Wait longer (TimescaleDB can take a while)
sleep 60

# Test connection manually
PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "SELECT 1"
```

### Issue: Kafka connection errors

**Solution:**
```bash
# Check if all Kafka services are running
docker compose ps

# Verify Zookeeper is running (Kafka depends on it)
docker compose logs zookeeper

# Test Kafka connection
uv run python -c "from confluent_kafka.admin import AdminClient; print(AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5))"
```

### Issue: Tests still failing after initialization

**Common causes:**

1. **Services not fully ready** - Wait longer (up to 2 minutes) for all services to be healthy
2. **Sample data not processed** - For data integrity tests, ensure messages are processed to PostgreSQL
3. **Port conflicts** - Check if other services are using ports 5432, 9092, 2181, 8081
4. **Test table missing** - Run `uv run python postgres/scripts/init_db.py` to create test tables

**Debug steps:**
```bash
# Check all services
make check-services

# View Docker logs
make logs

# Run specific failing test with verbose output
uv run pytest tests/spark/test_data_integrity.py::TestDataIntegrityFields::test_session_has_all_required_fields -v -s
```

### Issue: "No module named 'confluent_kafka'"

**Solution:**
```bash
# Ensure you're using the virtual environment
source .venv/bin/activate

# Or use uv directly
uv run python your_script.py
```

### Issue: Docker compose command not found

**Solution:**
```bash
# Try docker-compose (with hyphen)
docker-compose up -d

# Or install Docker Compose separately
```

## Verification Commands

Check that everything is initialized correctly:

```bash
# Check Docker containers
make ps

# Check database tables
PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "\dt ocpp.*"

# Check Kafka topics
make kafka-topics

# Check Schema Registry
curl http://localhost:8081/subjects
```

## Summary

| Step | Command | Required For | Time |
|------|---------|--------------|------|
| 1 | `docker compose up -d postgres zookeeper kafka schema-registry` | All integration tests | ~1 min |
| 2 | `uv run python postgres/scripts/init_db.py` | PostgreSQL tests | ~5 sec |
| 3 | `uv run python kafka/scripts/create_topics.py` | Kafka tests | ~5 sec |
| 4 | `make produce-data && sleep 10` | Data integrity tests | ~10 sec |
| 5 | `make test` | All tests | ~2 min |
| **Total** | | **All tests passing** | **~3-4 min** |

Use `make init-test` to automate all of these steps.
