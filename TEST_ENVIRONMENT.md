# Test Environment

This document describes the isolated test environment setup for running tests completely independently from the production environment.

## Overview

The test environment uses a **separate Docker Compose file** (`docker-compose.test.yml`) that spins up its own isolated containers with different ports and names to avoid any conflicts with the production environment.

## Quick Start

```bash
# Start the isolated test environment
make start-test

# Run tests against the isolated environment
make test-test

# Stop the test environment
make stop-test

# Clean up (removes containers and volumes)
make clean-test
```

## Architecture

### Test Environment vs Production Environment

| Service | Production | Test Environment |
|---------|-----------|------------------|
| PostgreSQL | `localhost:5432` | `localhost:5433` |
| Kafka | `localhost:9092` | `localhost:9093` |
| Zookeeper | `localhost:2181` | `localhost:2182` |
| Schema Registry | `localhost:8081` | `localhost:8082` |
| Grafana | `localhost:3000` | `localhost:3001` |
| Database Name | `ev_coorp` | `ev_coorp_test` |
| Container Prefix | `ev_coorp_*` | `*_test` |
| Docker Network | `default` | `test-network` |

### Test-Specific Resources

The test environment uses the same resource names with `_test` suffix:
- Kafka topics: `ocpp.messages_test`, `ocpp.active_test`, `ocpp.active.raw_test`
- PostgreSQL table: `ocpp_history_test`

## Docker Compose Files

### `docker-compose.yml` (Production)
- Standard services on default ports
- Used for development and production testing
- Database: `ev_coorp`

### `docker-compose.test.yml` (Isolated Test)
- Separate services on different ports
- Completely isolated from production
- Database: `ev_coorp_test`
- Uses a separate Docker network (`test-network`)

## Configuration

### Environment Variables for Tests

When running tests against the isolated environment, the following environment variables are used:

```python
# Kafka
TEST_KAFKA_BROKER = "localhost:9093"

# PostgreSQL
TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5433/ev_coorp_test"

# Schema Registry
TEST_SCHEMA_REGISTRY_URL = "http://localhost:8082"
```

See `tests/test_config.py` for the full configuration.

### Spark Pipeline Configuration

When running the Spark pipeline against the test environment, use these environment variables:

```bash
KAFKA_BROKER=localhost:9093 \
POSTGRES_URL=jdbc:postgresql://localhost:5433/ev_coorp_test \
POSTGRES_USER=ev_user \
POSTGRES_PASSWORD=ev_password \
uv run python spark/scripts/spark_kafka_to_postgres.py
```

## Usage Scenarios

### Scenario 1: Run Tests in Isolation

```bash
# Start fresh test environment
make clean-test
make start-test

# Run tests
make test-test

# Clean up when done
make clean-test
```

### Scenario 2: Development with Both Environments

```bash
# Start production environment
make start
make init

# In another terminal, start test environment
make start-test

# Run tests in isolation
make test-test

# Both environments run independently
```

### Scenario 3: CI/CD Pipeline

```bash
# In your CI script:
make clean-test
make start-test
sleep 30  # Wait for services
make test-test
make clean-test
```

## Benefits

1. **Complete Isolation**: Test environment is completely separate from production
2. **No Conflicts**: Different ports, container names, and networks
3. **Parallel Development**: Can run production and test environments simultaneously
4. **Clean State**: Each test run can start with a fresh environment
5. **CI/CD Friendly**: Perfect for automated testing pipelines
6. **Same Code**: Uses the same Docker images, just different configurations

## Implementation Details

### Container Names

Test environment containers have `-test` suffix:
- `ev_coorp_postgres_test` (instead of `ev_coorp_postgres`)
- `kafka_test` (instead of `kafka`)
- `zookeeper_test` (instead of `zookeeper`)
- `schema_registry_test` (instead of `schema-registry`)
- `spark_test` (instead of `spark`)
- `grafana_test` (instead of `grafana`)

### Port Mapping

All test services are mapped to different host ports to avoid conflicts:
- PostgreSQL: `5433:5432`
- Kafka: `9093:9092`, `29093:29092`
- Zookeeper: `2182:2181`
- Schema Registry: `8082:8081`
- Grafana: `3001:3000`
- Spark UI: `4041:4040`

### Network

Test environment uses its own Docker network (`test-network`) to ensure complete isolation.

## Modifying the Test Environment

To modify the test environment configuration:

1. Edit `docker-compose.test.yml` for service configurations
2. Edit `tests/test_config.py` for connection strings and settings
3. Update the `test-test` target in `Makefile` if needed

## Troubleshooting

### Port Already in Use

If you get port conflicts, make sure the production environment is stopped:
```bash
make stop
make stop-test
```

### Connection Refused

Make sure the test environment is running:
```bash
docker compose -f docker-compose.test.yml ps
```

### Tests Failing

Check that the test environment is healthy:
```bash
# Check PostgreSQL
PGPASSWORD=ev_password psql -h localhost -p 5433 -U ev_user -d ev_coorp_test -c "SELECT 1"

# Check Kafka
uv run python -c "from confluent_kafka.admin import AdminClient; a=AdminClient({'bootstrap.servers':'localhost:9093'}); print(list(a.list_topics(timeout=5).topics.keys()))"
```

## Cleanup

To completely remove the test environment:
```bash
make clean-test
```

This removes:
- All test containers
- All test volumes (data is persistend in volumes between runs)

To remove volumes as well:
```bash
docker compose -f docker-compose.test.yml down -v
```

## Future Enhancements

1. **Test-specific Grafana provisioning**: Configure Grafana data sources for the test environment
2. **Test data seeding**: Pre-load test data into the test environment
3. **Health checks**: Add better health check commands
4. **Docker Compose extends**: Use `extends` in `docker-compose.test.yml` to inherit from main file (Docker Compose v2.4+)
