"""
Test configuration for isolated test environment.

This module provides configuration for tests running against the
separate test Docker Compose environment (docker-compose.test.yml).

Test Environment Ports:
- PostgreSQL: localhost:5433 (instead of 5432)
- Kafka: localhost:9093 (instead of 9092)
- Zookeeper: localhost:2182 (instead of 2181)
- Schema Registry: localhost:8082 (instead of 8081)
- Grafana: localhost:3001 (instead of 3000)

Test Environment Names:
- Database: ev_coorp_test (instead of ev_coorp)
- Kafka topics: ocpp.messages, ocpp.active, ocpp.active.raw
- PostgreSQL table: ocpp_history
"""

# Test environment connection strings
TEST_KAFKA_BROKER = "localhost:9093"
TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5433/ev_coorp_test"
TEST_SCHEMA_REGISTRY_URL = "http://localhost:8082"

# Test-specific resource names
TEST_DATABASE = "ev_coorp_test"
TEST_KAFKA_TOPICS = ["ocpp.messages", "ocpp.active", "ocpp.active.raw"]
TEST_POSTGRES_TABLE = "ocpp_history"

# Container names for health checks
TEST_POSTGRES_CONTAINER = "ev_coorp_postgres_test"
TEST_KAFKA_CONTAINER = "kafka_test"
TEST_ZOOKEEPER_CONTAINER = "zookeeper_test"


def get_test_kafka_config():
    """Get Kafka configuration for test environment."""
    return {
        "bootstrap.servers": TEST_KAFKA_BROKER
    }


def get_test_postgres_config():
    """Get PostgreSQL configuration for test environment."""
    return {
        "host": "localhost",
        "port": 5433,
        "database": TEST_DATABASE,
        "user": "ev_user",
        "password": "ev_password"
    }
