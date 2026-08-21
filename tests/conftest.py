"""Pytest configuration and fixtures for the test suite."""

import pytest
import os
import subprocess
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import psycopg2
from sqlalchemy import text, create_engine


# Test infrastructure configuration - read from environment or use defaults
TEST_KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")
TEST_POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")

# Resource names - same as production since test environment is isolated
TEST_TOPICS = ["ocpp.messages", "ocpp.active", "ocpp.active.raw"]
TEST_TABLE = "ocpp.history"


@pytest.fixture(scope="session", autouse=True)
def setup_test_infrastructure():
    """Set up test Kafka topics and PostgreSQL tables before tests run."""
    
    # Create test Kafka topics
    admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
    
    test_topic_configs = {
        "ocpp.messages": {},
        "ocpp.active": {
            "cleanup.policy": "compact",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        },
        "ocpp.active.raw": {
            "cleanup.policy": "compact",
            "retention.ms": "259200000",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        }
    }
    
    for topic_name in ["ocpp.messages", "ocpp.active", "ocpp.active.raw"]:
        if topic_name not in admin.list_topics(timeout=5).topics:
            new_topic = NewTopic(
                topic_name,
                num_partitions=10,
                replication_factor=1,
                config=test_topic_configs.get(topic_name, {})
            )
            admin.create_topics([new_topic])
            # Wait for topic to be created
            time.sleep(2)
            print(f"Created test Kafka topic: {topic_name}")
    
    # Create test PostgreSQL table
    engine = create_engine(TEST_POSTGRES_URL)
    
    with engine.connect() as conn:
        # Create the ocpp schema if it doesn't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS ocpp"))
        conn.commit()
        
        # Check if table already exists in ocpp schema
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'ocpp' AND table_name = 'history'
        """)).fetchone()
        
        if result is None:
            # Create the table in the ocpp schema to match production
            conn.execute(text("""
                CREATE TABLE ocpp.history (
                    sessionId TEXT PRIMARY KEY,
                    stationId TEXT NOT NULL,
                    transactionId TEXT NOT NULL,
                    startTime TIMESTAMP NOT NULL,
                    endTime TIMESTAMP NOT NULL,
                    duration INTEGER NOT NULL,
                    terminationReason TEXT,
                    totalEnergyConsumed FLOAT,
                    avgPower FLOAT,
                    maxPower FLOAT,
                    idTag TEXT,
                    connectorId INTEGER,
                    meterStart INTEGER,
                    meterStop INTEGER,
                    socStart FLOAT,
                    socEnd FLOAT,
                    voltageAvg FLOAT,
                    eventCount INTEGER DEFAULT 0
                )
            """))
            
            # Create indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_stationid ON ocpp.history (stationId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_transactionid ON ocpp.history (transactionId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_starttime ON ocpp.history (startTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_endtime ON ocpp.history (endTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_terminationreason ON ocpp.history (terminationReason)"))
            
            conn.commit()
            print("Created PostgreSQL table: ocpp.history")
    
    yield
    
    # Teardown - clean up test resources
    # (Optional: we can leave them for now to speed up repeated test runs)


class TestInfrastructure:
    """Helper class to check and create test infrastructure."""
    
    @staticmethod
    def ensure_kafka_topics():
        """Ensure Kafka topics exist."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        for topic in ["ocpp.active", "ocpp.active.raw"]:
            if topic not in admin.list_topics(timeout=5).topics:
                raise RuntimeError(f"Topic {topic} does not exist")
    
    @staticmethod
    def ensure_postgres_table():
        """Ensure PostgreSQL table exists."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'ocpp' AND table_name = 'history'
        """)
        if cursor.fetchone() is None:
            conn.close()
            raise RuntimeError("Table ocpp.history does not exist")
        conn.close()
