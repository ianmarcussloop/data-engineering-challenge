"""Pytest configuration and fixtures for the test suite."""

import pytest
import os
import subprocess
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import psycopg2
from sqlalchemy import text, create_engine


# Test infrastructure configuration
TEST_KAFKA_BROKER = "localhost:9092"
TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"

# Test-specific resource names (to avoid conflicts with production)
TEST_TOPICS = ["ocpp.messages_test", "ocpp.active_test", "ocpp.active.raw_test"]
TEST_TABLE = "ocpp.history_test"


@pytest.fixture(scope="session", autouse=True)
def setup_test_infrastructure():
    """Set up test Kafka topics and PostgreSQL tables before tests run."""
    
    # Create test Kafka topics
    admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
    
    test_topic_configs = {
        "ocpp.messages_test": {},
        "ocpp.active_test": {
            "cleanup.policy": "compact",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        },
        "ocpp.active.raw_test": {
            "cleanup.policy": "compact",
            "retention.ms": "259200000",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        }
    }
    
    for topic_name in ["ocpp.messages_test", "ocpp.active_test", "ocpp.active.raw_test"]:
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
        # Check if test table already exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp_history_test'
        """)).fetchone()
        
        if result is None:
            # Create the test table with the same schema as ocpp.history
            conn.execute(text("""
                CREATE TABLE public.ocpp_history_test (
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
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_stationid ON public.ocpp_history_test (stationId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_transactionid ON public.ocpp_history_test (transactionId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_starttime ON public.ocpp_history_test (startTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_endtime ON public.ocpp_history_test (endTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_terminationreason ON public.ocpp_history_test (terminationReason)"))
            
            conn.commit()
            print("Created test PostgreSQL table: ocpp.history_test")
    
    yield
    
    # Teardown - clean up test resources
    # (Optional: we can leave them for now to speed up repeated test runs)


class TestInfrastructure:
    """Helper class to check and create test infrastructure."""
    
    @staticmethod
    def ensure_kafka_topics():
        """Ensure test Kafka topics exist."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        for topic in ["ocpp.active_test", "ocpp.active.raw_test"]:
            if topic not in admin.list_topics(timeout=5).topics:
                raise RuntimeError(f"Test topic {topic} does not exist")
    
    @staticmethod
    def ensure_postgres_table():
        """Ensure test PostgreSQL table exists."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp.history_test'
        """)
        if cursor.fetchone() is None:
            conn.close()
            raise RuntimeError("Test table ocpp.history_test does not exist")
        conn.close()
