"""Phase 0: Test Infrastructure - Verify test resources exist in existing infrastructure."""

import pytest
from confluent_kafka.admin import AdminClient
import psycopg2
import os


# Use existing infrastructure, but respect test environment
TEST_KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")
TEST_POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")


class TestDockerSetup:
    """Test that test infrastructure (topics and tables) exists in the existing Docker environment."""
    
    def test_kafka_running(self):
        """Test that Kafka is running and has the ocpp.messages topic."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.messages" in topics, "ocpp.messages topic should exist in Kafka"
    
    def test_ocpp_active_test_topic_exists(self):
        """Test that ocpp.active topic exists."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active" in topics, "ocpp.active topic should exist"
    
    def test_ocpp_active_raw_test_topic_exists(self):
        """Test that ocpp.active.raw topic exists."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active.raw" in topics, "ocpp.active.raw topic should exist"
    
    def test_postgres_running(self):
        """Test that PostgreSQL is running."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        assert conn is not None
        conn.close()
    
    def test_ocpp_history_table_exists(self):
        """Test that ocpp.history table exists with all required fields."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Check table exists in ocpp schema
        cursor.execute("""
            SELECT "table_name" FROM information_schema.tables 
            WHERE "table_schema" = 'ocpp' AND "table_name" = 'history'
        """)
        result = cursor.fetchone()
        assert result is not None, "ocpp.history table should exist in ocpp schema"
        
        # Check required columns (camelCase as per schema)
        cursor.execute("""
            SELECT "column_name" FROM information_schema.columns 
            WHERE "table_schema" = 'ocpp' AND "table_name" = 'history'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        required_fields = [
            "sessionId", "stationId", "transactionId", "startTime", "endTime",
            "duration", "terminationReason", "totalEnergyConsumed",
            "meterStart", "meterStop", "idTag"
        ]
        for field in required_fields:
            assert field in columns, f"ocpp.history table should have {field} column, has: {columns}"
        
        conn.close()
    
    def test_ocpp_history_indexes(self):
        """Test that ocpp.history table has required indexes."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Get all indexes on ocpp.history in the ocpp schema
        cursor.execute("""
            SELECT "indexname", "indexdef" 
            FROM pg_indexes 
            WHERE "tablename" = 'history' AND "schemaname" = 'ocpp'
        """)
        indexes = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Check required indexes exist (camelCase column names as per schema)
        required_indexes = ["stationId", "transactionId", "startTime", "endTime", "terminationReason"]
        for idx_col in required_indexes:
            found = any(f'("{idx_col}")' in indexdef or f'("{idx_col}",' in indexdef or f', "{idx_col}")' in indexdef 
                       for indexdef in indexes.values())
            assert found, f"ocpp.history should have index on {idx_col}, has: {list(indexes.keys())}"
        
        conn.close()
