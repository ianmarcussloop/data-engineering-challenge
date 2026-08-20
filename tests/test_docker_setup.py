"""Phase 0: Test Infrastructure - Verify test resources exist in existing infrastructure."""

import pytest
from confluent_kafka.admin import AdminClient
import psycopg2


# Use existing infrastructure
TEST_KAFKA_BROKER = "localhost:9092"
TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"


class TestDockerSetup:
    """Test that test infrastructure (topics and tables) exists in the existing Docker environment."""
    
    def test_kafka_running(self):
        """Test that Kafka is running and has the ocpp.messages_test topic."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.messages_test" in topics, "ocpp.messages_test topic should exist in Kafka"
    
    def test_ocpp_active_test_topic_exists(self):
        """Test that ocpp.active_test topic exists."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active_test" in topics, "ocpp.active_test topic should exist"
    
    def test_ocpp_active_raw_test_topic_exists(self):
        """Test that ocpp.active.raw_test topic exists."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active.raw_test" in topics, "ocpp.active.raw_test topic should exist"
    
    def test_postgres_running(self):
        """Test that PostgreSQL is running."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        assert conn is not None
        conn.close()
    
    def test_ocpp_history_test_table_exists(self):
        """Test that ocpp_history_test table exists with all required fields."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Check table exists
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp_history_test'
        """)
        result = cursor.fetchone()
        assert result is not None, "ocpp_history_test table should exist"
        
        # Check required columns (PostgreSQL stores them as lowercase)
        cursor.execute("""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = 'ocpp_history_test'
        """)
        columns = [row[0] for row in cursor.fetchall()]
        
        # PostgreSQL converts to lowercase, so check lowercase versions
        required_fields = [
            "sessionid", "stationid", "transactionid", "starttime", "endtime",
            "duration", "terminationreason", "totalenergyconsumed",
            "meterstart", "meterstop", "idtag"
        ]
        for field in required_fields:
            assert field in columns, f"ocpp_history_test table should have {field} column"
        
        conn.close()
    
    def test_ocpp_history_test_indexes(self):
        """Test that ocpp_history_test table has required indexes."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Get all indexes on ocpp_history_test
        cursor.execute("""
            SELECT indexname, indexdef 
            FROM pg_indexes 
            WHERE tablename = 'ocpp_history_test' AND schemaname = 'public'
        """)
        indexes = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Check required indexes exist (PostgreSQL column names are lowercase)
        required_indexes = ["stationid", "transactionid", "starttime", "endtime", "terminationreason"]
        for idx_col in required_indexes:
            found = any(f'({idx_col})' in indexdef or f'({idx_col},' in indexdef or f', {idx_col})' in indexdef 
                       for indexdef in indexes.values())
            assert found, f"ocpp_history_test should have index on {idx_col}"
        
        conn.close()
