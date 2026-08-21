"""Phase 2.1: Test PostgreSQL production schema (ocpp.history table).

These tests verify that the production ocpp.history table exists with all
required fields and indexes as specified in the architecture.

Note: The table name is "ocpp.history" (with a dot) in the public schema,
not in an "ocpp" schema.
"""

import pytest
import psycopg2
from sqlalchemy import create_engine, inspect, text


TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"


def get_table_columns(table_name):
    """Helper to get column names for a table."""
    conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
    cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [row[0] for row in cursor.fetchall()]
    conn.close()
    return columns


class TestProductionTableExists:
    """Test that the production ocpp.history table exists."""

    def test_ocpp_history_table_exists(self):
        """ocpp.history table should exist in PostgreSQL."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name = 'ocpp.history'
        """)
        result = cursor.fetchone()
        
        assert result is not None, "ocpp.history table should exist"
        assert result[0] == "ocpp.history"
        
        conn.close()


class TestProductionTableSchema:
    """Test that ocpp.history has all required columns from the OcppHistory SQLModel."""

    def test_ocpp_history_has_all_required_columns(self):
        """ocpp.history should have all required fields."""
        columns = get_table_columns("ocpp.history")
        
        # All fields from OcppHistory SQLModel
        required_fields = [
            "sessionId",
            "stationId",
            "transactionId",
            "startTime",
            "endTime",
            "duration",
            "terminationReason",
            "totalEnergyConsumed",
            "avgPower",
            "maxPower",
            "idTag",
            "connectorId",
            "meterStart",
            "meterStop",
            "socStart",
            "socEnd",
            "voltageAvg",
            "eventCount"
        ]
        
        for field in required_fields:
            assert field in columns, f"ocpp.history should have {field} column, has: {columns}"

    def test_ocpp_history_primary_key(self):
        """ocpp.history should have sessionId as primary key."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Get primary key using a different method that works with quoted table names
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.key_column_usage 
            WHERE table_name = 'ocpp.history'
        """)
        pk_columns = [row[0] for row in cursor.fetchall()]
        
        assert "sessionId" in pk_columns, f"sessionId should be primary key, has: {pk_columns}"
        
        conn.close()

    def test_ocpp_history_column_types(self):
        """ocpp.history should have correct column types."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Get column types
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'ocpp.history'
        """)
        col_types = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Check key column types
        # Note: PostgreSQL's "character varying" is equivalent to "text"
        assert col_types["sessionId"] in ["text", "character varying"]
        assert col_types["stationId"] in ["text", "character varying"]
        assert col_types["transactionId"] in ["text", "character varying"]
        assert col_types["duration"] == "integer"
        assert col_types["totalEnergyConsumed"] in ["double precision", "real"]
        
        conn.close()


class TestProductionTableIndexes:
    """Test that ocpp.history has required indexes for performance."""

    def test_ocpp_history_has_all_required_indexes(self):
        """ocpp.history should have indexes on key query fields."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname, indexdef FROM pg_indexes 
            WHERE tablename = 'ocpp.history'
        """)
        indexes = {row[0]: row[1] for row in cursor.fetchall()}
        
        # Check for required indexes (PostgreSQL may have created them with different names)
        required_columns = ["stationId", "transactionId", "startTime", "endTime", "terminationReason"]
        
        for col in required_columns:
            # Check if any index contains this column
            found = any(col.lower() in indexdef.lower() or col.replace("Id", "id").lower() in indexdef.lower() 
                       for indexdef in indexes.values())
            assert found, f"ocpp.history should have index on {col}, has: {list(indexes.keys())}"
        
        conn.close()


class TestProductionTableConstraints:
    """Test table constraints."""

    def test_sessionid_not_null(self):
        """sessionId should be NOT NULL."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'ocpp.history' AND column_name = 'sessionId'
        """)
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "NO"
        
        conn.close()

    def test_stationid_not_null(self):
        """stationId should be NOT NULL."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'ocpp.history' AND column_name = 'stationId'
        """)
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "NO"
        
        conn.close()

    def test_transactionid_not_null(self):
        """transactionId should be NOT NULL."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'ocpp.history' AND column_name = 'transactionId'
        """)
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "NO"
        
        conn.close()
