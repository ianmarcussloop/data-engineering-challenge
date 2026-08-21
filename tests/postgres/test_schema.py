"""Phase 2: Test PostgreSQL schema for ocpp.history table.

These tests verify that the PostgreSQL table exists with all required
fields and indexes. They use tables that are auto-created
by conftest.py. They will FAIL initially if test infrastructure doesn't exist,
then PASS after conftest.py creates them.
"""

import pytest
import psycopg2
import os
from sqlalchemy import create_engine, inspect


TEST_POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")


class TestPostgresTableExists:
    """Test that the ocpp.history table exists."""

    def test_ocpp_history_table_exists(self):
        """ocpp.history table should exist in PostgreSQL."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp.history'
        """)
        result = cursor.fetchone()
        
        assert result is not None, "ocpp.history table should exist"
        assert result[0] == "ocpp.history"
        
        conn.close()


class TestPostgresTableSchema:
    """Test that ocpp.history has all required columns."""

    def test_ocpp_history_has_all_required_columns(self):
        """ocpp.history should have all required fields from the schema."""
        engine = create_engine(TEST_POSTGRES_URL)
        inspector = inspect(engine)
        
        columns = inspector.get_columns("ocpp.history")
        actual_fields = [col["name"] for col in columns]
        
        # Note: PostgreSQL converts camelCase to snake_case
        # Check that all required fields are present (using snake_case as PostgreSQL stores them)
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
            assert field in actual_fields, f"ocpp.history should have {field} column, has: {actual_fields}"

    def test_ocpp_history_primary_key(self):
        """ocpp.history should have sessionId as primary key."""
        engine = create_engine(TEST_POSTGRES_URL)
        inspector = inspect(engine)
        
        primary_keys = inspector.get_pk_constraint("ocpp.history")
        assert primary_keys is not None
        # PostgreSQL stores it as sessionid (lowercase)
        assert "sessionid" in primary_keys["constrained_columns"] or "sessionId" in primary_keys["constrained_columns"]


class TestPostgresTableIndexes:
    """Test that ocpp.history has required indexes for performance."""

    def test_ocpp_history_has_stationid_index(self):
        """ocpp.history should have an index on stationid."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'ocpp.history' AND schemaname = 'public'
        """)
        index_names = [row[0] for row in cursor.fetchall()]
        
        # Check for stationid index (might be named differently)
        stationid_indexes = [idx for idx in index_names if 'stationid' in idx.lower() or 'station_id' in idx.lower()]
        assert len(stationid_indexes) > 0, f"ocpp.history should have index on stationid, has: {index_names}"
        
        conn.close()

    def test_ocpp_history_has_transactionid_index(self):
        """ocpp.history should have an index on transactionid."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'ocpp.history' AND schemaname = 'public'
        """)
        index_names = [row[0] for row in cursor.fetchall()]
        
        transactionid_indexes = [idx for idx in index_names if 'transactionid' in idx.lower() or 'transaction_id' in idx.lower()]
        assert len(transactionid_indexes) > 0, f"ocpp.history should have index on transactionid, has: {index_names}"
        
        conn.close()

    def test_ocpp_history_has_starttime_index(self):
        """ocpp.history should have an index on starttime."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'ocpp.history' AND schemaname = 'public'
        """)
        index_names = [row[0] for row in cursor.fetchall()]
        
        starttime_indexes = [idx for idx in index_names if 'starttime' in idx.lower() or 'start_time' in idx.lower()]
        assert len(starttime_indexes) > 0, f"ocpp.history should have index on starttime, has: {index_names}"
        
        conn.close()

    def test_ocpp_history_has_endtime_index(self):
        """ocpp.history should have an index on endtime."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'ocpp.history' AND schemaname = 'public'
        """)
        index_names = [row[0] for row in cursor.fetchall()]
        
        endtime_indexes = [idx for idx in index_names if 'endtime' in idx.lower() or 'end_time' in idx.lower()]
        assert len(endtime_indexes) > 0, f"ocpp.history should have index on endtime, has: {index_names}"
        
        conn.close()

    def test_ocpp_history_has_terminationreason_index(self):
        """ocpp.history should have an index on terminationreason."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'ocpp.history' AND schemaname = 'public'
        """)
        index_names = [row[0] for row in cursor.fetchall()]
        
        reason_indexes = [idx for idx in index_names if 'terminationreason' in idx.lower() or 'termination_reason' in idx.lower()]
        assert len(reason_indexes) > 0, f"ocpp.history should have index on terminationreason, has: {index_names}"
        
        conn.close()
