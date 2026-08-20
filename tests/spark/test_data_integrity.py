"""Phase 3.3: Test data integrity in PostgreSQL ocpp.history_test table.

These tests verify that completed sessions written to PostgreSQL have all
required fields populated correctly. They use the _test table that is auto-created
by conftest.py. They will FAIL initially if test infrastructure doesn't exist,
then PASS after the Spark pipeline correctly processes and writes sessions.

Note: These tests require the Spark streaming pipeline to be running to populate data.
"""

import pytest
import psycopg2
from datetime import datetime
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../spark/scripts'))

# Import fixtures from the tests.fixtures module
from tests.fixtures.ocpp_messages import start_transaction, meter_values, stop_transaction


TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"


@pytest.mark.postgres
class TestDataIntegrityFields:
    """Test that all required fields are populated in PostgreSQL."""

    def test_session_has_all_required_fields(self):
        """Completed sessions should have all required fields populated."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        # Get a session from the database (test table)
        cursor.execute("SELECT * FROM ocpp_history_test LIMIT 1")
        row = cursor.fetchone()
        
        if row is None:
            pytest.skip("No sessions in ocpp_history_test table yet")
        
        # Map row to column names
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'ocpp_history_test' ORDER BY ordinal_position")
        columns = [col[0] for col in cursor.fetchall()]
        session = dict(zip(columns, row))
        
        # Check required fields are not None (use lowercase for PostgreSQL column names)
        required_fields = [
            "sessionid",
            "stationid",
            "transactionid",
            "starttime",
            "endtime",
            "duration",
        ]
        
        for field in required_fields:
            assert session.get(field) is not None, f"Session should have {field} populated, has: {list(session.keys())}"
        
        conn.close()

    def test_session_has_meter_values(self):
        """Completed sessions should have meterStart and meterStop populated."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT meterstart, meterstop FROM ocpp_history_test WHERE meterstart IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        
        if row is None:
            pytest.skip("No sessions with meter values in ocpp_history_test table yet")
        
        meter_start, meter_stop = row
        assert meter_start is not None, "Session should have meterStart"
        assert meter_stop is not None, "Session should have meterStop"
        assert meter_stop >= meter_start, "meterStop should be >= meterStart"
        
        conn.close()

    def test_session_has_energy_calculation(self):
        """Completed sessions should have totalEnergyConsumed calculated."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT totalenergyconsumed, avgpower, duration 
            FROM ocpp_history_test 
            WHERE totalenergyconsumed IS NOT NULL 
            AND avgpower IS NOT NULL 
            AND duration IS NOT NULL 
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        if row is None:
            pytest.skip("No sessions with energy calculation in ocpp_history_test table yet")
        
        total_energy, avg_power, duration = row
        
        # Verify calculation: totalEnergyConsumed = avgPower * (duration / 3600)
        expected_energy = avg_power * (duration / 3600)
        assert abs(total_energy - expected_energy) < 0.01, \
            f"Energy calculation incorrect: expected {expected_energy}, got {total_energy}"
        
        conn.close()

    def test_session_duration_calculation(self):
        """Session duration should be calculated correctly from startTime and endTime."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT duration, starttime, endtime 
            FROM ocpp_history_test 
            WHERE duration IS NOT NULL 
            AND starttime IS NOT NULL 
            AND endtime IS NOT NULL 
            LIMIT 1
        """)
        row = cursor.fetchone()
        
        if row is None:
            pytest.skip("No sessions with duration in ocpp_history_test table yet")
        
        duration, start_time, end_time = row
        
        # Calculate expected duration
        if isinstance(start_time, datetime) and isinstance(end_time, datetime):
            expected_duration = int((end_time - start_time).total_seconds())
        else:
            # If they're strings, parse them
            expected_duration = int((end_time - start_time).total_seconds())
        
        # Allow 1 second tolerance
        assert abs(duration - expected_duration) <= 1, \
            f"Duration incorrect: expected {expected_duration}, got {duration}"
        
        conn.close()


@pytest.mark.postgres
class TestDataIntegrityTypes:
    """Test that data types are correct in PostgreSQL."""

    def test_session_id_is_string(self):
        """sessionid should be a string."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT sessionid FROM ocpp_history_test LIMIT 1")
        row = cursor.fetchone()
        
        if row is not None:
            assert isinstance(row[0], str), "sessionid should be a string"
        
        conn.close()

    def test_duration_is_integer(self):
        """duration should be an integer."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT duration FROM ocpp_history_test WHERE duration IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        
        if row is not None:
            assert isinstance(row[0], int), "duration should be an integer"
        
        conn.close()

    def test_energy_is_float(self):
        """totalenergyconsumed should be a float."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT totalenergyconsumed FROM ocpp_history_test WHERE totalenergyconsumed IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        
        if row is not None:
            assert isinstance(row[0], (float, int)), "totalenergyconsumed should be a number"
        
        conn.close()
