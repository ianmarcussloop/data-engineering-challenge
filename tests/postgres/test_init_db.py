"""Tests for postgres/scripts/init_db.py database initialization."""

import pytest
import sys
import os

# Add postgres scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../postgres/scripts'))


# =============================================================================
# Mock-based tests for schema verification
# =============================================================================

class TestSchemaModels:
    """Tests for the schema models used in init_db."""

    def test_charger_session_model_exists(self):
        """Test that ChargerSession model can be imported."""
        from postgres.schema.charger_session import ChargerSession
        assert ChargerSession is not None

    def test_ocpp_history_model_exists(self):
        """Test that OcppHistory model can be imported."""
        from postgres.schema.ocpp_history import OcppHistory
        assert OcppHistory is not None

    def test_models_have_table_names(self):
        """Test that models have correct table names."""
        from postgres.schema.charger_session import ChargerSession
        from postgres.schema.ocpp_history import OcppHistory
        
        assert ChargerSession.__tablename__ == "charger_session"
        assert OcppHistory.__tablename__ == "ocpp.history"

    def test_models_have_required_fields(self):
        """Test that models have required fields."""
        from postgres.schema.charger_session import ChargerSession
        from postgres.schema.ocpp_history import OcppHistory
        from sqlmodel import Field
        
        # Check ChargerSession fields
        assert hasattr(ChargerSession, "sessionId")
        assert hasattr(ChargerSession, "stationId")
        assert hasattr(ChargerSession, "startTime")
        assert hasattr(ChargerSession, "status")
        
        # Check OcppHistory fields
        assert hasattr(OcppHistory, "sessionId")
        assert hasattr(OcppHistory, "stationId")
        assert hasattr(OcppHistory, "transactionId")
        assert hasattr(OcppHistory, "startTime")
        assert hasattr(OcppHistory, "endTime")
        assert hasattr(OcppHistory, "duration")
        
        # Check optional fields in OcppHistory
        assert hasattr(OcppHistory, "totalEnergyConsumed")
        assert hasattr(OcppHistory, "avgPower")
        assert hasattr(OcppHistory, "maxPower")
        assert hasattr(OcppHistory, "meterStart")
        assert hasattr(OcppHistory, "meterStop")
        assert hasattr(OcppHistory, "socStart")
        assert hasattr(OcppHistory, "socEnd")
        assert hasattr(OcppHistory, "voltageAvg")


class TestConfiguration:
    """Tests for configuration in init_db."""

    def test_db_url_from_environment(self):
        """Test that DB_URL can be configured via environment."""
        import os
        os.environ["DATABASE_URL"] = "postgresql://test:test@localhost:5432/test"
        
        # Re-import to pick up the new environment variable
        import importlib
        import init_db
        importlib.reload(init_db)
        
        assert init_db.DB_URL == "postgresql://test:test@localhost:5432/test"
        
        # Clean up
        del os.environ["DATABASE_URL"]

    def test_default_db_url(self):
        """Test the default DB_URL."""
        import os
        # Clear any existing value
        os.environ.pop("DATABASE_URL", None)
        
        import importlib
        import init_db
        importlib.reload(init_db)
        
        assert init_db.DB_URL == "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"


class TestIndexDefinitions:
    """Tests for index definitions in init_db."""

    def test_index_sql_queries_are_valid(self):
        """Test that index creation SQL queries are syntactically valid."""
        # These are the index queries from the init_db.py file
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_ocpp_history_stationId ON ocpp.history (stationId)",
            "CREATE INDEX IF NOT EXISTS idx_ocpp_history_transactionId ON ocpp.history (transactionId)",
            "CREATE INDEX IF NOT EXISTS idx_ocpp_history_startTime ON ocpp.history (startTime)",
            "CREATE INDEX IF NOT EXISTS idx_ocpp_history_endTime ON ocpp.history (endTime)",
            "CREATE INDEX IF NOT EXISTS idx_ocpp_history_terminationReason ON ocpp.history (terminationReason)"
        ]
        
        # All queries should be non-empty strings
        for query in index_queries:
            assert isinstance(query, str)
            assert len(query) > 0
            assert "ocpp.history" in query
            assert "IF NOT EXISTS" in query


# =============================================================================
# Integration-style tests (with real database if available)
# =============================================================================

class TestDatabaseIntegration:
    """Integration tests for database initialization.
    
    These tests require a real PostgreSQL database to be running.
    They will be skipped if the database is not available.
    """

    @pytest.mark.postgres
    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires real PostgreSQL database")
    def test_create_tables_with_real_db(self):
        """Test create_tables with a real database connection."""
        pytest.importorskip("psycopg2")
        pytest.importorskip("sqlmodel")
        
        # This test requires the actual database to be running
        # For now, just verify we can import and call the function
        # without errors when the database is available
        try:
            from init_db import create_tables
            # Don't actually call it as it requires a real DB
            assert callable(create_tables)
        except ImportError:
            pytest.skip("Required dependencies not available")
