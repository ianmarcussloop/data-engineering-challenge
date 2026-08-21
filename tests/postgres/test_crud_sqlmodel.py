"""Phase 2.2: Test PostgreSQL CRUD operations on ocpp.history table using SQLModel.

These tests verify that we can insert, read, update, and delete data in the
ocpp.history table using SQLModel. We use SQLModel exclusively to avoid
issues with the table name containing a dot.
"""

import pytest
import uuid
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlmodel import Session, select, delete
from postgres.schema.ocpp_history import OcppHistory


TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"


def get_unique_session_id():
    """Generate a unique session ID for testing."""
    return f"test_session_{uuid.uuid4().hex[:8]}"


class TestInsertOperations:
    """Test inserting data into ocpp.history using SQLModel."""

    def test_insert_full_record(self):
        """Should be able to insert a complete record."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300,
                terminationReason="EVDriverDisconnected",
                totalEnergyConsumed=1.5,
                avgPower=5.0,
                maxPower=7.5,
                idTag="RFID123",
                connectorId=1,
                meterStart=1000,
                meterStop=1050,
                socStart=20.0,
                socEnd=25.0,
                voltageAvg=230.0,
                eventCount=10
            )
            
            session.add(history)
            session.commit()
            session.refresh(history)
            
            assert history.sessionId == session_id
            
            session.delete(history)
            session.commit()

    def test_insert_minimal_record(self):
        """Should be able to insert with minimal required fields."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            
            session.add(history)
            session.commit()
            session.refresh(history)
            
            assert history.sessionId == session_id
            assert history.eventCount == 0
            
            session.delete(history)
            session.commit()


class TestReadOperations:
    """Test reading data from ocpp.history."""

    def test_select_by_sessionid(self):
        """Should be able to select a record by sessionId."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            session.add(history)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            history = result.first()
            
            assert history is not None
            assert history.sessionId == session_id
            
            session.delete(history)
            session.commit()

    def test_select_by_stationid(self):
        """Should be able to select records by stationId."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        station_id = f"test_station_{uuid.uuid4().hex[:8]}"
        
        with Session(engine) as session:
            for i in range(3):
                history = OcppHistory(
                    sessionId=f"{station_id}_session_{i}",
                    stationId=station_id,
                    transactionId=f"txn{i}",
                    startTime=datetime(2025, 8, 18, 10, i, 0, tzinfo=timezone.utc),
                    endTime=datetime(2025, 8, 18, 10, i+1, 0, tzinfo=timezone.utc),
                    duration=60
                )
                session.add(history)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.stationId == station_id)
            result = session.exec(statement)
            histories = result.all()
            
            assert len(histories) == 3
        
        with Session(engine) as session:
            statement = delete(OcppHistory).where(OcppHistory.stationId == station_id)
            session.exec(statement)
            session.commit()


class TestUpdateOperations:
    """Test updating data in ocpp.history."""

    def test_update_single_field(self):
        """Should be able to update a single field."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            session.add(history)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            history = result.first()
            
            history.terminationReason = "EmergencyStop"
            session.add(history)
            session.commit()
            session.refresh(history)
            
            assert history.terminationReason == "EmergencyStop"
            
            session.delete(history)
            session.commit()

    def test_update_multiple_fields(self):
        """Should be able to update multiple fields."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            session.add(history)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            history = result.first()
            
            history.terminationReason = "RemoteStop"
            history.totalEnergyConsumed = 2.5
            history.avgPower = 5.0
            
            session.add(history)
            session.commit()
            session.refresh(history)
            
            assert history.terminationReason == "RemoteStop"
            assert history.totalEnergyConsumed == 2.5
            assert history.avgPower == 5.0
            
            session.delete(history)
            session.commit()


class TestDeleteOperations:
    """Test deleting data from ocpp.history."""

    def test_delete_by_sessionid(self):
        """Should be able to delete a record by sessionId."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            session.add(history)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            assert result.first() is not None
            
            statement = delete(OcppHistory).where(OcppHistory.sessionId == session_id)
            session.exec(statement)
            session.commit()
        
        with Session(engine) as session:
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            assert result.first() is None

    def test_delete_object(self):
        """Should be able to delete a record using the object."""
        engine = create_engine(TEST_POSTGRES_URL)
        
        session_id = get_unique_session_id()
        history_obj = None
        
        with Session(engine) as session:
            history = OcppHistory(
                sessionId=session_id,
                stationId="charger1",
                transactionId="txn001",
                startTime=datetime(2025, 8, 18, 10, 0, 0, tzinfo=timezone.utc),
                endTime=datetime(2025, 8, 18, 10, 5, 0, tzinfo=timezone.utc),
                duration=300
            )
            session.add(history)
            session.commit()
            
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            history_obj = result.first()
        
        with Session(engine) as session:
            session.delete(history_obj)
            session.commit()
            
            statement = select(OcppHistory).where(OcppHistory.sessionId == session_id)
            result = session.exec(statement)
            assert result.first() is None
