"""
Unit tests for spark_kafka_to_postgres.py

Tests the helper functions that parse and process OCPP messages.
"""
import pytest
import ast
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict, Any, Iterator


# =============================================================================
# Mock GroupState for testing update_session
# =============================================================================
class MockGroupState:
    """Mock implementation of pyspark.sql.streaming.GroupState for testing."""
    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        self._data = initial_data
        self._exists = initial_data is not None

    def exists(self) -> bool:
        return self._exists

    def get(self) -> Dict[str, Any]:
        if not self._exists:
            raise ValueError("State does not exist")
        return self._data

    def update(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._exists = True

    def remove(self) -> None:
        self._data = None
        self._exists = False


# =============================================================================
# Import functions under test
# =============================================================================
import sys
sys.path.insert(0, '/Users/iansloop/data-engineering-challenge/spark/scripts')

# Import the functions - safe now that main code is guarded by if __name__ == "__main__"
from spark_kafka_to_postgres import (
    parse_ocpp_message,
    extract_power_values,
    get_transaction_id,
    update_session,
)


# =============================================================================
# Fixtures
# =============================================================================
@pytest.fixture
def sample_meter_values_message():
    """A sample OCPP MeterValues message as a Python tuple (serialized with ast)."""
    return (
        2,
        "msg-001",
        "MeterValues",
        {
            "timestamp": "2024-01-15T10:30:00Z",
            "meterValue": [
                {
                    "timestamp": "2024-01-15T10:30:00Z",
                    "sampledValue": [
                        {"measurand": "Power.Active.Import", "value": "2200.5"},
                        {"measurand": "Energy.Active.Import.Register", "value": "100.0"},
                    ]
                },
                {
                    "timestamp": "2024-01-15T10:31:00Z",
                    "sampledValue": [
                        {"measurand": "Power.Active.Import", "value": "2300.0"},
                    ]
                }
            ]
        }
    )


@pytest.fixture
def sample_stop_transaction_message():
    """A sample OCPP StopTransaction message."""
    return (
        2,
        "msg-002",
        "StopTransaction",
        {
            "timestamp": "2024-01-15T11:00:00Z",
            "transactionId": 123,
            "reason": "EVDriver"
        }
    )


@pytest.fixture
def sample_status_notification_message():
    """A sample OCPP StatusNotification message."""
    return (
        2,
        "msg-003",
        "StatusNotification",
        {
            "status": "Available",
            "timestamp": "2024-01-15T10:00:00Z"
        }
    )


@pytest.fixture
def sample_heartbeat_message():
    """A sample OCPP Heartbeat message."""
    return (
        2,
        "msg-004",
        "Heartbeat",
        {}
    )


@pytest.fixture
def sample_short_message():
    """A message that's too short to be valid."""
    return (1, "msg-short")


@pytest.fixture
def sample_invalid_message():
    """An invalid message string."""
    return "not a valid tuple"


# =============================================================================
# Tests for parse_ocpp_message
# =============================================================================
class TestParseOcppMessage:
    """Tests for the parse_ocpp_message function."""

    def test_parse_valid_message_with_payload(self, sample_meter_values_message):
        """Test parsing a valid message with action and payload."""
        raw = str(sample_meter_values_message)
        result = parse_ocpp_message(raw)
        
        assert result is not None
        assert result["action"] == "MeterValues"
        assert result["uniqueId"] == "msg-001"
        assert "timestamp" in result["payload"]
        assert "meterValue" in result["payload"]

    def test_parse_valid_message_without_payload(self):
        """Test parsing a valid message without payload (only 3 elements)."""
        msg = (2, "msg-005", "BootNotification")
        raw = str(msg)
        result = parse_ocpp_message(raw)
        
        assert result is not None
        assert result["action"] == "BootNotification"
        assert result["uniqueId"] == "msg-005"
        assert result["payload"] == {}

    def test_parse_short_message_returns_none(self, sample_short_message):
        """Test that messages with less than 3 elements return None."""
        raw = str(sample_short_message)
        result = parse_ocpp_message(raw)
        assert result is None

    def test_parse_invalid_string_returns_none(self, sample_invalid_message):
        """Test that invalid strings return None."""
        result = parse_ocpp_message(sample_invalid_message)
        assert result is None

    def test_parse_empty_string_returns_none(self):
        """Test that empty strings return None."""
        result = parse_ocpp_message("")
        assert result is None

    def test_parse_message_with_nested_payload(self, sample_stop_transaction_message):
        """Test parsing a message with nested payload data."""
        raw = str(sample_stop_transaction_message)
        result = parse_ocpp_message(raw)
        
        assert result is not None
        assert result["action"] == "StopTransaction"
        assert result["payload"]["transactionId"] == 123
        assert result["payload"]["reason"] == "EVDriver"


# =============================================================================
# Tests for extract_power_values
# =============================================================================
class TestExtractPowerValues:
    """Tests for the extract_power_values function."""

    def test_extract_single_power_value(self):
        """Test extracting a single power value."""
        payload = {
            "meterValue": [
                {
                    "sampledValue": [
                        {"measurand": "Power.Active.Import", "value": "2200.5"}
                    ]
                }
            ]
        }
        result = extract_power_values(payload)
        assert result == [2200.5]

    def test_extract_multiple_power_values(self, sample_meter_values_message):
        """Test extracting multiple power values from nested meterValue."""
        payload = sample_meter_values_message[3]
        result = extract_power_values(payload)
        assert len(result) == 2
        assert 2200.5 in result
        assert 2300.0 in result

    def test_extract_no_power_values_wrong_measurand(self):
        """Test that non-Power.Active.Import values are ignored."""
        payload = {
            "meterValue": [
                {
                    "sampledValue": [
                        {"measurand": "Energy.Active.Import.Register", "value": "100.0"}
                    ]
                }
            ]
        }
        result = extract_power_values(payload)
        assert result == []

    def test_extract_no_meter_value(self):
        """Test with payload missing meterValue."""
        payload = {"someOtherField": "value"}
        result = extract_power_values(payload)
        assert result == []

    def test_extract_empty_payload(self):
        """Test with empty payload."""
        result = extract_power_values({})
        assert result == []

    def test_extract_with_none_payload(self):
        """Test with None payload."""
        result = extract_power_values(None)
        assert result == []

    def test_extract_with_invalid_value(self):
        """Test with non-numeric value (should be skipped)."""
        payload = {
            "meterValue": [
                {
                    "sampledValue": [
                        {"measurand": "Power.Active.Import", "value": "not_a_number"}
                    ]
                }
            ]
        }
        result = extract_power_values(payload)
        assert result == []

    def test_extract_mixed_valid_invalid_values(self):
        """Test with mix of valid and invalid values."""
        payload = {
            "meterValue": [
                {
                    "sampledValue": [
                        {"measurand": "Power.Active.Import", "value": "100.5"},
                        {"measurand": "Power.Active.Import", "value": "invalid"},
                        {"measurand": "Power.Active.Import", "value": "200.0"},
                    ]
                }
            ]
        }
        result = extract_power_values(payload)
        assert result == [100.5, 200.0]


# =============================================================================
# Tests for get_transaction_id
# =============================================================================
class TestGetTransactionId:
    """Tests for the get_transaction_id function."""

    def test_get_transaction_id_from_payload(self, sample_stop_transaction_message):
        """Test extracting transactionId from message payload."""
        raw = str(sample_stop_transaction_message)
        result = get_transaction_id(raw)
        assert result == 123

    def test_fallback_to_unique_id(self, sample_meter_values_message):
        """Test falling back to uniqueId when no transactionId in payload."""
        raw = str(sample_meter_values_message)
        result = get_transaction_id(raw)
        # The payload doesn't have transactionId, so fallback to msg[1] which is "msg-001"
        assert result == "msg-001"

    def test_fallback_with_no_payload(self):
        """Test fallback when message has no payload."""
        msg = (2, "msg-fallback", "StatusNotification")
        raw = str(msg)
        result = get_transaction_id(raw)
        assert result == "msg-fallback"

    def test_invalid_message_returns_none(self, sample_invalid_message):
        """Test that invalid messages return None."""
        result = get_transaction_id(sample_invalid_message)
        assert result is None

    def test_empty_string_returns_none(self):
        """Test that empty strings return None."""
        result = get_transaction_id("")
        assert result is None

    def test_short_message_returns_none(self, sample_short_message):
        """Test that short messages return None."""
        raw = str(sample_short_message)
        result = get_transaction_id(raw)
        assert result is None


# =============================================================================
# Tests for update_session
# =============================================================================
class TestUpdateSession:
    """Tests for the update_session function."""

    def _create_dataframe(self, messages: List[Tuple]) -> pd.DataFrame:
        """Helper to create a DataFrame with 'message' column."""
        data = [{"message": str(msg)} for msg in messages]
        return pd.DataFrame(data)

    def test_new_session_initialization(self):
        """Test that a new session state is initialized correctly."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([])
        mock_state = MockGroupState()
        
        # Create a generator from the function
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        # After processing empty df, state should exist
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["chargerId"] == "charger-001"
        assert state_data["status"] == "active"
        assert state_data["eventCount"] == 0

    def test_meter_values_updates_state(self, sample_meter_values_message):
        """Test that MeterValues updates the session state."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([sample_meter_values_message])
        mock_state = MockGroupState()
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["eventCount"] == 1
        assert state_data["power_values"] == [2200.5, 2300.0]
        assert state_data["startTime"] is not None
        assert state_data["endTime"] is not None

    def test_stop_transaction_yields_result(self, sample_meter_values_message, sample_stop_transaction_message):
        """Test that StopTransaction yields a result tuple."""
        key = ("charger-001", "tx-001")
        
        # First add MeterValues, then StopTransaction
        messages = [sample_meter_values_message, sample_stop_transaction_message]
        df = self._create_dataframe(messages)
        mock_state = MockGroupState()
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        # Should yield exactly one result
        assert len(results) == 1
        result = results[0]
        
        # Result is a tuple of 9 elements
        assert len(result) == 9
        session_id, station_id, status, start_time, end_time, duration, energy, event_count, termination_reason = result
        
        assert station_id == "charger-001"
        assert status == "ended"
        assert termination_reason == "EVDriver"
        assert event_count == 2  # MeterValues + StopTransaction
        assert duration >= 0
        assert energy >= 0

    def test_status_notification_increments_count(self, sample_status_notification_message):
        """Test that StatusNotification increments event count."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([sample_status_notification_message])
        mock_state = MockGroupState()
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["eventCount"] == 1

    def test_heartbeat_increments_count(self, sample_heartbeat_message):
        """Test that Heartbeat increments event count."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([sample_heartbeat_message])
        mock_state = MockGroupState()
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["eventCount"] == 1

    def test_existing_state_is_used(self):
        """Test that existing state is retrieved and updated."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([])
        
        initial_state = {
            "chargerId": "charger-001",
            "startTime": "2024-01-15T10:00:00",
            "endTime": "2024-01-15T10:30:00",
            "power_values": [100.0, 200.0],
            "eventCount": 5,
            "status": "active",
            "terminationReason": None
        }
        mock_state = MockGroupState(initial_state)
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        # State should still exist with same data
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["eventCount"] == 5  # No change from empty df

    def test_invalid_messages_are_skipped(self, sample_invalid_message):
        """Test that invalid messages are skipped without error."""
        key = ("charger-001", "tx-001")
        df = self._create_dataframe([("invalid",)])  # This will fail parse_ocpp_message
        mock_state = MockGroupState()
        
        # Should not raise an exception
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        # State should exist but with 0 eventCount (message was skipped)
        assert mock_state.exists()
        state_data = mock_state.get()
        assert state_data["eventCount"] == 0

    def test_multiple_messages_in_sequence(self, sample_meter_values_message, sample_stop_transaction_message):
        """Test processing multiple messages in sequence."""
        key = ("charger-001", "tx-001")
        
        # Create a sequence: MeterValues, MeterValues, StopTransaction
        messages = [
            sample_meter_values_message,
            sample_meter_values_message,  # Same message again
            sample_stop_transaction_message
        ]
        df = self._create_dataframe(messages)
        mock_state = MockGroupState()
        
        gen = update_session(key, df, mock_state)
        results = list(gen)
        
        assert len(results) == 1  # One result from StopTransaction
        result = results[0]
        assert result[2] == "ended"  # status
        assert result[7] == 3  # eventCount: 2 MeterValues + 1 StopTransaction
