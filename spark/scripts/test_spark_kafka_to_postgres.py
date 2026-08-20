"""
Unit tests for spark_kafka_to_postgres.py

Tests the helper functions that parse and process OCPP messages.
"""
import pytest
import ast


# =============================================================================
# Import functions under test
# =============================================================================
import sys
sys.path.insert(0, '/Users/iansloop/data-engineering-challenge/spark/scripts')

# Import the functions - safe now that main code is guarded by if __name__ == "__main__"
from spark_kafka_to_postgres import (
    parse_ocpp_message,
    extract_power_value,
    get_transaction_id,
    parse_timestamp,
    parse_action,
    parse_power,
    parse_meter_start,
    parse_meter_stop,
    parse_id_tag,
    parse_connector_id,
    parse_soc,
    parse_voltage,
    parse_reason,
    is_stop_action,
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

    def test_parse_short_message_returns_none(self):
        """Test that messages with less than 3 elements return None."""
        msg = (1, "msg-short")
        raw = str(msg)
        result = parse_ocpp_message(raw)
        assert result is None

    def test_parse_invalid_string_returns_none(self):
        """Test that invalid strings return None."""
        result = parse_ocpp_message("not a valid tuple")
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
# Tests for extract_power_value
# =============================================================================
class TestExtractPowerValue:
    """Tests for the extract_power_value function."""

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
        result = extract_power_value(payload)
        assert result == 2200.5

    def test_extract_multiple_power_values_returns_first(self, sample_meter_values_message):
        """Test extracting first power value from nested meterValue (only returns first match)."""
        payload = sample_meter_values_message[3]
        result = extract_power_value(payload)
        # Returns the first Power.Active.Import value found
        assert result == 2200.5

    def test_extract_no_power_value_wrong_measurand(self):
        """Test that non-Power.Active.Import values return None."""
        payload = {
            "meterValue": [
                {
                    "sampledValue": [
                        {"measurand": "Energy.Active.Import.Register", "value": "100.0"}
                    ]
                }
            ]
        }
        result = extract_power_value(payload)
        assert result is None

    def test_extract_no_meter_value(self):
        """Test with payload missing meterValue."""
        payload = {"someOtherField": "value"}
        result = extract_power_value(payload)
        assert result is None

    def test_extract_empty_payload(self):
        """Test with empty payload."""
        result = extract_power_value({})
        assert result is None

    def test_extract_with_none_payload(self):
        """Test with None payload."""
        result = extract_power_value(None)
        assert result is None

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
        result = extract_power_value(payload)
        assert result is None


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

    def test_no_transaction_id_returns_none(self, sample_meter_values_message):
        """Test that messages without transactionId in payload return None."""
        raw = str(sample_meter_values_message)
        result = get_transaction_id(raw)
        # The payload doesn't have transactionId, so returns None
        assert result is None

    def test_no_payload_returns_none(self):
        """Test that messages without payload return None."""
        msg = (2, "msg-fallback", "StatusNotification")
        raw = str(msg)
        result = get_transaction_id(raw)
        # No dict payload, so returns None
        assert result is None

    def test_invalid_message_returns_none(self):
        """Test that invalid messages return None."""
        result = get_transaction_id("not a valid tuple")
        assert result is None

    def test_empty_string_returns_none(self):
        """Test that empty strings return None."""
        result = get_transaction_id("")
        assert result is None

    def test_short_message_returns_none(self):
        """Test that short messages return None."""
        msg = (1, "msg-short")
        raw = str(msg)
        result = get_transaction_id(raw)
        assert result is None


# =============================================================================
# Tests for additional parsing functions
# =============================================================================
class TestParsingFunctions:
    """Additional tests for all parsing functions."""

    def test_parse_action_start_transaction(self):
        """Test parsing StartTransaction action."""
        msg = '[2, "charger1", "StartTransaction", {"transactionId": "txn123"}]'
        assert parse_action(msg) == "StartTransaction"

    def test_parse_action_meter_values(self):
        """Test parsing MeterValues action."""
        msg = '[2, "charger1", "MeterValues", {"power": 22.5}]'
        assert parse_action(msg) == "MeterValues"

    def test_parse_action_stop_transaction(self):
        """Test parsing StopTransaction action."""
        msg = '[2, "charger1", "StopTransaction", {"reason": "Done"}]'
        assert parse_action(msg) == "StopTransaction"

    def test_parse_action_remote_stop(self):
        """Test parsing RemoteStopTransaction action."""
        msg = '[2, "charger1", "RemoteStopTransaction", {}]'
        assert parse_action(msg) == "RemoteStopTransaction"

    def test_get_transaction_id(self):
        """Test extracting transactionId."""
        msg = '[2, "charger1", "StartTransaction", {"transactionId": "txn123"}]'
        assert get_transaction_id(msg) == "txn123"

    def test_parse_timestamp_meter_values(self):
        """Test parsing timestamp from MeterValues."""
        msg = '[2, "charger1", "MeterValues", {"meterValue": [{"timestamp": "2025-01-01T10:00:00Z"}]}]'
        result = parse_timestamp(msg)
        assert result == "2025-01-01T10:00:00+00:00"

    def test_parse_timestamp_start_transaction(self):
        """Test parsing timestamp from StartTransaction."""
        msg = '[2, "charger1", "StartTransaction", {"timestamp": "2025-01-01T10:00:00Z"}]'
        result = parse_timestamp(msg)
        assert result == "2025-01-01T10:00:00+00:00"

    def test_parse_power(self):
        """Test parsing power value."""
        msg = '[2, "charger1", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Power.Active.Import", "value": "22.5"}]}]}]'
        assert parse_power(msg) == 22.5

    def test_parse_meter_start(self):
        """Test parsing meterStart value."""
        msg = '[2, "321", "StartTransaction", {"transactionId": "txn123", "meterStart": 1000, "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_meter_start(msg) == 1000

    def test_parse_meter_stop(self):
        """Test parsing meterStop value."""
        msg = '[2, "322", "StopTransaction", {"transactionId": "txn123", "meterStop": 1500, "timestamp": "2025-01-01T10:05:00Z"}]'
        assert parse_meter_stop(msg) == 1500

    def test_parse_id_tag(self):
        """Test parsing idTag value."""
        msg = '[2, "321", "StartTransaction", {"idTag": "RFID123", "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_id_tag(msg) == "RFID123"

    def test_parse_connector_id(self):
        """Test parsing connectorId value."""
        msg = '[2, "321", "StartTransaction", {"connectorId": 1, "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_connector_id(msg) == 1

    def test_parse_soc(self):
        """Test parsing SOC value."""
        msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Battery.SOC", "value": 75}]}]}]'
        assert parse_soc(msg) == 75.0

    def test_parse_voltage(self):
        """Test parsing voltage value."""
        msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Voltage", "value": 230}]}]}]'
        assert parse_voltage(msg) == 230.0

    def test_parse_reason(self):
        """Test parsing reason value."""
        msg = '[2, "charger1", "StopTransaction", {"transactionId": "txn123", "reason": "EVDriver"}]'
        assert parse_reason(msg) == "EVDriver"

    def test_is_stop_action_stop_transaction(self):
        """Test is_stop_action for StopTransaction."""
        assert is_stop_action("StopTransaction") == True

    def test_is_stop_action_remote_stop(self):
        """Test is_stop_action for RemoteStopTransaction."""
        assert is_stop_action("RemoteStopTransaction") == True

    def test_is_stop_action_meter_values(self):
        """Test is_stop_action for MeterValues."""
        assert is_stop_action("MeterValues") == False

    def test_is_stop_action_start_transaction(self):
        """Test is_stop_action for StartTransaction."""
        assert is_stop_action("StartTransaction") == False
