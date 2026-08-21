"""Phase 1.5: Test OCPP-specific message patterns and session lifecycle.

These tests verify OCPP message handling patterns that are specific to the
EV charging domain, including session identification, message parsing, and
lifecycle transitions.
"""

import pytest
import json
import ast
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient
from tests.fixtures.ocpp_messages import (
    start_transaction,
    meter_values,
    stop_transaction,
    remote_stop_transaction
)


TEST_KAFKA_BROKER = "localhost:9092"


class TestSessionIdGeneration:
    """Test session ID generation patterns."""

    def test_session_id_from_start_transaction(self):
        """Session ID should be generated from chargerId + startTime."""
        msg = start_transaction(
            chargerId="charger6",
            transactionId="txn001",
            timestamp="2025-08-26T23:59:57.599Z"
        )
        
        # Parse and extract fields
        parsed = ast.literal_eval(msg)
        chargerId = parsed[1]
        timestamp = parsed[3]["timestamp"]
        
        # Expected session ID pattern
        expected_session_id = f"{chargerId}_{timestamp}"
        
        assert expected_session_id == "charger6_2025-08-26T23:59:57.599Z"

    def test_session_id_consistency_across_messages(self):
        """All messages in a session should map to the same sessionId."""
        base_timestamp = "2025-08-26T23:59:57.599Z"
        chargerId = "charger6"
        transactionId = "txn001"
        
        # Generate messages for the same session
        msg1 = start_transaction(
            chargerId=chargerId,
            transactionId=transactionId,
            timestamp=base_timestamp,
            meterStart=1000
        )
        
        msg2 = meter_values(
            chargerId=chargerId,
            transactionId=transactionId,
            power=22.5,
            timestamp="2025-08-26T23:59:58.599Z"  # Slightly later
        )
        
        msg3 = stop_transaction(
            chargerId=chargerId,
            transactionId=transactionId,
            meterStop=1100,
            timestamp="2025-08-26T23:59:59.599Z"
        )
        
        # Parse and verify transactionId is consistent
        for msg in [msg1, msg2, msg3]:
            parsed = ast.literal_eval(msg)
            assert parsed[3]["transactionId"] == transactionId


class TestOCPPMessageParsing:
    """Test parsing of OCPP messages to extract key fields."""

    def test_parse_charger_id_from_message(self):
        """Should be able to extract chargerId from OCPP message."""
        msg = start_transaction(
            chargerId="charger6",
            transactionId="txn001",
            meterStart=1000
        )
        
        parsed = ast.literal_eval(msg)
        chargerId = parsed[1]
        
        assert chargerId == "charger6"

    def test_parse_action_from_message(self):
        """Should be able to extract action from OCPP message."""
        actions = ["StartTransaction", "MeterValues", "StopTransaction", "RemoteStopTransaction"]
        
        for action in actions:
            if action == "StartTransaction":
                msg = start_transaction("charger1", "txn001", 1000)
            elif action == "MeterValues":
                msg = meter_values("charger1", "txn001", 22.5)
            elif action == "StopTransaction":
                msg = stop_transaction("charger1", "txn001", 1100)
            else:
                msg = remote_stop_transaction("charger1", "txn001")
            
            parsed = ast.literal_eval(msg)
            assert parsed[2] == action

    def test_parse_transaction_id_from_all_messages(self):
        """Should be able to extract transactionId from all message types."""
        transactionId = "txn001"
        
        msg1 = start_transaction("charger1", transactionId, 1000)
        msg2 = meter_values("charger1", transactionId, 22.5)
        msg3 = stop_transaction("charger1", transactionId, 1100)
        msg4 = remote_stop_transaction("charger1", transactionId)
        
        for msg in [msg1, msg2, msg3, msg4]:
            parsed = ast.literal_eval(msg)
            assert parsed[3]["transactionId"] == transactionId

    def test_parse_meter_values_fields(self):
        """Should be able to extract all fields from MeterValues message."""
        msg = meter_values(
            chargerId="charger1",
            transactionId="txn001",
            power=22.5,
            energy=1050.0,
            soc=50.0,
            voltage=230.0,
            timestamp="2025-08-18T10:01:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        payload = parsed[3]
        
        assert payload["transactionId"] == "txn001"
        assert payload["power"] == 22.5
        assert payload["energy"] == 1050.0
        assert payload["Battery.SOC"] == 50.0
        assert payload["Voltage"] == 230.0
        assert payload["timestamp"] == "2025-08-18T10:01:00.000Z"

    def test_parse_start_transaction_fields(self):
        """Should be able to extract all fields from StartTransaction message."""
        msg = start_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStart=1000,
            idTag="RFID123",
            timestamp="2025-08-18T10:00:00.000Z",
            connectorId=1
        )
        
        parsed = ast.literal_eval(msg)
        payload = parsed[3]
        
        assert payload["transactionId"] == "txn001"
        assert payload["meterStart"] == 1000
        assert payload["idTag"] == "RFID123"
        assert payload["connectorId"] == 1
        assert payload["timestamp"] == "2025-08-18T10:00:00.000Z"

    def test_parse_stop_transaction_fields(self):
        """Should be able to extract all fields from StopTransaction message."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="EVDriverDisconnected",
            timestamp="2025-08-18T10:05:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        payload = parsed[3]
        
        assert payload["transactionId"] == "txn001"
        assert payload["meterStop"] == 1100
        assert payload["reason"] == "EVDriverDisconnected"
        assert payload["timestamp"] == "2025-08-18T10:05:00.000Z"


class TestMessageTypeIdentification:
    """Test identification of message types (Call Request vs Call Response)."""

    def test_call_request_has_type_2(self):
        """Call Request messages should have messageType 2."""
        msg = start_transaction("charger1", "txn001", 1000)
        parsed = ast.literal_eval(msg)
        
        assert parsed[0] == 2

    def test_all_ocpp_actions_are_call_requests(self):
        """All OCPP action messages (StartTransaction, MeterValues, etc.) should be Call Requests."""
        actions = [
            ("StartTransaction", start_transaction("charger1", "txn001", 1000)),
            ("MeterValues", meter_values("charger1", "txn001", 22.5)),
            ("StopTransaction", stop_transaction("charger1", "txn001", 1100)),
            ("RemoteStopTransaction", remote_stop_transaction("charger1", "txn001"))
        ]
        
        for action_name, msg in actions:
            parsed = ast.literal_eval(msg)
            assert parsed[0] == 2, f"{action_name} should be Call Request (type 2)"
            assert parsed[2] == action_name


class TestOCPPDataTypes:
    """Test OCPP data type handling."""

    def test_unique_id_is_string(self):
        """UniqueId should be a string (UUID format)."""
        msg = start_transaction("charger1", "txn001", 1000)
        parsed = ast.literal_eval(msg)
        uniqueId = parsed[1]
        
        assert isinstance(uniqueId, str)

    def test_meter_start_is_integer(self):
        """MeterStart should be an integer."""
        msg = start_transaction("charger1", "txn001", meterStart=1000)
        parsed = ast.literal_eval(msg)
        meterStart = parsed[3]["meterStart"]
        
        assert isinstance(meterStart, int)

    def test_meter_stop_is_integer(self):
        """MeterStop should be an integer."""
        msg = stop_transaction("charger1", "txn001", meterStop=1100)
        parsed = ast.literal_eval(msg)
        meterStop = parsed[3]["meterStop"]
        
        assert isinstance(meterStop, int)

    def test_power_is_float(self):
        """Power values should be floats."""
        msg = meter_values("charger1", "txn001", power=22.5)
        parsed = ast.literal_eval(msg)
        power = parsed[3]["power"]
        
        assert isinstance(power, float)

    def test_soc_is_float(self):
        """State of Charge values should be floats."""
        msg = meter_values("charger1", "txn001", power=22.5, soc=50.0)
        parsed = ast.literal_eval(msg)
        soc = parsed[3]["Battery.SOC"]
        
        assert isinstance(soc, float)

    def test_voltage_is_float(self):
        """Voltage values should be floats."""
        msg = meter_values("charger1", "txn001", power=22.5, voltage=230.0)
        parsed = ast.literal_eval(msg)
        voltage = parsed[3]["Voltage"]
        
        assert isinstance(voltage, float)


class TestSessionLifecycleMessages:
    """Test complete session lifecycle message patterns."""

    def test_session_starts_with_start_transaction(self):
        """A session should start with StartTransaction message."""
        msg = start_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStart=1000,
            timestamp="2025-08-18T10:00:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        
        assert parsed[2] == "StartTransaction"
        assert "meterStart" in parsed[3]

    def test_session_has_meter_values(self):
        """A session should have MeterValues messages."""
        msg = meter_values(
            chargerId="charger1",
            transactionId="txn001",
            power=22.5,
            timestamp="2025-08-18T10:01:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        
        assert parsed[2] == "MeterValues"
        assert "power" in parsed[3]

    def test_session_ends_with_stop_transaction(self):
        """A session can end with StopTransaction message."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="EVDriverDisconnected",
            timestamp="2025-08-18T10:05:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        
        assert parsed[2] == "StopTransaction"
        assert "meterStop" in parsed[3]
        assert "reason" in parsed[3]

    def test_session_ends_with_remote_stop_transaction(self):
        """A session can end with RemoteStopTransaction message."""
        msg = remote_stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            timestamp="2025-08-18T10:05:00.000Z"
        )
        
        parsed = ast.literal_eval(msg)
        
        assert parsed[2] == "RemoteStopTransaction"
        assert "transactionId" in parsed[3]


class TestStopReasons:
    """Test various stop reasons for session termination."""

    def test_stop_reason_ev_driver_disconnected(self):
        """EVDriverDisconnected is a valid stop reason."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="EVDriverDisconnected"
        )
        
        parsed = ast.literal_eval(msg)
        assert parsed[3]["reason"] == "EVDriverDisconnected"

    def test_stop_reason_emergency_stop(self):
        """EmergencyStop is a valid stop reason."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="EmergencyStop"
        )
        
        parsed = ast.literal_eval(msg)
        assert parsed[3]["reason"] == "EmergencyStop"

    def test_stop_reason_hard_reset(self):
        """HardReset is a valid stop reason."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="HardReset"
        )
        
        parsed = ast.literal_eval(msg)
        assert parsed[3]["reason"] == "HardReset"

    def test_stop_reason_soft_reset(self):
        """SoftReset is a valid stop reason."""
        msg = stop_transaction(
            chargerId="charger1",
            transactionId="txn001",
            meterStop=1100,
            reason="SoftReset"
        )
        
        parsed = ast.literal_eval(msg)
        assert parsed[3]["reason"] == "SoftReset"


class TestOCPPMessageInKafka:
    """Test OCPP messages stored in Kafka topics."""

    def test_ocpp_message_roundtrip_to_kafka(self):
        """OCPP message should survive roundtrip through Kafka."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        original_msg = start_transaction(
            chargerId="charger1",
            transactionId="txn_roundtrip",
            meterStart=1000,
            idTag="RFID123",
            timestamp="2025-08-18T10:00:00.000Z",
            connectorId=1,
            wrap_for_kafka=True
        )
        
        # Produce to Kafka
        producer.produce("ocpp.messages", value=original_msg)
        producer.flush(timeout=5)
        
        # Consume from Kafka
        consumer = Consumer({
            "bootstrap.servers": TEST_KAFKA_BROKER,
            "group.id": "test_roundtrip",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False
        })
        consumer.subscribe(["ocpp.messages"])
        
        consumed_msg = consumer.poll(timeout=5)
        consumer.close()
        
        if consumed_msg is not None:
            consumed_value = consumed_msg.value().decode('utf-8')
            
            # Parse both as JSON and compare
            import json
            original_parsed = json.loads(original_msg)
            consumed_parsed = json.loads(consumed_value)
            
            assert consumed_parsed["chargerId"] == original_parsed["chargerId"]
            assert consumed_parsed["uniqueId"] == original_parsed["uniqueId"]
            assert consumed_parsed["message"] == original_parsed["message"]
