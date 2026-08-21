"""Test parsing functions for Spark pipeline."""

import pytest
import sys
import os

# Add spark scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../spark/scripts'))

from spark_kafka_to_postgres import (
    parse_meter_start,
    parse_meter_stop,
    parse_id_tag,
    parse_connector_id,
    parse_soc,
    parse_voltage,
    parse_action,
    parse_timestamp,
    get_transaction_id,
    parse_power,
    extract_power_value,
    is_stop_action
)


class TestParsingFunctions:
    """Test all parsing functions."""
    
    def test_parse_action_start_transaction(self):
        msg = '[2, "charger1", "StartTransaction", {"transactionId": "txn123"}]'
        assert parse_action(msg) == "StartTransaction"
    
    def test_parse_action_meter_values(self):
        msg = '[2, "charger1", "MeterValues", {"power": 22.5}]'
        assert parse_action(msg) == "MeterValues"
    
    def test_parse_action_stop_transaction(self):
        msg = '[2, "charger1", "StopTransaction", {"reason": "Done"}]'
        assert parse_action(msg) == "StopTransaction"
    
    def test_parse_action_remote_stop(self):
        msg = '[2, "charger1", "RemoteStopTransaction", {}]'
        assert parse_action(msg) == "RemoteStopTransaction"

    def test_get_transaction_id(self):
        msg = '[2, "charger1", "StartTransaction", {"transactionId": "txn123"}]'
        assert get_transaction_id(msg) == "txn123"
    
    def test_parse_timestamp_meter_values(self):
        msg = '[2, "charger1", "MeterValues", {"meterValue": [{"timestamp": "2025-01-01T10:00:00Z"}]}]'
        result = parse_timestamp(msg)
        assert result == "2025-01-01T10:00:00+00:00"
    
    def test_parse_timestamp_start_transaction(self):
        msg = '[2, "charger1", "StartTransaction", {"timestamp": "2025-01-01T10:00:00Z"}]'
        result = parse_timestamp(msg)
        assert result == "2025-01-01T10:00:00+00:00"
    
    def test_parse_power(self):
        msg = '[2, "charger1", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Power.Active.Import", "value": "22.5"}]}]}]'
        assert parse_power(msg) == 22.5
    
    def test_parse_meter_start(self):
        msg = '[2, "321", "StartTransaction", {"transactionId": "txn123", "meterStart": 1000, "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_meter_start(msg) == 1000
    
    def test_parse_meter_stop(self):
        msg = '[2, "322", "StopTransaction", {"transactionId": "txn123", "meterStop": 1500, "timestamp": "2025-01-01T10:05:00Z"}]'
        assert parse_meter_stop(msg) == 1500
    
    def test_parse_id_tag(self):
        msg = '[2, "321", "StartTransaction", {"idTag": "RFID123", "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_id_tag(msg) == "RFID123"
    
    def test_parse_connector_id(self):
        msg = '[2, "321", "StartTransaction", {"connectorId": 1, "timestamp": "2025-01-01T10:00:00Z"}]'
        assert parse_connector_id(msg) == 1
    
    def test_parse_soc(self):
        msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Battery.SOC", "value": 75}]}]}]'
        assert parse_soc(msg) == 75.0
    
    def test_parse_voltage(self):
        msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Voltage", "value": 230}]}]}]'
        assert parse_voltage(msg) == 230.0
    
    def test_is_stop_action_stop_transaction(self):
        assert is_stop_action("StopTransaction") == True
    
    def test_is_stop_action_remote_stop(self):
        assert is_stop_action("RemoteStopTransaction") == True
    
    def test_is_stop_action_meter_values(self):
        assert is_stop_action("MeterValues") == False
    
    def test_is_stop_action_start_transaction(self):
        assert is_stop_action("StartTransaction") == False


class TestExtractPowerValue:
    """Test power extraction from different payload formats."""
    
    def test_extract_power_from_simple_payload(self):
        payload = {
            "meterValue": [{
                "sampledValue": [{
                    "measurand": "Power.Active.Import",
                    "value": "22.5"
                }]
            }]
        }
        assert extract_power_value(payload) == 22.5
    
    def test_extract_power_from_multiple_measurands(self):
        payload = {
            "meterValue": [{
                "sampledValue": [
                    {"measurand": "Voltage", "value": "230"},
                    {"measurand": "Power.Active.Import", "value": "22.5"},
                    {"measurand": "Energy", "value": "1000"}
                ]
            }]
        }
        assert extract_power_value(payload) == 22.5
    
    def test_extract_power_returns_none_when_not_found(self):
        payload = {"meterValue": []}
        assert extract_power_value(payload) is None
