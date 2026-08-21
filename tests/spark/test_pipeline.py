"""Phase 3: Test Spark pipeline components that can be tested without running Spark."""

import pytest
from confluent_kafka import Producer
import psycopg2
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../spark/scripts'))

# Read from environment or use defaults
TEST_KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")
TEST_POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")


@pytest.mark.kafka
class TestKafkaProducers:
    """Test Kafka producers for pipeline."""
    
    @pytest.mark.kafka
    def test_produce_to_active_raw_format(self):
        """Test producing messages in ocpp.active.raw format."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        test_topic = "ocpp.active.raw"
        session_id = "pipeline_test_raw_001"
        
        msg = {
            "stationId": "charger1",
            "timestamp": "2025-01-01T10:00:00.000+00:00",
            "action": "MeterValues",
            "value": {"power": 22.5, "soc": 50.0}
        }
        
        # produce() returns None when partition is not specified (message is queued)
        # The important thing is that it doesn't raise an exception
        result = producer.produce(test_topic, key=session_id, value=json.dumps(msg))
        producer.flush()
        
        # In confluent-kafka-python 2.x, produce() returns None when no partition is specified
        # This is expected behavior, so we just verify no exception was raised
    
    @pytest.mark.kafka
    def test_produce_to_active_state_format(self):
        """Test producing messages in ocpp.active format."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        test_topic = "ocpp.active"
        session_id = "pipeline_test_state_001"
        
        msg = {
            "sessionId": session_id,
            "stationId": "charger1",
            "transactionId": "txn001",
            "status": "active",
            "startTime": "2025-01-01T10:00:00.000+00:00",
            "lastSeen": "2025-01-01T10:01:00.000+00:00",
            "duration": 60,
            "energyConsumedSoFar": 0.5,
            "runningCount": 1
        }
        
        # produce() returns None when partition is not specified (message is queued)
        result = producer.produce(test_topic, key=session_id, value=json.dumps(msg))
        producer.flush()
        
        # In confluent-kafka-python 2.x, produce() returns None when no partition is specified
        # This is expected behavior, so we just verify no exception was raised
    
    @pytest.mark.kafka
    def test_produce_tombstone(self):
        """Test producing tombstones (null values)."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        test_topic = "ocpp.active"
        session_id = "pipeline_test_tombstone_001"
        
        # produce() returns None when partition is not specified (message is queued)
        result1 = producer.produce(test_topic, key=session_id, value=json.dumps({"test": "data"}))
        result2 = producer.produce(test_topic, key=session_id, value=None)
        
        producer.flush()
        
        # In confluent-kafka-python 2.x, produce() returns None when no partition is specified
        # This is expected behavior, so we just verify no exception was raised


@pytest.mark.postgres
class TestPostgresIntegration:
    """Test PostgreSQL integration for pipeline."""
    
    @pytest.mark.postgres
    def test_ocpp_history_table_exists(self):
        """Test that ocpp.history table exists."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ocpp' AND table_name = 'history'")
        result = cursor.fetchone()
        
        assert result is not None
        assert result[0] == "history"
        conn.close()
    
    @pytest.mark.postgres
    def test_ocpp_history_has_required_columns(self):
        """Test that ocpp.history has all required columns (camelCase)."""
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'ocpp' AND table_name = 'history' ORDER BY ordinal_position")
        columns = [row[0] for row in cursor.fetchall()]
        
        # Columns are camelCase as per schema requirement
        required_fields = [
            "sessionId", "stationId", "transactionId", "startTime", "endTime",
            "duration", "terminationReason", "totalEnergyConsumed",
            "avgPower", "maxPower", "idTag", "connectorId",
            "meterStart", "meterStop", "socStart", "socEnd",
            "voltageAvg", "eventCount"
        ]
        
        for field in required_fields:
            assert field in columns, f"ocpp.history should have {field} column, has: {columns}"
        
        conn.close()


class TestSparkParsersIntegration:
    """Test that Spark parsers can be imported and used."""
    
    def test_import_all_parsers(self):
        """Test that all parser functions can be imported from Spark script."""
        from spark_kafka_to_postgres import (
            parse_ocpp_message,
            get_transaction_id,
            parse_timestamp,
            parse_action,
            parse_power,
            extract_power_value,
            parse_meter_start,
            parse_meter_stop,
            parse_id_tag,
            parse_connector_id,
            parse_soc,
            parse_voltage,
            parse_reason,
            is_stop_action
        )
        
        assert callable(parse_ocpp_message)
        assert callable(get_transaction_id)
        assert callable(parse_timestamp)
        assert callable(parse_action)
        assert callable(parse_power)
        assert callable(extract_power_value)
        assert callable(parse_meter_start)
        assert callable(parse_meter_stop)
        assert callable(parse_id_tag)
        assert callable(parse_connector_id)
        assert callable(parse_soc)
        assert callable(parse_voltage)
        assert callable(parse_reason)
        assert callable(is_stop_action)
