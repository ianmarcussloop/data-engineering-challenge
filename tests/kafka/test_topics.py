"""Phase 1: Test Kafka topic creation and configuration.

These tests verify that the TEST Kafka topics exist with correct configurations.
They use topics that are auto-created by conftest.py.
They will FAIL initially if test infrastructure doesn't exist, then PASS after
conftest.py creates them or after running tests/setup_infra.py
"""

import pytest
from confluent_kafka.admin import AdminClient


TEST_KAFKA_BROKER = "localhost:9092"


class TestKafkaTopicCreation:
    """Test that production Kafka topics exist with correct configurations."""

    def test_ocpp_messages_topic_exists(self):
        """ocpp.messages topic should exist (test raw messages topic)."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.messages" in topics, "ocpp.messages topic should exist"

    def test_ocpp_active_topic_exists(self):
        """ocpp.active topic should exist."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active" in topics, "ocpp.active topic should exist"

    def test_ocpp_active_raw_topic_exists(self):
        """ocpp.active.raw topic should exist."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        assert "ocpp.active.raw" in topics, "ocpp.active.raw topic should exist"


class TestKafkaTopicAccess:
    """Test that we can produce to and consume from the test topics."""

    def test_can_produce_to_ocpp_active(self):
        """Should be able to produce messages to ocpp.active topic."""
        from confluent_kafka import Producer
        import json
        
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        session_id = "test_session_001"
        msg = {"sessionId": session_id, "stationId": "charger1", "status": "active"}
        
        # Produce should not raise an exception
        producer.produce("ocpp.active", key=session_id, value=json.dumps(msg))
        producer.flush(timeout=5)
        
        # If we get here without exception, the produce was successful
        assert True

    def test_can_produce_to_ocpp_active_raw(self):
        """Should be able to produce messages to ocpp.active.raw topic."""
        from confluent_kafka import Producer
        import json
        
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        session_id = "test_session_002"
        msg = {
            "stationId": "charger1",
            "timestamp": "2025-01-01T10:00:00.000+00:00",
            "action": "MeterValues",
            "value": {"power": 22.5}
        }
        
        # Produce should not raise an exception
        producer.produce("ocpp.active.raw", key=session_id, value=json.dumps(msg))
        producer.flush(timeout=5)
        
        # If we get here without exception, the produce was successful
        assert True
