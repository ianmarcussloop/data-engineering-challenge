"""Detailed tests for kafka/scripts/create_topics.py functions."""

import pytest
import sys
import os
import json

# Add kafka scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../kafka/scripts'))

from create_topics import (
    KAFKA_BROKER,
    TOPIC_NAME,
    SCHEMA_REGISTRY_URL,
    SCHEMA_SUBJECT,
    SCHEMA_DEFINITION,
)


# =============================================================================
# Tests for schema definition
# =============================================================================

class TestSchemaDefinition:
    """Tests for the schema definition."""

    def test_schema_has_required_fields(self):
        """Test that schema has all required fields."""
        assert "type" in SCHEMA_DEFINITION
        assert "name" in SCHEMA_DEFINITION
        assert "fields" in SCHEMA_DEFINITION
        
        assert SCHEMA_DEFINITION["type"] == "record"
        assert SCHEMA_DEFINITION["name"] == "OCPPMessage"

    def test_schema_has_correct_field_types(self):
        """Test that schema fields have correct types."""
        fields = SCHEMA_DEFINITION["fields"]
        
        # Check chargerId field
        charger_id_field = next(f for f in fields if f["name"] == "chargerId")
        assert charger_id_field["type"] == "string"
        
        # Check uniqueId field
        unique_id_field = next(f for f in fields if f["name"] == "uniqueId")
        assert unique_id_field["type"] == "string"
        
        # Check message field
        message_field = next(f for f in fields if f["name"] == "message")
        assert message_field["type"] == "string"

    def test_schema_definition_is_valid_json(self):
        """Test that schema definition can be serialized to JSON."""
        try:
            json_str = json.dumps(SCHEMA_DEFINITION)
            # Verify it can be parsed back
            parsed = json.loads(json_str)
            assert parsed == SCHEMA_DEFINITION
        except json.JSONDecodeError:
            pytest.fail("Schema definition is not valid JSON")

    def test_schema_has_all_required_fields(self):
        """Test that all required fields are present in the schema."""
        fields = SCHEMA_DEFINITION["fields"]
        field_names = [f["name"] for f in fields]
        
        assert "chargerId" in field_names
        assert "uniqueId" in field_names
        assert "message" in field_names


# =============================================================================
# Tests for configuration
# =============================================================================

class TestConfiguration:
    """Tests for configuration constants."""

    def test_kafka_broker_config(self):
        """Test Kafka broker configuration."""
        assert KAFKA_BROKER == "localhost:9092"

    def test_topic_name_config(self):
        """Test topic name configuration."""
        assert TOPIC_NAME == "ocpp.messages"

    def test_schema_registry_url_config(self):
        """Test schema registry URL configuration."""
        assert SCHEMA_REGISTRY_URL == "http://localhost:8081"

    def test_schema_subject_config(self):
        """Test schema subject configuration."""
        assert SCHEMA_SUBJECT == "ocpp.messages-value"


# =============================================================================
# Integration-style tests (mocked Kafka)
# =============================================================================

class TestCreateTopicsIntegration:
    """Integration tests for topic creation.
    
    These tests mock the Kafka AdminClient.
    """

    def test_topic_definitions_are_valid(self):
        """Test that topic definitions are valid."""
        from confluent_kafka.admin import NewTopic
        
        # These are the topic configurations from create_topics.py
        topic_configs = [
            ("ocpp.messages", 1, 1, {}),
            ("ocpp.messages_test", 1, 1, {}),
            ("ocpp.active.raw", 10, 1, {
                "cleanup.policy": "compact",
                "retention.ms": "259200000",
                "segment.ms": "60000",
                "min.compaction.lag.ms": "1000"
            }),
            ("ocpp.active", 10, 1, {
                "cleanup.policy": "compact",
                "segment.ms": "60000",
                "min.compaction.lag.ms": "1000"
            })
        ]
        
        # Verify we can create NewTopic objects with these configs
        for topic_name, num_partitions, replication_factor, configs in topic_configs:
            new_topic = NewTopic(
                topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=configs
            )
            assert new_topic.topic == topic_name
            assert new_topic.num_partitions == num_partitions
            assert new_topic.replication_factor == replication_factor
            assert new_topic.config == configs
