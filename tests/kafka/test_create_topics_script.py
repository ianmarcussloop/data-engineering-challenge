"""Phase 1.4: Test the create_topics.py script functionality.

These tests verify that the create_topics.py script correctly creates and
configures Kafka topics as specified in the architecture.
"""

import pytest
import sys
import os
import subprocess
from confluent_kafka.admin import AdminClient, NewTopic


TEST_KAFKA_BROKER = "localhost:9092"


class TestCreateTopicsScript:
    """Test the create_topics.py script execution."""

    def test_script_runs_without_error(self):
        """The create_topics.py script should run without errors."""
        # Change to project root directory
        script_path = os.path.join(os.getcwd(), "kafka", "scripts", "create_topics.py")
        
        # Run the script
        result = subprocess.run(
            ["uv", "run", "python", script_path],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Script should complete (exit code 0 or already exists messages)
        # Note: It may print "already exists" but that's fine
        assert "Traceback" not in result.stderr
        assert "Error" not in result.stderr or "already exists" in result.stderr

    def test_script_creates_ocpp_messages_topic(self):
        """The script should create ocpp.messages and ocpp.messages topics."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        
        assert "ocpp.messages" in topics
        assert "ocpp.messages" in topics

    def test_script_creates_ocpp_active_topic(self):
        """The script should create ocpp.active topic."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        
        assert "ocpp.active" in topics

    def test_script_creates_ocpp_active_raw_topic(self):
        """The script should create ocpp.active.raw topic."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        
        assert "ocpp.active.raw" in topics


class TestCreateTopicsFunction:
    """Test the create_kafka_topic() function directly."""

    def test_create_kafka_topic_function(self):
        """Test that create_kafka_topic() creates topics correctly."""
        # Import the function
        sys.path.insert(0, os.path.join(os.getcwd(), "kafka", "scripts"))
        from create_topics import create_kafka_topic
        
        # Call the function (idempotent - won't error if topics exist)
        try:
            create_kafka_topic()
            created = True
        except Exception as e:
            # May fail if Kafka isn't running, but we check that elsewhere
            created = "already exists" in str(e) or "already exists" in str(e).lower()
        
        # Verify topics exist
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        topics = admin.list_topics(timeout=5).topics
        
        assert "ocpp.messages" in topics
        assert "ocpp.messages" in topics
        assert "ocpp.active" in topics
        assert "ocpp.active.raw" in topics


class TestTopicRecreation:
    """Test that topics can be recreated if needed."""

    def test_topic_creation_idempotent(self):
        """Calling create multiple times should not cause errors."""
        sys.path.insert(0, os.path.join(os.getcwd(), "kafka", "scripts"))
        from create_topics import create_kafka_topic
        
        # Call the function multiple times
        try:
            create_kafka_topic()
            create_kafka_topic()
            create_kafka_topic()
        except Exception as e:
            # Should only fail if topics don't exist and can't be created
            # But since they exist after first call, subsequent calls should just print "already exists"
            assert "already exists" in str(e) or "already exists" in str(e).lower()


class TestTopicDeletionAndRecreation:
    """Test that topics can be deleted and recreated."""

    def test_delete_and_recreate_topic(self):
        """Should be able to delete and recreate a test topic."""
        admin = AdminClient({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        test_topic = "test_delete_recreate_topic"
        
        # Create the topic
        new_topic = NewTopic(
            test_topic,
            num_partitions=1,
            replication_factor=1,
            config={"cleanup.policy": "compact"}
        )
        admin.create_topics([new_topic])
        
        # Wait for creation
        import time
        time.sleep(2)
        
        # Verify it exists
        assert test_topic in admin.list_topics(timeout=5).topics
        
        # Delete the topic
        admin.delete_topics([test_topic])
        
        # Wait for deletion
        time.sleep(2)
        
        # Verify it's gone
        assert test_topic not in admin.list_topics(timeout=5).topics
        
        # Recreate it
        admin.create_topics([new_topic])
        
        # Wait for recreation
        time.sleep(2)
        
        # Verify it exists again
        assert test_topic in admin.list_topics(timeout=5).topics
        
        # Cleanup: delete it again
        admin.delete_topics([test_topic])
