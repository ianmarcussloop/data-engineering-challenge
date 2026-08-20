"""Detailed tests for kafka/scripts/ocpp_producer.py functions."""

import pytest
import sys
import os
import ast
import re
from typing import List, Dict

# Add kafka scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../kafka/scripts'))

from ocpp_producer import parse_txt_file, publish_to_kafka, TXT_FILE_PATH


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_txt_content(tmp_path):
    """Create a temporary .txt file with sample OCPP messages."""
    txt_file = tmp_path / "test_ocpp_data.txt"
    content = """charger1: [2,"a098df69","MeterValues",{"timestamp":"2025-01-01T10:00:00Z","meterValue":[{"timestamp":"2025-01-01T10:00:00Z","sampledValue":[{"measurand":"Power.Active.Import","value":"22.5"}]}]}]
charger1: [2,"b198df70","MeterValues",{"timestamp":"2025-01-01T10:01:00Z","meterValue":[{"timestamp":"2025-01-01T10:01:00Z","sampledValue":[{"measurand":"Power.Active.Import","value":"25.0"}]}]}]
charger2: [2,"c2098df71","StartTransaction",{"transactionId":"txn001","meterStart":1000,"timestamp":"2025-01-01T09:00:00Z"}]
charger1: [2,"d3198df72","StopTransaction",{"transactionId":"txn001","meterStop":1100,"timestamp":"2025-01-01T10:05:00Z","reason":"EVDriver"}]
"""
    txt_file.write_text(content)
    return str(txt_file)


@pytest.fixture
def empty_txt_content(tmp_path):
    """Create an empty .txt file."""
    txt_file = tmp_path / "empty.txt"
    txt_file.write_text("")
    return str(txt_file)


@pytest.fixture
def malformed_txt_content(tmp_path):
    """Create a .txt file with malformed lines."""
    txt_file = tmp_path / "malformed.txt"
    content = """charger1: [2,"a098df69","MeterValues",{"timestamp":"2025-01-01T10:00:00Z"}]
not a valid line
charger2: [2,"b198df70"]
: [2,"c2098df71","MeterValues"]
charger3: invalid message
"""
    txt_file.write_text(content)
    return str(txt_file)


# =============================================================================
# Tests for parse_txt_file
# =============================================================================

class TestParseTxtFile:
    """Tests for the parse_txt_file function."""

    def test_parse_valid_file(self, sample_txt_content):
        """Test parsing a file with valid OCPP messages."""
        messages = parse_txt_file(sample_txt_content)
        
        assert len(messages) == 4
        
        # Check first message
        assert messages[0]["chargerId"] == "charger1"
        assert messages[0]["uniqueId"] == "a098df69"
        # The message field contains the full OCPP message as a string
        assert "MeterValues" in messages[0]["message"]
        
        # Check second message
        assert messages[1]["chargerId"] == "charger1"
        assert messages[1]["uniqueId"] == "b198df70"
        assert "MeterValues" in messages[1]["message"]
        
        # Check third message (different charger)
        assert messages[2]["chargerId"] == "charger2"
        assert messages[2]["uniqueId"] == "c2098df71"
        assert "StartTransaction" in messages[2]["message"]
        
        # Check fourth message
        assert messages[3]["chargerId"] == "charger1"
        assert "StopTransaction" in messages[3]["message"]

    def test_parse_empty_file(self, empty_txt_content):
        """Test parsing an empty file."""
        messages = parse_txt_file(empty_txt_content)
        
        assert messages == []

    def test_parse_file_with_empty_lines(self, tmp_path):
        """Test parsing a file with empty lines."""
        txt_file = tmp_path / "with_empty_lines.txt"
        content = """

charger1: [2,"a098df69","MeterValues",{}]

charger2: [2,"b198df70","MeterValues",{}]

"""
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 2
        assert messages[0]["chargerId"] == "charger1"
        assert messages[1]["chargerId"] == "charger2"

    def test_parse_file_with_malformed_lines(self, malformed_txt_content, caplog):
        """Test parsing a file with malformed lines."""
        import logging
        with caplog.at_level(logging.WARNING):
            messages = parse_txt_file(malformed_txt_content)
        
        # Should still parse valid lines
        assert len(messages) == 2
        assert messages[0]["chargerId"] == "charger1"
        assert messages[1]["chargerId"] == "charger2"

    def test_parse_message_without_colon_separator(self, tmp_path):
        """Test that lines without colon separator are skipped."""
        txt_file = tmp_path / "no_colon.txt"
        content = "charger1 [2,\"a098df69\",\"MeterValues\",{}]"
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 0

    def test_parse_message_with_missing_charger_id(self, tmp_path):
        """Test that lines without charger ID are skipped."""
        txt_file = tmp_path / "no_id.txt"
        content = ": [2,\"a098df69\",\"MeterValues\",{}]"
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 0

    def test_parse_message_with_short_ocpp_message(self, tmp_path):
        """Test that messages with less than 2 elements are skipped."""
        txt_file = tmp_path / "short.txt"
        content = "charger1: [2]"
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 0

    def test_parse_message_preserves_full_message(self, tmp_path):
        """Test that the full message string is preserved."""
        txt_file = tmp_path / "preserve.txt"
        # Note: ast.literal_eval will convert the string to a Python object,
        # then str() will convert it back, but with Python representation (single quotes)
        original_msg = '[2,"a098df69","MeterValues",{"timestamp":"2025-01-01T10:00:00Z"}]'
        content = f"charger1: {original_msg}"
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 1
        # The message field will be the string representation of the parsed tuple
        # which uses Python's repr() format (single quotes for strings)
        # So we check that it contains the expected data, not exact string match
        assert "MeterValues" in messages[0]["message"]
        assert "a098df69" in messages[0]["message"]
        assert "timestamp" in messages[0]["message"]

    def test_parse_multiple_chargers(self, tmp_path):
        """Test parsing messages from multiple chargers."""
        txt_file = tmp_path / "multi.txt"
        content = """charger1: [2,"a098df69","MeterValues",{}]
charger2: [2,"b198df70","MeterValues",{}]
charger3: [2,"c2098df71","MeterValues",{}]
charger1: [2,"d3198df72","MeterValues",{}]
"""
        txt_file.write_text(content)
        
        messages = parse_txt_file(str(txt_file))
        
        assert len(messages) == 4
        chargers = [m["chargerId"] for m in messages]
        assert chargers == ["charger1", "charger2", "charger3", "charger1"]
        
        # Verify all messages have the required fields
        for msg in messages:
            assert "chargerId" in msg
            assert "uniqueId" in msg
            assert "message" in msg


# =============================================================================
# Tests for publish_to_kafka (mocked) - using monkeypatch
# =============================================================================

class TestPublishToKafka:
    """Tests for the publish_to_kafka function."""

    def test_publish_empty_list(self, monkeypatch):
        """Test publishing an empty list of messages."""
        from unittest.mock import MagicMock, patch
        
        mock_producer = MagicMock()
        with patch('ocpp_producer.Producer', return_value=mock_producer):
            publish_to_kafka([])
        
        # Producer should be created but no messages should be published
        mock_producer.produce.assert_not_called()
        mock_producer.flush.assert_called_once()

    def test_publish_single_message(self, monkeypatch):
        """Test publishing a single message."""
        from unittest.mock import MagicMock, patch
        
        mock_producer = MagicMock()
        with patch('ocpp_producer.Producer', return_value=mock_producer):
            messages = [{"chargerId": "charger1", "uniqueId": "msg001", "message": "test"}]
            publish_to_kafka(messages)
        
        mock_producer.produce.assert_called_once()
        mock_producer.flush.assert_called_once()

    def test_publish_multiple_messages(self, monkeypatch):
        """Test publishing multiple messages."""
        from unittest.mock import MagicMock, patch
        
        mock_producer = MagicMock()
        with patch('ocpp_producer.Producer', return_value=mock_producer):
            messages = [
                {"chargerId": "charger1", "uniqueId": "msg001", "message": "test1"},
                {"chargerId": "charger2", "uniqueId": "msg002", "message": "test2"},
                {"chargerId": "charger3", "uniqueId": "msg003", "message": "test3"}
            ]
            publish_to_kafka(messages)
        
        assert mock_producer.produce.call_count == 3
        mock_producer.flush.assert_called_once()

    def test_publish_handles_producer_error(self, monkeypatch, caplog):
        """Test handling of producer errors."""
        from unittest.mock import MagicMock, patch
        import logging
        
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = Exception("Producer error")
        with patch('ocpp_producer.Producer', return_value=mock_producer):
            messages = [{"chargerId": "charger1", "uniqueId": "msg001", "message": "test"}]
            
            with caplog.at_level(logging.ERROR):
                publish_to_kafka(messages)
        
        mock_producer.flush.assert_called_once()
