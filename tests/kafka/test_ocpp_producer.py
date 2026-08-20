# Test parsing logic for .txt files
import pytest
from kafka.scripts.ocpp_producer import parse_txt_file

def test_parse_txt_file():
    # Create a temporary test file
    test_data = """
    charger6 : [2,"ef51a638-0e05-4a9d-be52-594ada28f153","MeterValues",{}]
    charger10 : [2,"2e71389f-174a-44ef-a11d-6fa4db1b75a2","Heartbeat",{}]
    """
    with open("test_data.txt", "w") as f:
        f.write(test_data)

    # Parse the file
    messages = parse_txt_file("test_data.txt")

    # Assertions
    assert len(messages) == 2
    assert messages[0]["uniqueId"] == "ef51a638-0e05-4a9d-be52-594ada28f153"
    assert messages[0]["message"].startswith("[2,")
    assert messages[1]["uniqueId"] == "2e71389f-174a-44ef-a11d-6fa4db1b75a2"
    assert messages[1]["message"].startswith("[2,")

    # Clean up
    import os
    os.remove("test_data.txt")