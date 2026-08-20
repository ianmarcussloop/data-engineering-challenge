from confluent_kafka import Producer
import json
import os
import ast
import re

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ocpp.messages"
TXT_FILE_PATH = "ocpp-data-many-chargers.txt"

# --- Parse .txt File ---
def parse_txt_file(file_path):
    messages = []
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue  # Skip empty lines

            # Use regex to extract charger ID and OCPP message
            match = re.match(r'^([^\s:]+)\s*:\s*(.+)$', line)
            if not match:
                print(f"⚠️ Skipping malformed line: {line}")
                continue

            charger_id = match.group(1)  # e.g., "charger6"
            ocpp_message_str = match.group(2)  # e.g., "[2,\"ef51a638...\",\"MeterValues\",{...}]"

            try:
                # Parse the OCPP message as a Python list
                ocpp_message = ast.literal_eval(ocpp_message_str)

                # Extract uniqueId (second element in the OCPP message)
                if len(ocpp_message) >= 2:
                    unique_id = ocpp_message[1]  # Second element is uniqueId
                    message_str = str(ocpp_message)  # Entire OCPP message as a string

                    messages.append({
                        "chargerId": charger_id,  # <-- ADD THIS
                        "uniqueId": unique_id,
                        "message": message_str
                    })
                else:
                    print(f"⚠️ Skipping malformed OCPP message: {ocpp_message_str}")
            except (SyntaxError, ValueError) as e:
                print(f"⚠️ Skipping malformed line: {line} (Error: {e})")

    return messages

# --- Publish to Kafka ---
def publish_to_kafka(messages):
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    for message in messages:
        try:
            producer.produce(
                topic=TOPIC_NAME,
                value=json.dumps(message).encode('utf-8')
            )
            print(f"✅ Published: {message}")
        except Exception as e:
            print(f"❌ Failed to publish {message}: {e}")

    producer.flush()
    print("✨ All messages published!")

# --- Main ---
if __name__ == "__main__":
    print("📂 Parsing .txt file and publishing to Kafka...")
    messages = parse_txt_file(TXT_FILE_PATH)
    print(f"📋 Parsed {len(messages)} messages from {TXT_FILE_PATH}.")
    publish_to_kafka(messages)