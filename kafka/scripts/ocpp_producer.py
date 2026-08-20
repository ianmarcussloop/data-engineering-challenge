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
    # Increase queue size and add delivery callback
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "queue.buffering.max.messages": 200000,  # Increase from default 100k
        "queue.buffering.max.ms": 500,  # Wait up to 500ms to batch
        "message.timeout.ms": 30000,  # 30s timeout for individual messages
    })

    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Message delivery failed: {err}")
        else:
            pass  # Silent success to reduce verbosity

    total = len(messages)
    for i, message in enumerate(messages):
        try:
            producer.produce(
                topic=TOPIC_NAME,
                value=json.dumps(message).encode('utf-8'),
                callback=delivery_report
            )
            # Poll every 5000 messages to process delivery reports and free queue
            if i % 5000 == 0:
                producer.poll(0)
                print(f"📊 Published {i}/{total} messages...")
        except Exception as e:
            # Check if it's a queue full error
            if "Queue full" in str(e):
                print(f"⚠️ Queue full at {i}/{total}, waiting...")
                producer.poll(1.0)  # Wait up to 1 second for delivery reports
                # Retry
                producer.produce(
                    topic=TOPIC_NAME,
                    value=json.dumps(message).encode('utf-8'),
                    callback=delivery_report
                )
            else:
                print(f"❌ Failed to publish {message}: {e}")

    # Flush remaining messages
    print(f"🔄 Flushing remaining messages...")
    producer.flush()
    print("✨ All messages published!")

# --- Main ---
if __name__ == "__main__":
    print("📂 Parsing .txt file and publishing to Kafka...")
    messages = parse_txt_file(TXT_FILE_PATH)
    print(f"📋 Parsed {len(messages)} messages from {TXT_FILE_PATH}.")
    publish_to_kafka(messages)