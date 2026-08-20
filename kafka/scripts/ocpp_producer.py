from confluent_kafka import Producer
import json
import os
import ast
import re

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
VALID_TOPIC = "ocpp.messages"
MALFORMED_TOPIC = "ocpp.malformed"
TXT_FILE_PATHS = ["ocpp-data-many-chargers.txt", "ocpp-data-many-days.txt"]
TXT_FILE_PATH = "ocpp-sample-data.txt"  # For backward compatibility with tests

# --- Parse and Validate .txt File ---
def parse_and_validate_txt_file(file_path):
    """
    Parse OCPP messages from file and separate into:
    - Valid request/response pairs (both exist with matching uniqueId)
    - Malformed messages (can't be parsed, or no matching pair)
    """
    # Track all parsed messages by uniqueId
    requests = {}   # uniqueId -> {chargerId, message, raw_line}
    responses = {}  # uniqueId -> {chargerId, message, raw_line}
    unparseable = []  # Lines that couldn't be parsed
    
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            
            # Skip comment lines
            if line.startswith('#'):
                continue

            # Use regex to extract charger ID and OCPP message
            match = re.match(r'^([^\s:]+)\s*:\s*(.+)$', line)
            if not match:
                unparseable.append({"line": line, "reason": "regex match failed"})
                continue

            charger_id = match.group(1)
            ocpp_message_str = match.group(2)

            try:
                # Fix timestamps with spaces for JSON parsing
                fixed_str = re.sub(r'"(\d{8})\s+(\d+Z)"', r'"\1T\2"', ocpp_message_str)
                ocpp_message = json.loads(fixed_str)
            except json.JSONDecodeError:
                try:
                    ocpp_message = ast.literal_eval(ocpp_message_str)
                except (SyntaxError, ValueError):
                    unparseable.append({"line": line, "reason": "JSON/ast parsing failed"})
                    continue

            # Extract message type and uniqueId
            if len(ocpp_message) < 2:
                unparseable.append({"line": line, "reason": "message too short"})
                continue

            message_type = ocpp_message[0]
            unique_id = ocpp_message[1]

            # Store based on type
            if message_type == 2:  # Call Request
                requests[unique_id] = {
                    "chargerId": charger_id,
                    "message": ocpp_message,
                    "raw": line
                }
            elif message_type == 3:  # Call Response
                responses[unique_id] = {
                    "chargerId": charger_id,
                    "message": ocpp_message,
                    "raw": line
                }
            else:
                # Unknown message type
                unparseable.append({"line": line, "reason": f"unknown message type: {message_type}"})

    # Find matching pairs
    validated_pairs = []
    unpaired_requests = []
    unpaired_responses = []

    # Find requests with matching responses
    for unique_id, request in requests.items():
        if unique_id in responses:
            # Both request and response exist - validated pair
            validated_pairs.append({
                "chargerId": request["chargerId"],
                "uniqueId": unique_id,
                "request": request["message"],
                "response": responses[unique_id]["message"]
            })
        else:
            unpaired_requests.append(request)

    # Find responses without matching requests
    for unique_id, response in responses.items():
        if unique_id not in requests:
            unpaired_responses.append(response)

    return validated_pairs, unpaired_requests, unpaired_responses, unparseable


# --- Simple parsing function (backward compatibility) ---
def parse_txt_file(file_path):
    """
    Simple parsing function for backward compatibility with tests.
    Parses OCPP messages from file and returns a list of message dicts.
    Each dict contains: chargerId, uniqueId, message (as string)
    Skips malformed lines.
    """
    messages = []
    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            
            # Skip comment lines
            if line.startswith('#'):
                continue

            # Use regex to extract charger ID and OCPP message
            match = re.match(r'^([^\s:]+)\s*:\s*(.+)$', line)
            if not match:
                continue

            charger_id = match.group(1)
            ocpp_message_str = match.group(2)

            try:
                # Fix timestamps with spaces for JSON parsing
                fixed_str = re.sub(r'"(\d{8})\s+(\d+Z)"', r'"\1T\2"', ocpp_message_str)
                ocpp_message = json.loads(fixed_str)
            except json.JSONDecodeError:
                try:
                    ocpp_message = ast.literal_eval(ocpp_message_str)
                except (SyntaxError, ValueError):
                    continue

            # Extract message type and uniqueId
            if len(ocpp_message) < 2:
                continue

            message_type = ocpp_message[0]
            unique_id = ocpp_message[1]

            messages.append({
                "chargerId": charger_id,
                "uniqueId": unique_id,
                "message": str(ocpp_message)
            })

    return messages


# --- Publish to Kafka ---
def publish_to_kafka(validated_pairs, unpaired_requests, unpaired_responses, unparseable):
    """
    Publish messages to appropriate Kafka topics:
    - validated_pairs -> ocpp.messages (only the request part, as Call Responses are ACKs)
    - unpaired_requests + unpaired_responses + unparseable -> ocpp.malformed
    """
    # Increase queue size and add delivery callback
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "queue.buffering.max.messages": 200000,
        "queue.buffering.max.ms": 500,
        "message.timeout.ms": 30000,
    })

    def delivery_report(err, msg):
        if err is not None:
            print(f"❌ Message delivery failed: {err}")

    # Publish validated pairs (only the request - responses are just ACKs)
    valid_count = 0
    malformed_count = 0

    print(f"📊 Publishing {len(validated_pairs)} validated message pairs...")
    for i, pair in enumerate(validated_pairs):
        try:
            # Publish just the request to ocpp.messages
            message_data = {
                "chargerId": pair["chargerId"],
                "uniqueId": pair["uniqueId"],
                "message": str(pair["request"])
            }
            producer.produce(
                topic=VALID_TOPIC,
                value=json.dumps(message_data).encode('utf-8'),
                callback=delivery_report
            )
            valid_count += 1
            if valid_count % 5000 == 0:
                producer.poll(0)
                print(f"  ✅ Published {valid_count}/{len(validated_pairs)} validated messages...")
        except Exception as e:
            if "Queue full" in str(e):
                print(f"  ⚠️ Queue full at {valid_count}, waiting...")
                producer.poll(1.0)
                producer.produce(
                    topic=VALID_TOPIC,
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=delivery_report
                )
            else:
                print(f"  ❌ Failed to publish validated message: {e}")
                malformed_count += 1

    print(f"📊 Publishing {len(unpaired_requests) + len(unpaired_responses) + len(unparseable)} malformed/incomplete messages to {MALFORMED_TOPIC}...")
    
    # Publish unpaired requests
    for req in unpaired_requests:
        try:
            message_data = {
                "chargerId": req["chargerId"],
                "uniqueId": req["raw"].split(":")[1].split(",")[1].strip(),
                "message": str(req["message"]),
                "reason": "unpaired_request_no_response"
            }
            producer.produce(
                topic=MALFORMED_TOPIC,
                value=json.dumps(message_data).encode('utf-8'),
                callback=delivery_report
            )
            malformed_count += 1
        except Exception as e:
            if "Queue full" in str(e):
                producer.poll(1.0)
                producer.produce(
                    topic=MALFORMED_TOPIC,
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=delivery_report
                )
            else:
                print(f"  ❌ Failed to publish unpaired request: {e}")

    # Publish unpaired responses
    for resp in unpaired_responses:
        try:
            unique_id = resp["raw"].split(":")[1].split(",")[1].strip()
            message_data = {
                "chargerId": resp["chargerId"],
                "uniqueId": unique_id,
                "message": str(resp["message"]),
                "reason": "unpaired_response_no_request"
            }
            producer.produce(
                topic=MALFORMED_TOPIC,
                value=json.dumps(message_data).encode('utf-8'),
                callback=delivery_report
            )
            malformed_count += 1
        except Exception as e:
            if "Queue full" in str(e):
                producer.poll(1.0)
                producer.produce(
                    topic=MALFORMED_TOPIC,
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=delivery_report
                )
            else:
                print(f"  ❌ Failed to publish unpaired response: {e}")

    # Publish unparseable lines
    for item in unparseable:
        try:
            message_data = {
                "line": item["line"],
                "reason": item["reason"]
            }
            producer.produce(
                topic=MALFORMED_TOPIC,
                value=json.dumps(message_data).encode('utf-8'),
                callback=delivery_report
            )
            malformed_count += 1
        except Exception as e:
            if "Queue full" in str(e):
                producer.poll(1.0)
                producer.produce(
                    topic=MALFORMED_TOPIC,
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=delivery_report
                )
            else:
                print(f"  ❌ Failed to publish unparseable line: {e}")

    # Flush remaining messages
    print(f"🔄 Flushing remaining messages...")
    producer.flush()
    print(f"✨ Published {valid_count} validated messages to {VALID_TOPIC}")
    print(f"✨ Published {malformed_count} malformed messages to {MALFORMED_TOPIC}")

# --- Main ---
if __name__ == "__main__":
    print("📂 Parsing and validating .txt files...")
    
    # Initialize aggregated results
    all_validated_pairs = []
    all_unpaired_requests = []
    all_unpaired_responses = []
    all_unparseable = []
    
    # Process each file
    for file_path in TXT_FILE_PATHS:
        print(f"  📄 Processing {file_path}...")
        validated_pairs, unpaired_requests, unpaired_responses, unparseable = parse_and_validate_txt_file(file_path)
        all_validated_pairs.extend(validated_pairs)
        all_unpaired_requests.extend(unpaired_requests)
        all_unpaired_responses.extend(unpaired_responses)
        all_unparseable.extend(unparseable)
    
    print(f"📊 Validation results:")
    print(f"  ✅ Validated pairs (request+response): {len(all_validated_pairs)}")
    print(f"  ⚠️  Unpaired requests (no response): {len(all_unpaired_requests)}")
    print(f"  ⚠️  Unpaired responses (no request): {len(all_unpaired_responses)}")
    print(f"  ❌ Unparseable lines: {len(all_unparseable)}")
    
    publish_to_kafka(all_validated_pairs, all_unpaired_requests, all_unpaired_responses, all_unparseable)