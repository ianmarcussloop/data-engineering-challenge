from confluent_kafka import Consumer, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import json
import os

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")  # Use "kafka:29092" if running in Docker
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")  # Use "http://schema-registry:8081" if running in Docker
TOPIC_NAME = "ocpp.messages"
SCHEMA_SUBJECT = f"{TOPIC_NAME}-value"

# --- Schema Definition (must match the registered schema) ---
SCHEMA_DEFINITION = {
    "type": "record",
    "name": "OCPPMessage",
    "fields": [
        {"name": "uniqueId", "type": "string"},
        {"name": "message", "type": "string"}
    ]
}

# --- Consume Messages ---
def consume_messages():
    # Initialize Schema Registry and Avro Deserializer
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry,
        schema_str=json.dumps(SCHEMA_DEFINITION)
    )

    # Initialize Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "test-consumer-group",  # Consumer group ID
        "auto.offset.reset": "earliest",  # Start from the earliest message if no offset is stored
        "enable.auto.commit": False  # Disable auto-commit for manual control
    })

    # Subscribe to the topic
    consumer.subscribe([TOPIC_NAME])

    try:
        while True:
            # Poll for messages (timeout in seconds)
            msg = consumer.poll(1.0)

            if msg is None:
                continue  # No message received within timeout
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    print(f"📜 Reached end of partition {msg.partition()}")
                else:
                    print(f"❌ Consumer error: {msg.error()}")
                continue

            # Deserialize the message using Avro
            try:
                message = avro_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE)
                )
                print(f"✅ Consumed message:")
                print(f"   uniqueId: {message['uniqueId']}")
                print(f"   message: {message['message']}")
                print("---")
            except Exception as e:
                print(f"❌ Failed to deserialize message: {e}")

    except KeyboardInterrupt:
        print("🛑 Consumer interrupted by user.")
    finally:
        # Close the consumer
        consumer.close()
        print("🔚 Consumer closed.")

# --- Main ---
if __name__ == "__main__":
    print(f"📥 Starting Kafka consumer for topic '{TOPIC_NAME}'...")
    consume_messages()