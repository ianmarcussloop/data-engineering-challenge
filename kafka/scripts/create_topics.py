from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
import json
import os

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ocpp.messages"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMA_SUBJECT = f"{TOPIC_NAME}-value"  # Subject for the schema

# --- Schema Definition ---
SCHEMA_DEFINITION = {
    "type": "record",
    "name": "OCPPMessage",
    "fields": [
        {"name": "chargerId", "type": "string"},
        {"name": "uniqueId", "type": "string"},
        {"name": "message", "type": "string"}
    ]
}

# --- Create Kafka Topic ---
def create_kafka_topic():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    # Check if topic already exists
    topic_metadata = admin_client.list_topics(timeout=10)
    if TOPIC_NAME in topic_metadata.topics:
        print(f"⚠️ Topic {TOPIC_NAME} already exists.")
        return

    # Create the topic
    new_topic = NewTopic(
        TOPIC_NAME,
        num_partitions=1,
        replication_factor=1,
    )
    admin_client.create_topics([new_topic])
    print(f"✅ Created Kafka topic: {TOPIC_NAME}")

# TODO: add recreate_kafka_topic() function to delete and recreate the topic if needed? 

# --- Register Schema ---
def register_schema():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Create a Schema object from the schema definition
    schema = Schema(
        schema_type="AVRO",
        schema_str=json.dumps(SCHEMA_DEFINITION)
    )

    try:
        # Register the schema using the Schema object
        schema_id = schema_registry.register_schema(
            subject_name=SCHEMA_SUBJECT,
            schema=schema
        )
        print(f"✅ Registered schema for {SCHEMA_SUBJECT} (ID: {schema_id})")
    except Exception as e:
        if "already exists" in str(e):
            print(f"⚠️ Schema for {SCHEMA_SUBJECT} already exists.")
        else:
            print(f"❌ Failed to register schema: {e}")

# --- Main ---
if __name__ == "__main__":
    print("🚀 Initializing Kafka topic and schema...")
    create_kafka_topic()
    register_schema()
    print("✨ Done!")