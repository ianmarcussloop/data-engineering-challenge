from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
import json
import os

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = "ocpp.messages"
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMA_SUBJECT = f"{TOPIC_NAME}-value"  # Subject for the schema

# Debug: Print configuration to verify environment variables are being read
import sys
print(f"[DEBUG create_topics.py] KAFKA_BROKER={KAFKA_BROKER}, SCHEMA_REGISTRY_URL={SCHEMA_REGISTRY_URL}")
sys.stdout.flush()

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
    import time
    from confluent_kafka import KafkaException
    
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    # Define all topics to create
    topics = [
        # Main raw OCPP messages topic - only validated request/response pairs
        ("ocpp.messages", 1, 1, {}),
        # Malformed messages topic - for messages without complete pairs
        ("ocpp.malformed", 1, 1, {}),
        
        # NEW: Normalized topic - all processed messages for ACTIVE sessions only
        ("ocpp.active.raw", 10, 1, {
            "cleanup.policy": "compact",
            "retention.ms": "259200000",  # 3 days - short for active debugging
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        }),
        
        # NEW: Compacted topic for ACTIVE sessions ONLY
        ("ocpp.active", 10, 1, {
            "cleanup.policy": "compact",
            "segment.ms": "60000",           # 1 minute
            "min.compaction.lag.ms": "1000" # Allow consumers 1s to catch up
        })
    ]
    
    for topic_name, num_partitions, replication_factor, configs in topics:
        if topic_name in admin_client.list_topics(timeout=10).topics:
            print(f"Topic {topic_name} already exists.")
        else:
            new_topic = NewTopic(
                topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=configs
            )
            # Create topic and wait for it to be created
            future = admin_client.create_topics([new_topic])
            # Wait for the topic to be created
            try:
                future[topic_name].result(timeout=30)
                print(f"Created Kafka topic: {topic_name}")
            except KafkaException as e:
                print(f"Error creating topic {topic_name}: {e}")
                raise

# TODO: add recreate_kafka_topic() function to delete and recreate the topic if needed? 

# --- Register Schema ---
def register_schema():
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Create a Schema object from the schema definition
    schema = Schema(
        schema_type="AVRO",
        schema_str=json.dumps(SCHEMA_DEFINITION)
    )

    # List of subjects to register
    subjects = ["ocpp.messages-value"]
    
    for subject in subjects:
        try:
            # Register the schema using the Schema object
            schema_id = schema_registry.register_schema(
                subject_name=subject,
                schema=schema
            )
            print(f"✅ Registered schema for {subject} (ID: {schema_id})")
        except Exception as e:
            if "already exists" in str(e):
                print(f"⚠️ Schema for {subject} already exists.")
            else:
                print(f"❌ Failed to register schema for {subject}: {e}")

# --- Main ---
if __name__ == "__main__":
    print("🚀 Initializing Kafka topic and schema...")
    create_kafka_topic()
    register_schema()
    print("✨ Done!")