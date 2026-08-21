"""Setup test infrastructure - create test Kafka topics and PostgreSQL tables."""

from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine, text
import time

import os

KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")
POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")

def create_test_kafka_topics():
    """Create Kafka topics."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    
    topics_to_create = [
        ("ocpp.messages", 1, 1, {}),
        ("ocpp.active", 10, 1, {
            "cleanup.policy": "compact",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        }),
        ("ocpp.active.raw", 10, 1, {
            "cleanup.policy": "compact",
            "retention.ms": "259200000",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        })
    ]
    
    for topic_name, num_partitions, replication_factor, config in topics_to_create:
        if topic_name in admin.list_topics(timeout=5).topics:
            print(f"✓ Topic {topic_name} already exists")
        else:
            new_topic = NewTopic(
                topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=config
            )
            admin.create_topics([new_topic])
            time.sleep(2)  # Wait for topic creation
            print(f"✓ Created Kafka topic: {topic_name}")


def create_test_postgres_tables():
    """Create PostgreSQL tables."""
    engine = create_engine(POSTGRES_URL)
    
    with engine.connect() as conn:
        # Check if table already exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp_history'
        """)).fetchone()
        
        if result is None:
            # Create the table
            conn.execute(text("""
                CREATE TABLE public.ocpp_history (
                    sessionId TEXT PRIMARY KEY,
                    stationId TEXT NOT NULL,
                    transactionId TEXT NOT NULL,
                    startTime TIMESTAMP NOT NULL,
                    endTime TIMESTAMP NOT NULL,
                    duration INTEGER NOT NULL,
                    terminationReason TEXT,
                    totalEnergyConsumed FLOAT,
                    avgPower FLOAT,
                    maxPower FLOAT,
                    idTag TEXT,
                    connectorId INTEGER,
                    meterStart INTEGER,
                    meterStop INTEGER,
                    socStart FLOAT,
                    socEnd FLOAT,
                    voltageAvg FLOAT,
                    eventCount INTEGER DEFAULT 0
                )
            """))
            
            # Create indexes
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_stationid ON public.ocpp_history (stationId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_transactionid ON public.ocpp_history (transactionId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_starttime ON public.ocpp_history (startTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_endtime ON public.ocpp_history (endTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_terminationreason ON public.ocpp_history (terminationReason)"))
            
            conn.commit()
            print("✓ Created PostgreSQL table: ocpp.history")
        else:
            print("✓ Table ocpp.history already exists")


if __name__ == "__main__":
    print("Setting up infrastructure...")
    create_test_kafka_topics()
    create_test_postgres_tables()
    print("✓ Infrastructure setup complete!")
