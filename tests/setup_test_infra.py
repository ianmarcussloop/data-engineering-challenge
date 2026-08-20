"""Setup test infrastructure - create test Kafka topics and PostgreSQL tables."""

from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import create_engine, text
import time

KAFKA_BROKER = "localhost:9092"
POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"

def create_test_kafka_topics():
    """Create test Kafka topics."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    
    topics_to_create = [
        ("ocpp.messages_test", 1, 1, {}),
        ("ocpp.active_test", 10, 1, {
            "cleanup.policy": "compact",
            "segment.ms": "60000",
            "min.compaction.lag.ms": "1000"
        }),
        ("ocpp.active.raw_test", 10, 1, {
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
    """Create test PostgreSQL table."""
    engine = create_engine(POSTGRES_URL)
    
    with engine.connect() as conn:
        # Check if test table already exists
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'ocpp.history_test'
        """)).fetchone()
        
        if result is None:
            # Create the test table
            conn.execute(text("""
                CREATE TABLE public.ocpp_history_test (
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
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_stationId ON public.ocpp_history_test (stationId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_transactionId ON public.ocpp_history_test (transactionId)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_startTime ON public.ocpp_history_test (startTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_endTime ON public.ocpp_history_test (endTime)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ocpp_history_test_terminationReason ON public.ocpp_history_test (terminationReason)"))
            
            conn.commit()
            print("✓ Created PostgreSQL table: ocpp.history_test")
        else:
            print("✓ Table ocpp.history_test already exists")


if __name__ == "__main__":
    print("Setting up test infrastructure...")
    create_test_kafka_topics()
    create_test_postgres_tables()
    print("✓ Test infrastructure setup complete!")
