#!/usr/bin/env python3
"""
Cleanup Script for Data Engineering Challenge

This script cleans up either the test environment or the main project environment
by truncating all data from PostgreSQL tables and emptying all Kafka topics.

Usage:
    python scripts/cleanup.py              # Clean up main project environment
    python scripts/cleanup.py --test       # Clean up test environment
    python scripts/cleanup.py --main       # Clean up main project environment (explicit)
    python scripts/cleanup.py --help       # Show help
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException
import psycopg2
from sqlalchemy import create_engine, text


# Colors for output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class CleanupScript:
    """Main cleanup script class."""
    
    def __init__(self):
        self.project_dir = Path(__file__).parent.parent.absolute()
        self.scripts_dir = Path(__file__).parent.absolute()
        
    def log_info(self, message: str) -> None:
        """Log an info message."""
        print(f"{Colors.BLUE}[INFO]{Colors.NC} {message}")
    
    def log_success(self, message: str) -> None:
        """Log a success message."""
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
    
    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
    
    def log_error(self, message: str) -> None:
        """Log an error message."""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
    
    # =========================================================================
    # PostgreSQL Configuration
    # =========================================================================
    
    # Main project configuration
    MAIN_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"
    MAIN_TABLES = [
        ("ocpp", "history"),           # ocpp.history table
        ("public", "charger_session"),  # charger_session table
    ]
    
    # Test environment configuration
    TEST_POSTGRES_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"
    TEST_TABLES = [
        ("public", "ocpp_history_test"),  # test table
    ]
    
    # =========================================================================
    # Kafka Configuration
    # =========================================================================
    
    KAFKA_BROKER = "localhost:9092"
    
    # Main project topics
    MAIN_TOPICS = [
        "ocpp.messages",
        "ocpp.active.raw",
        "ocpp.active",
    ]
    
    # Test topics
    TEST_TOPICS = [
        "ocpp.messages_test",
        "ocpp.active_test",
        "ocpp.active.raw_test",
    ]
    
    def check_postgres_connection(self, db_url: str) -> bool:
        """Check if PostgreSQL is accessible."""
        self.log_info("Checking PostgreSQL connection...")
        try:
            conn = psycopg2.connect(db_url, connect_timeout=5)
            conn.close()
            self.log_success("PostgreSQL is accessible")
            return True
        except Exception as e:
            self.log_error(f"PostgreSQL connection failed: {e}")
            return False
    
    def check_kafka_connection(self) -> bool:
        """Check if Kafka is accessible."""
        self.log_info("Checking Kafka connection...")
        try:
            admin = AdminClient({"bootstrap.servers": self.KAFKA_BROKER})
            admin.list_topics(timeout=5)
            self.log_success("Kafka is accessible")
            return True
        except Exception as e:
            self.log_error(f"Kafka connection failed: {e}")
            return False
    
    def truncate_postgres_tables(self, db_url: str, tables: List[Tuple[str, str]]) -> bool:
        """Truncate all specified PostgreSQL tables."""
        self.log_info(f"Truncating {len(tables)} PostgreSQL table(s)...")
        
        try:
            engine = create_engine(db_url)
            
            for schema, table_name in tables:
                full_table_name = f"{schema}.{table_name}"
                self.log_info(f"  Truncating table: {full_table_name}")
                
                with engine.connect() as conn:
                    # Check if table exists
                    result = conn.execute(text(f"""
                        SELECT table_name FROM information_schema.tables 
                        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
                    """)).fetchone()
                    
                    if result is None:
                        self.log_warning(f"  Table {full_table_name} does not exist, skipping")
                        continue
                    
                    # Truncate the table (with CASCADE to handle foreign key constraints)
                    conn.execute(text(f'TRUNCATE TABLE {schema}.{table_name} CASCADE'))
                    conn.commit()
                    self.log_success(f"  Truncated table: {full_table_name}")
            
            return True
            
        except Exception as e:
            self.log_error(f"Failed to truncate PostgreSQL tables: {e}")
            return False
    
    def empty_kafka_topics(self, topics: List[str]) -> bool:
        """Empty all specified Kafka topics."""
        self.log_info(f"Emptying {len(topics)} Kafka topic(s)...")
        
        try:
            admin = AdminClient({"bootstrap.servers": self.KAFKA_BROKER})
            
            # Get existing topics
            existing_topics = admin.list_topics(timeout=10).topics
            
            for topic_name in topics:
                if topic_name in existing_topics:
                    self.log_info(f"  Emptying topic: {topic_name}")
                    
                    # Create a temporary consumer to read and acknowledge all messages
                    # This effectively empties the topic by consuming all messages
                    from confluent_kafka import Consumer
                    
                    consumer = Consumer({
                        'bootstrap.servers': self.KAFKA_BROKER,
                        'group.id': f'cleanup_{topic_name}_{int(time.time())}',
                        'auto.offset.reset': 'earliest',
                        'enable.auto.commit': False
                    })
                    
                    consumer.subscribe([topic_name])
                    
                    # Poll until no more messages
                    msg_count = 0
                    while True:
                        msg = consumer.poll(timeout=1.0)
                        if msg is None:
                            break
                        if msg.error():
                            raise KafkaException(msg.error())
                        msg_count += 1
                    
                    consumer.close()
                    self.log_success(f"  Emptied topic: {topic_name} (consumed {msg_count} messages)")
                else:
                    self.log_warning(f"  Topic {topic_name} does not exist, skipping")
            
            return True
            
        except Exception as e:
            self.log_error(f"Failed to empty Kafka topics: {e}")
            return False
    
    def cleanup_main_environment(self) -> bool:
        """Clean up the main project environment."""
        self.log_info("=" * 60)
        self.log_info("Cleaning up MAIN project environment")
        self.log_info("=" * 60)
        
        # Check connections
        if not self.check_postgres_connection(self.MAIN_POSTGRES_URL):
            self.log_error("PostgreSQL is not accessible. Cannot clean up main environment.")
            return False
        
        if not self.check_kafka_connection():
            self.log_error("Kafka is not accessible. Cannot clean up main environment.")
            return False
        
        # Truncate PostgreSQL tables
        if not self.truncate_postgres_tables(self.MAIN_POSTGRES_URL, self.MAIN_TABLES):
            self.log_error("Failed to truncate main PostgreSQL tables")
            return False
        
        # Empty Kafka topics
        if not self.empty_kafka_topics(self.MAIN_TOPICS):
            self.log_error("Failed to empty main Kafka topics")
            return False
        
        self.log_success("Main project environment cleaned up successfully!")
        return True
    
    def cleanup_test_environment(self) -> bool:
        """Clean up the test environment."""
        self.log_info("=" * 60)
        self.log_info("Cleaning up TEST environment")
        self.log_info("=" * 60)
        
        # Check connections
        if not self.check_postgres_connection(self.TEST_POSTGRES_URL):
            self.log_error("PostgreSQL is not accessible. Cannot clean up test environment.")
            return False
        
        if not self.check_kafka_connection():
            self.log_error("Kafka is not accessible. Cannot clean up test environment.")
            return False
        
        # Truncate PostgreSQL tables
        if not self.truncate_postgres_tables(self.TEST_POSTGRES_URL, self.TEST_TABLES):
            self.log_error("Failed to truncate test PostgreSQL tables")
            return False
        
        # Empty Kafka topics
        if not self.empty_kafka_topics(self.TEST_TOPICS):
            self.log_error("Failed to empty test Kafka topics")
            return False
        
        self.log_success("Test environment cleaned up successfully!")
        return True
    
    def cleanup_all(self) -> bool:
        """Clean up both main and test environments."""
        self.log_info("=" * 60)
        self.log_info("Cleaning up BOTH main and test environments")
        self.log_info("=" * 60)
        
        main_success = self.cleanup_main_environment()
        test_success = self.cleanup_test_environment()
        
        if main_success and test_success:
            self.log_success("Both environments cleaned up successfully!")
            return True
        else:
            self.log_error("Failed to clean up one or both environments")
            return False
    
    def run(self, args: argparse.Namespace) -> None:
        """Main run method."""
        # Check which environment to clean up
        if args.all:
            success = self.cleanup_all()
        elif args.test:
            success = self.cleanup_test_environment()
        else:
            # Default to main environment
            success = self.cleanup_main_environment()
        
        if not success:
            sys.exit(1)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Clean up Data Engineering Challenge environments"
    )
    
    # Add mutually exclusive group for environment selection
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--test",
        action="store_true",
        help="Clean up test environment only"
    )
    group.add_argument(
        "--main",
        action="store_true",
        help="Clean up main project environment (default)"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Clean up both main and test environments"
    )
    
    args = parser.parse_args()
    
    script = CleanupScript()
    script.run(args)


if __name__ == "__main__":
    main()
