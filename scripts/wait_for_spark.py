#!/usr/bin/env python3
"""
Wait for Spark streaming pipeline to be ready.

This script checks that:
1. The Spark process is running
2. Kafka is accessible
3. Optionally: wait for first messages to be processed
"""

import sys
import time
import subprocess
import os
from confluent_kafka.admin import AdminClient

def check_kafka_connected(bootstrap_servers, timeout=30):
    """Check if Kafka is accessible."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap_servers})
            topics = admin.list_topics(timeout=5).topics
            if topics:
                return True
        except Exception as e:
            print(f"[wait_for_spark] Kafka not ready: {e}")
            time.sleep(2)
    return False

def check_spark_process_running(timeout=30):
    """Check if Spark Python process is running."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = subprocess.run(
                ["pgrep", "-f", "spark_kafka_to_postgres"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                return True
        except:
            pass
        print("[wait_for_spark] Spark process not found, waiting...")
        time.sleep(2)
    return False

def check_spark_logs_show_started(log_file="/tmp/spark_test.log", timeout=60):
    """Wait for Spark to print its 'started' message."""
    start = time.time()
    last_size = 0
    while time.time() - start < timeout:
        try:
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    content = f.read()
                    if "=== Spark Streaming Pipeline Started ===" in content:
                        return True
                    # Print new log content
                    current_size = len(content)
                    if current_size > last_size:
                        print(content[last_size:])
                        last_size = current_size
        except:
            pass
        time.sleep(2)
    return False

def main():
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9093")
    spark_log = os.environ.get("SPARK_LOG", "/tmp/spark_test.log")
    
    print(f"[wait_for_spark] Waiting for Spark pipeline to be ready...")
    print(f"[wait_for_spark] Kafka broker: {kafka_broker}")
    print(f"[wait_for_spark] Spark log: {spark_log}")
    
    # Step 1: Wait for Spark process to start
    if not check_spark_process_running(timeout=30):
        print("[wait_for_spark] ERROR: Spark process never started")
        sys.exit(1)
    print("[wait_for_spark] ✓ Spark process is running")
    
    # Step 2: Wait for Kafka connection
    if not check_kafka_connected(kafka_broker, timeout=30):
        print("[wait_for_spark] ERROR: Cannot connect to Kafka")
        sys.exit(1)
    print("[wait_for_spark] ✓ Kafka is accessible")
    
    # Step 3: Wait for Spark to print started message
    if not check_spark_logs_show_started(spark_log, timeout=60):
        print("[wait_for_spark] ERROR: Spark never printed started message")
        sys.exit(1)
    print("[wait_for_spark] ✓ Spark pipeline has started")
    
    # Step 4: Optional - wait a bit more for first batch processing
    print("[wait_for_spark] Waiting for first batch processing...")
    time.sleep(10)
    
    print("[wait_for_spark] ✓ Spark is ready!")

if __name__ == "__main__":
    main()
