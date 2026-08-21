"""Phase 4: End-to-end tests for complete session lifecycle.

These tests verify the full flow from Kafka message to PostgreSQL and Kafka tombstones.
They use _test suffixed resources that are auto-created by conftest.py.
They will FAIL initially if test infrastructure doesn't exist, then PASS once
the entire pipeline is working correctly.

Note: These tests require the Spark streaming pipeline to be running.
"""

import pytest
import json
import time
import psycopg2
from confluent_kafka import Producer, Consumer
import sys
import os

from tests.fixtures.ocpp_messages import start_transaction, meter_values, stop_transaction

# Read from environment or use defaults
TEST_KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:9092")
TEST_POSTGRES_URL = os.environ.get("TEST_POSTGRES_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")


class TestSessionLifecycle:
    """Test complete session lifecycle from StartTransaction to StopTransaction."""

    def test_active_session_in_kafka_not_postgres(self):
        """Active sessions (no StopTransaction) should be in Kafka only, not PostgreSQL."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        # Use test topics to avoid polluting production
        station_id = "charger1"
        transaction_id = "txn_e2e_001"
        session_id = f"{station_id}_{transaction_id}"  # Match pipeline's sessionId construction
        
        # Send StartTransaction
        start_msg = start_transaction(
            chargerId=station_id,
            transactionId=transaction_id,
            meterStart=1000,
            idTag="RFID123",
            timestamp="2025-01-01T10:00:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=start_msg)
        
        # Send MeterValues (makes session active)
        meter_msg = meter_values(
            chargerId=station_id,
            transactionId=transaction_id,
            power=22.5,
            energy=1050.0,
            soc=50.0,
            voltage=230.0,
            timestamp="2025-01-01T10:01:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=meter_msg)
        producer.flush(timeout=5)
        
        # Wait for Spark pipeline to process (this may take time)
        time.sleep(20)
        
        # Check: Session should be in ocpp.active
        consumer = Consumer({
            "bootstrap.servers": TEST_KAFKA_BROKER,
            "group.id": "e2e_test_active_check",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
        consumer.subscribe(["ocpp.active"])
        
        found_in_active = False
        msg = consumer.poll(timeout=5)
        while msg is not None:
            if msg.key() == session_id.encode('utf-8') and msg.value() is not None:
                found_in_active = True
                session_data = json.loads(msg.value())
                assert session_data["status"] in ["pending", "active"]
                break
            msg = consumer.poll(timeout=1)
        
        consumer.close()
        
        # Check: Session should be in ocpp.active.raw
        consumer = Consumer({
            "bootstrap.servers": TEST_KAFKA_BROKER,
            "group.id": "e2e_test_raw_check",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False
        })
        consumer.subscribe(["ocpp.active.raw"])
        
        found_in_raw = False
        msg = consumer.poll(timeout=5)
        while msg is not None:
            if msg.key() == session_id.encode('utf-8') and msg.value() is not None:
                found_in_raw = True
                break
            msg = consumer.poll(timeout=1)
        
        consumer.close()
        
        # Check: Session should NOT be in PostgreSQL ocpp.history
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM "ocpp"."history" WHERE "sessionId" = %s', (session_id,))
        in_postgres = cursor.fetchone() is not None
        conn.close()
        
        assert found_in_active, "Active session should be in ocpp.active"
        assert found_in_raw, "Active session should be in ocpp.active.raw"
        assert not in_postgres, "Active session should NOT be in ocpp.history"

    def test_completed_session_in_postgres_not_kafka(self):
        """Completed sessions should be in PostgreSQL and removed from Kafka."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        station_id = "charger2"
        transaction_id = "txn_e2e_002"
        session_id = f"{station_id}_{transaction_id}"  # Match pipeline's sessionId construction
        
        # Send complete session lifecycle
        start_msg = start_transaction(
            chargerId=station_id,
            transactionId=transaction_id,
            meterStart=2000,
            idTag="RFID456",
            timestamp="2025-01-01T11:00:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=start_msg)
        
        meter_msg = meter_values(
            chargerId=station_id,
            transactionId=transaction_id,
            power=25.0,
            energy=1100.0,
            soc=60.0,
            voltage=240.0,
            timestamp="2025-01-01T11:01:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=meter_msg)
        
        stop_msg = stop_transaction(
            chargerId=station_id,
            transactionId=transaction_id,
            meterStop=2100,
            reason="EVDriverDisconnected",
            timestamp="2025-01-01T11:05:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=stop_msg)
        producer.flush(timeout=5)
        
        # Wait for Spark pipeline to process and send tombstones
        # Increased to 35 seconds to allow for compaction of tombstones
        time.sleep(35)
        
        # Check: Session should be in PostgreSQL
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM "ocpp"."history" WHERE "sessionId" = %s', (session_id,))
        pg_session = cursor.fetchone()
        conn.close()
        
        assert pg_session is not None, "Completed session should be in PostgreSQL ocpp.history"
        
        # Check: Session should NOT be in ocpp.active (tombstoned)
        # Use a fresh consumer with latest offset to see if the session is still present
        found_in_active = True
        for _ in range(3):  # Retry up to 3 times
            consumer = Consumer({
                "bootstrap.servers": TEST_KAFKA_BROKER,
                "group.id": f"e2e_test_completed_active_check_{_}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False
            })
            consumer.subscribe(["ocpp.active"])
            
            found_in_active = False
            msg = consumer.poll(timeout=5)
            while msg is not None:
                if msg.key() == session_id.encode('utf-8') and msg.value() is not None:
                    found_in_active = True
                    break
                msg = consumer.poll(timeout=1)
            
            consumer.close()
            
            if not found_in_active:
                break
            time.sleep(3)
        
        # If we still found it after retries, wait more and try from earliest
        if found_in_active:
            consumer = Consumer({
                "bootstrap.servers": TEST_KAFKA_BROKER,
                "group.id": "e2e_test_completed_active_check_final",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False
            })
            consumer.subscribe(["ocpp.active"])
            
            last_value_for_key = None
            msg = consumer.poll(timeout=10)
            while msg is not None:
                if msg.key() == session_id.encode('utf-8'):
                    last_value_for_key = msg.value()
                msg = consumer.poll(timeout=1)
            
            consumer.close()
            found_in_active = (last_value_for_key is not None)
        
        # Check: Session should NOT be in ocpp.active.raw (tombstoned)
        found_in_raw = True
        for _ in range(3):  # Retry up to 3 times
            consumer = Consumer({
                "bootstrap.servers": TEST_KAFKA_BROKER,
                "group.id": f"e2e_test_completed_raw_check_{_}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False
            })
            consumer.subscribe(["ocpp.active.raw"])
            
            found_in_raw = False
            msg = consumer.poll(timeout=5)
            while msg is not None:
                if msg.key() == session_id.encode('utf-8') and msg.value() is not None:
                    found_in_raw = True
                    break
                msg = consumer.poll(timeout=1)
            
            consumer.close()
            
            if not found_in_raw:
                break
            time.sleep(3)
        
        # If we still found it after retries, wait more and try from earliest
        if found_in_raw:
            consumer = Consumer({
                "bootstrap.servers": TEST_KAFKA_BROKER,
                "group.id": "e2e_test_completed_raw_check_final",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False
            })
            consumer.subscribe(["ocpp.active.raw"])
            
            last_value_for_key = None
            msg = consumer.poll(timeout=10)
            while msg is not None:
                if msg.key() == session_id.encode('utf-8'):
                    last_value_for_key = msg.value()
                msg = consumer.poll(timeout=1)
            
            consumer.close()
            found_in_raw = (last_value_for_key is not None)
        
        assert not found_in_active, "Completed session should NOT be in ocpp.active (tombstoned)"
        assert not found_in_raw, "Completed session should NOT be in ocpp.active.raw (tombstoned)"


class TestSessionDataCorrectness:
    """Test that session data is correctly written to PostgreSQL."""

    def test_session_data_populated_correctly(self):
        """Completed session in PostgreSQL should have all fields correctly populated."""
        producer = Producer({"bootstrap.servers": TEST_KAFKA_BROKER})
        
        station_id = "charger3"
        transaction_id = "txn_e2e_003"
        session_id = f"{station_id}_{transaction_id}"  # Match pipeline's sessionId construction
        meter_start = 3000
        meter_stop = 3500
        
        # Send complete session
        start_msg = start_transaction(
            chargerId=station_id,
            transactionId=transaction_id,
            meterStart=meter_start,
            idTag="RFID789",
            connectorId=1,
            timestamp="2025-01-01T12:00:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=start_msg)
        
        # Send multiple MeterValues
        for i in range(3):
            meter_msg = meter_values(
                chargerId=station_id,
                transactionId=transaction_id,
                power=30.0 + i,
                energy=3100.0 + i * 10,
                soc=70.0 + i * 5,
                voltage=250.0,
                timestamp=f"2025-01-01T12:0{i+1}:00.000Z",
                wrap_for_kafka=True
            )
            producer.produce("ocpp.messages", key=session_id, value=meter_msg)
        
        stop_msg = stop_transaction(
            chargerId=station_id,
            transactionId=transaction_id,
            meterStop=meter_stop,
            reason="RemoteStop",
            timestamp="2025-01-01T12:05:00.000Z",
            wrap_for_kafka=True
        )
        producer.produce("ocpp.messages", key=session_id, value=stop_msg)
        producer.flush(timeout=5)
        
        # Wait for processing - increased to 60 seconds for all messages to be processed
        time.sleep(60)
        
        # Check PostgreSQL
        conn = psycopg2.connect(TEST_POSTGRES_URL, connect_timeout=5)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT "sessionId", "stationId", "transactionId", "meterStart", "meterStop", 
                   "terminationReason", "duration", "totalEnergyConsumed", "eventCount"
            FROM "ocpp"."history" WHERE "sessionId" = %s
        ''', (session_id,))
        
        session = cursor.fetchone()
        conn.close()
        
        assert session is not None, "Session should exist in PostgreSQL"
        
        # Unpack the session data
        (db_session_id, db_station_id, db_transaction_id, db_meter_start, 
         db_meter_stop, db_reason, db_duration, db_energy, db_event_count) = session
        
        # Verify all fields
        assert db_session_id == session_id, f"Expected sessionId {session_id}, got {db_session_id}"
        assert db_station_id == station_id, f"Expected stationId {station_id}, got {db_station_id}"
        assert db_transaction_id == transaction_id, f"Expected transactionId {transaction_id}, got {db_transaction_id}"
        assert db_meter_start == meter_start, f"Expected meterStart {meter_start}, got {db_meter_start}"
        assert db_meter_stop == meter_stop, f"Expected meterStop {meter_stop}, got {db_meter_stop}"
        assert db_reason == "RemoteStop", f"Expected reason RemoteStop, got {db_reason}"
        assert db_event_count >= 5, f"Expected eventCount >= 5 (1 Start + 3 MeterValues + 1 Stop), got {db_event_count}"
        assert abs(db_duration - 300) <= 5, f"Expected duration ~300, got {db_duration}"  
        assert db_energy is not None and db_energy > 0, f"Expected positive energy, got {db_energy}"
