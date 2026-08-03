# postgres/scripts/kafka_to_postgres.py
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from sqlmodel import Session, create_engine
from postgres.schema.charger_session import ChargerSession
from typing import Dict, Any, Optional, TypedDict
import json, ast, time
from datetime import datetime
import os

# --- Type definitions ---
class SessionData(TypedDict, total=False):
    stationId: str
    startTime: Optional[datetime]
    endTime: Optional[datetime]
    power_values: list[float]
    eventCount: int
    status: Optional[str]
    terminationReason: Optional[str]

# --- Kafka Config ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "ocpp.messages"

# --- DB Config ---
DB_URL = os.getenv("DATABASE_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")
engine = create_engine(DB_URL)

# --- Session Tracking ---
active_sessions: Dict[str, SessionData] = {}

def parse_ocpp_message(raw_message_str: str) -> Optional[Dict[str, Any]]:
    """Parse OCPP message string into structured data.
    
    Checks len(ocpp_message) < 3 because a valid OCPP message must have at least:
    - message_type (int): 2 for request, 3 for response
    - unique_id (str): UUID of the message  
    - action (str): e.g., "MeterValues", "StopTransaction"
    Any message with fewer than 3 elements is malformed.
    """
    try:
        ocpp_message = ast.literal_eval(raw_message_str)
        if len(ocpp_message) < 3:
            return None
        message_type = ocpp_message[0]
        unique_id = ocpp_message[1]
        action = ocpp_message[2]
        payload = ocpp_message[3] if len(ocpp_message) > 3 else {}
        return {"uniqueId": unique_id, "action": action, "payload": payload}
    except:
        return None

def process_meter_values(session_data: SessionData, payload: Dict[str, Any]) -> None:
    """Extract power values and timestamps from MeterValues."""
    for mv in payload.get("meterValue", []):
        ts_str = mv.get("timestamp")
        if ts_str:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if not session_data["startTime"] or ts < session_data["startTime"]:
                session_data["startTime"] = ts
            session_data["endTime"] = ts
            for sv in mv.get("sampledValue", []):
                if sv.get("measurand") == "Power.Active.Import":
                    try:
                        session_data["power_values"].append(float(sv["value"]))
                    except (ValueError, KeyError):
                        pass
    session_data["eventCount"] += 1

def process_stop_transaction(session_data: SessionData, payload: Dict[str, Any]) -> None:
    """Handle session end."""
    if "timestamp" in payload:
        session_data["endTime"] = datetime.fromisoformat(payload["timestamp"].replace("Z", "+00:00"))
    session_data["status"] = "ended"
    session_data["terminationReason"] = payload.get("reason")
    session_data["eventCount"] += 1

def calculate_session_metadata(session_data: SessionData) -> None:
    """Calculate derived fields before DB write."""
    if session_data["power_values"] and session_data["startTime"] and session_data["endTime"]:
        avg_power = sum(session_data["power_values"]) / len(session_data["power_values"])
        duration = (session_data["endTime"] - session_data["startTime"]).total_seconds()
        session_data["duration"] = int(duration)
        session_data["totalEnergyConsumed"] = round(avg_power * (duration / 3600), 3)

def flush_session(transaction_id: str, charger_id: str, session_data: SessionData) -> None:
    """Write completed session to database."""
    calculate_session_metadata(session_data)
    session = ChargerSession(
        sessionId=f"{charger_id}_{session_data['startTime'].isoformat()}" if session_data.get("startTime") else f"{charger_id}_unknown",
        stationId=charger_id,
        status=session_data.get("status"),
        startTime=session_data.get("startTime"),
        endTime=session_data.get("endTime"),
        duration=session_data.get("duration"),
        totalEnergyConsumed=session_data.get("totalEnergyConsumed"),
        eventCount=session_data.get("eventCount", 0),
        terminationReason=session_data.get("terminationReason")
    )
    try:
        with Session(engine) as db_session:
            db_session.add(session)
            db_session.commit()
    except Exception as e:
        print(f"DB write failed: {e}")
    finally:
        active_sessions.pop(transaction_id, None)

def run_consumer() -> None:
    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client=schema_registry)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "postgres-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })
    consumer.subscribe([TOPIC])

    try:
        print("Consumer started. Waiting for messages...")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            try:
                message = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                if not message:
                    continue

                charger_id = message.get("chargerId", "unknown")
                parsed = parse_ocpp_message(message.get("message", ""))
                if not parsed:
                    continue

                transaction_id = parsed["payload"].get("transactionId", message.get("uniqueId", "unknown"))
                print(f"✉️  Received: {charger_id} | {parsed['action']} | transaction={transaction_id}")

                if transaction_id not in active_sessions:
                    active_sessions[transaction_id] = {
                        "stationId": charger_id,
                        "startTime": None,
                        "endTime": None,
                        "power_values": [],
                        "eventCount": 0,
                        "status": None,
                        "terminationReason": None
                    }

                session_data = active_sessions[transaction_id]

                if parsed["action"] == "MeterValues":
                    process_meter_values(session_data, parsed["payload"])
                elif parsed["action"] == "StopTransaction":
                    process_stop_transaction(session_data, parsed["payload"])
                    flush_session(transaction_id, charger_id, session_data)
                elif parsed["action"] in ["StatusNotification", "Heartbeat"]:
                    session_data["eventCount"] += 1

            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"Message processing error: {e}")

    except KeyboardInterrupt:
        print("\nShutting down gracefully...")
        # Flush with timeout
        for tid, data in list(active_sessions.items()):
            if data.get("startTime"):
                flush_session(tid, data.get("stationId", "unknown"), data)
        # Close consumer with timeout
        print("Closing consumer...")
        consumer.close()
        print("Consumer stopped.")
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()