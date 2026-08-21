# spark/scripts/spark_kafka_to_postgres.py
"""
Spark Streaming pipeline: Kafka -> Kafka (active topics) + PostgreSQL (history)

Implements the architecture from plan.md:
- ocpp.active: Compacted Kafka topic with latest state of ACTIVE sessions only
- ocpp.active.raw: Compacted Kafka topic with normalized messages for ACTIVE sessions only
- ocpp.history: PostgreSQL table with COMPLETED sessions only
- Tombstones sent to both Kafka topics when sessions complete
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, sum, count, max, min, avg, unix_timestamp, date_format, to_timestamp, concat_ws, udf, from_json
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import *
from typing import Optional, Dict, Any
import ast
import json
import os

# --- Configuration ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/ev_coorp")
POSTGRES_USER = os.getenv("POSTGRES_USER", "ev_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ev_password")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "./spark-checkpoints")

message_schema = "chargerId STRING, uniqueId STRING, message STRING"


# ============================================================================
# PARSING FUNCTIONS
# ============================================================================

def parse_ocpp_message(raw: str) -> Optional[Dict[str, Any]]:
    try:
        msg = ast.literal_eval(raw)
        if len(msg) < 3:
            return None
        return {"action": msg[2], "payload": msg[3] if len(msg) > 3 else {}, "uniqueId": msg[1]}
    except:
        return None

def get_transaction_id(message: str) -> Optional[str]:
    try:
        msg = ast.literal_eval(message)
        return msg[3].get("transactionId") if len(msg) > 3 and isinstance(msg[3], dict) else None
    except:
        return None

def parse_timestamp(message: str) -> Optional[str]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) < 3:
            return None
        action = msg[2]
        payload = msg[3] if len(msg) > 3 and isinstance(msg[3], dict) else {}
        
        # First, try to get timestamp from top-level payload (works for StartTransaction, StopTransaction, etc.)
        ts = payload.get("timestamp") if isinstance(payload, dict) else None
        if ts:
            return ts.replace("Z", "+00:00")
        
        # For MeterValues, timestamp is nested in meterValue[0].timestamp
        if action == "MeterValues" and isinstance(payload, dict):
            mv_list = payload.get("meterValue", [])
            if mv_list and len(mv_list) > 0 and isinstance(mv_list[0], dict):
                ts = mv_list[0].get("timestamp")
                if ts:
                    return ts.replace("Z", "+00:00")
        
        return None
    except Exception as e:
        # Debug logging for diagnosis
        print(f"[WARN] parse_timestamp failed for message: {message[:100]}... Error: {e}")
        return None

def parse_action(message: str) -> Optional[str]:
    try:
        msg = ast.literal_eval(message)
        return msg[2] if len(msg) > 2 else None
    except:
        return None

def parse_power(message: str) -> Optional[float]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            return extract_power_value(msg[3])
        return None
    except:
        return None

def extract_power_value(payload: Dict[str, Any]) -> Optional[float]:
    if payload is None:
        return None
    for mv in payload.get("meterValue", []):
        for sv in mv.get("sampledValue", []):
            if sv.get("measurand") == "Power.Active.Import":
                try:
                    return float(sv["value"])
                except:
                    pass
    return None

def parse_meter_start(message: str) -> Optional[int]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            ms = msg[3].get("meterStart")
            return int(ms) if ms is not None else None
        return None
    except:
        return None

def parse_meter_stop(message: str) -> Optional[int]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            ms = msg[3].get("meterStop")
            return int(ms) if ms is not None else None
        return None
    except:
        return None

def parse_id_tag(message: str) -> Optional[str]:
    try:
        msg = ast.literal_eval(message)
        return msg[3].get("idTag") if len(msg) > 3 and isinstance(msg[3], dict) else None
    except:
        return None

def parse_connector_id(message: str) -> Optional[int]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            ci = msg[3].get("connectorId")
            return int(ci) if ci is not None else None
        return None
    except:
        return None

def parse_soc(message: str) -> Optional[float]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            payload = msg[3]
            for mv in payload.get("meterValue", []):
                for sv in mv.get("sampledValue", []):
                    if sv.get("measurand") == "Battery.SOC":
                        return float(sv["value"])
        return None
    except:
        return None

def parse_voltage(message: str) -> Optional[float]:
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            payload = msg[3]
            for mv in payload.get("meterValue", []):
                for sv in mv.get("sampledValue", []):
                    if sv.get("measurand") == "Voltage":
                        return float(sv["value"])
        return None
    except:
        return None

def parse_reason(message: str) -> Optional[str]:
    try:
        msg = ast.literal_eval(message)
        return msg[3].get("reason") if len(msg) > 3 and isinstance(msg[3], dict) else None
    except:
        return None

def is_stop_action(action: str) -> bool:
    return action in ["StopTransaction", "RemoteStopTransaction"]


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def run_pipeline():
    print("[DEBUG] Starting run_pipeline()")
    print("[DEBUG] Setting SPARK_LOCAL_IP=127.0.0.1 via os.environ")
    import os
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    spark = SparkSession.builder \
        .appName("OCPPtoKafkaAndPostgres") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.ui.port", "4042") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3") \
        .config("spark.driver.extraJavaOptions", "-Dspark.local.ip=127.0.0.1 -Djava.net.preferIPv4Stack=true") \
        .getOrCreate()
    
    # Suppress Kafka threading warnings (KAFKA-1894)
    spark.sparkContext.setLogLevel("ERROR")
    print("=== Spark Streaming Pipeline Started ===")
    print("Subscribing to Kafka topics: ocpp.messages")

    # Register UDFs
    get_transaction_id_udf = udf(get_transaction_id, StringType())
    parse_timestamp_udf = udf(parse_timestamp, StringType())
    parse_action_udf = udf(parse_action, StringType())
    parse_power_udf = udf(parse_power, FloatType())
    parse_meter_start_udf = udf(parse_meter_start, IntegerType())
    parse_meter_stop_udf = udf(parse_meter_stop, IntegerType())
    parse_id_tag_udf = udf(parse_id_tag, StringType())
    parse_connector_id_udf = udf(parse_connector_id, IntegerType())
    parse_soc_udf = udf(parse_soc, FloatType())
    parse_voltage_udf = udf(parse_voltage, FloatType())
    parse_reason_udf = udf(parse_reason, StringType())
    is_stop_action_udf = udf(is_stop_action, BooleanType())

    # Read from Kafka - include both production and test topics
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "ocpp.messages") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON and extract fields
    # Message format in value: {"chargerId": "...", "uniqueId": "...", "message": "[2, ...]"}
    # Key is the uniqueId
    parsed_df = kafka_df.select(
        col("topic"),
        col("key").cast("string").alias("uniqueId"),
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("topic", "uniqueId", "data.*")

    messages_df = parsed_df \
        .withColumn("action", parse_action_udf(col("message"))) \
        .withColumn("transactionId", get_transaction_id_udf(col("message"))) \
        .withColumn("timestamp_str", parse_timestamp_udf(col("message"))) \
        .withColumn("power", parse_power_udf(col("message"))) \
        .withColumn("meterStart", parse_meter_start_udf(col("message"))) \
        .withColumn("meterStop", parse_meter_stop_udf(col("message"))) \
        .withColumn("idTag", parse_id_tag_udf(col("message"))) \
        .withColumn("connectorId", parse_connector_id_udf(col("message"))) \
        .withColumn("soc", parse_soc_udf(col("message"))) \
        .withColumn("voltage", parse_voltage_udf(col("message"))) \
        .withColumn("reason", parse_reason_udf(col("message")))

    valid_actions = ["MeterValues", "StopTransaction", "StartTransaction", "RemoteStopTransaction"]
    messages_df = messages_df.filter(
        (col("transactionId").isNotNull()) &
        (col("action").isin(valid_actions)) &
        (col("timestamp_str").isNotNull())
    )

    messages_df = messages_df.withColumn(
        "eventTime", to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    ).filter(col("eventTime").isNotNull())

    messages_df = messages_df.withColumn(
        "sessionId", concat_ws("_", col("chargerId"), col("transactionId"))
    )

    messages_df = messages_df.withColumn("isStop", is_stop_action_udf(col("action")))

    active_messages = messages_df.filter(col("isStop") == False)
    completed_messages = messages_df.filter(col("isStop") == True)

    # BRANCH 1: Normalized messages -> ocpp.active.raw or ocpp.active.raw
    def write_normalized(batch_df: DataFrame, batch_id: int) -> None:
        from confluent_kafka import Producer
        count = batch_df.count()
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        print(f"[write_normalized] Batch {batch_id}: Processing {count} active messages for ocpp.active.raw")
        for row in batch_df.collect():
            normalized = {
                "stationId": row["chargerId"],
                "timestamp": row["timestamp_str"],
                "action": row["action"],
                "value": {}
            }
            if row["action"] == "StartTransaction":
                normalized["value"] = {"transactionId": row["transactionId"], "meterStart": int(row["meterStart"]) if row["meterStart"] else None, "idTag": row["idTag"], "connectorId": int(row["connectorId"]) if row["connectorId"] else None}
            elif row["action"] == "MeterValues":
                normalized["value"] = {"power": float(row["power"]) if row["power"] else None, "soc": float(row["soc"]) if row["soc"] else None, "voltage": float(row["voltage"]) if row["voltage"] else None}
            producer.produce("ocpp.active.raw", key=row["sessionId"], value=json.dumps(normalized))
        producer.flush()
        print(f"[write_normalized] Batch {batch_id}: Wrote {count} messages to ocpp.active.raw")

    q1 = active_messages.writeStream.foreachBatch(write_normalized).outputMode("append").option("checkpointLocation", f"{CHECKPOINT_DIR}/active_raw").start()

    # BRANCH 2: Session state -> ocpp.active or ocpp.active
    def write_state(batch_df: DataFrame, batch_id: int) -> None:
        from confluent_kafka import Producer
        from datetime import datetime
        count = batch_df.count()
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        print(f"[write_state] Batch {batch_id}: Processing {count} active messages for ocpp.active")
        for row in batch_df.collect():
            sid = row["sessionId"]
            # Use eventTime for duration calculation since sessionId is now chargerId_transactionId
            start_ts = row["timestamp_str"]
            if row["action"] == "StartTransaction":
                # For StartTransaction, this is the start time
                pass
            try:
                start_dt = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(row["timestamp_str"].replace("Z", "+00:00"))
                duration = int((end_dt - start_dt).total_seconds())
                power = float(row["power"]) if row["power"] else 0.0
                energy = power * (duration / 3600) if power > 0 else 0.0
            except:
                duration, energy = 0, 0.0
            status = "pending" if row["action"] == "StartTransaction" else "active"
            state = {"sessionId": sid, "stationId": row["chargerId"], "transactionId": row["transactionId"], "status": status, "startTime": start_ts, "lastSeen": row["timestamp_str"], "duration": duration, "energyConsumedSoFar": round(energy, 4), "runningCount": 1}
            producer.produce("ocpp.active", key=sid, value=json.dumps(state))
        producer.flush()
        print(f"[write_state] Batch {batch_id}: Wrote {count} state updates to ocpp.active")

    q2 = active_messages.writeStream.foreachBatch(write_state).outputMode("append").option("checkpointLocation", f"{CHECKPOINT_DIR}/active").start()

    # BRANCH 3: Completed sessions -> PostgreSQL ocpp.history or ocpp.history_test
    messages_with_watermark = messages_df.withWatermark("eventTime", "1 hour")
    session_agg = messages_with_watermark.groupBy("chargerId", "transactionId", "sessionId").agg(
        min("eventTime").alias("startTime"),
        max(when(col("action") == "StopTransaction", col("eventTime"))).alias("endTime"),
        count("*").alias("eventCount"),
        max(when(col("action") == "StopTransaction", col("reason"))).alias("terminationReason"),
        sum("power").alias("powerSum"), count("power").alias("powerCount"),
        max(when(col("action") == "StartTransaction", col("meterStart"))).alias("meterStart"),
        max(when(col("action") == "StopTransaction", col("meterStop"))).alias("meterStop"),
        max(when(col("action") == "StartTransaction", col("idTag"))).alias("idTag"),
        max(when(col("action") == "StartTransaction", col("connectorId"))).alias("connectorId"),
        max("soc").alias("socEnd"), min("soc").alias("socStart"), avg("soc").alias("socAvg"),
        max("power").alias("maxPower"), avg("voltage").alias("voltageAvg")
    )
    result_df = session_agg.filter(col("endTime").isNotNull()) \
        .withColumn("stationId", col("chargerId")) \
        .withColumn("duration", (unix_timestamp(col("endTime")) - unix_timestamp(col("startTime"))).cast("int")) \
        .withColumn("totalEnergyConsumed", when((col("powerCount") > 0) & (col("duration").isNotNull()), spark_round(col("powerSum") / col("powerCount") * (col("duration") / 3600), 3)).otherwise(lit(None))) \
        .withColumn("avgPower", when(col("powerCount") > 0, col("powerSum") / col("powerCount")).otherwise(lit(None))) \
        .withColumn("lastSeen", col("endTime")) \
        .select(col("sessionId"), col("stationId"), col("transactionId"), lit("completed").alias("status"), col("startTime"), col("endTime"), col("duration"), col("lastSeen"), col("terminationReason"), col("totalEnergyConsumed"), col("avgPower"), col("maxPower"), col("idTag"), col("connectorId"), col("meterStart"), col("meterStop"), col("socStart"), col("socEnd"), col("voltageAvg"), col("eventCount"))

    def write_pg(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        print(f"[write_pg] Batch {batch_id}: Processing {count} completed sessions for PostgreSQL")
        try:
            batch_df.write.format("jdbc").option("url", POSTGRES_URL).option("dbtable", "ocpp.history").option("user", POSTGRES_USER).option("password", POSTGRES_PASSWORD).option("driver", "org.postgresql.Driver").mode("append").save()
            print(f"[write_pg] Batch {batch_id}: Wrote {count} completed sessions to ocpp.history")
        except Exception as e:
            if "duplicate key" in str(e):
                print(f"[write_pg] Batch {batch_id}: Duplicate session, skipping: {e}")
            else:
                raise

    q3 = result_df.writeStream.foreachBatch(write_pg).outputMode("update").option("checkpointLocation", f"{CHECKPOINT_DIR}/history").start()

    # BRANCH 4: Tombstones
    def send_tombstones(batch_df: DataFrame, batch_id: int) -> None:
        from confluent_kafka import Producer
        count = batch_df.select("sessionId").distinct().count()
        producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        print(f"[send_tombstones] Batch {batch_id}: Sending tombstones for {count} completed sessions")
        for row in batch_df.select("sessionId").distinct().collect():
            sid = row["sessionId"]
            producer.produce("ocpp.active", key=sid, value=None)
            producer.produce("ocpp.active.raw", key=sid, value=None)
        producer.flush()
        print(f"[send_tombstones] Batch {batch_id}: Sent tombstones for {count} sessions")

    q4 = completed_messages.writeStream.foreachBatch(send_tombstones).outputMode("append").option("checkpointLocation", f"{CHECKPOINT_DIR}/tombstones").start()

    q1.awaitTermination()
    q2.awaitTermination()
    q3.awaitTermination()
    q4.awaitTermination()


if __name__ == "__main__":
    run_pipeline()
