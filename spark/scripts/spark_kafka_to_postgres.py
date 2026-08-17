# spark/scripts/spark_kafka_to_postgres.py
"""
Spark Streaming pipeline: Kafka -> PostgreSQL

Reads OCPP messages from Kafka and writes charging session data to PostgreSQL.
Uses standard Spark structured streaming aggregations - no experimental APIs.

Architecture:
- Parse OCPP messages from Kafka
- Use event-time processing with stateful groupBy + agg (standard Spark streaming)
- Sessionization: group by chargerId + transactionId
- Write completed sessions to PostgreSQL charger_session table
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Optional, Dict, Any
import ast

# --- Schema Definitions ---
message_schema = "chargerId STRING, uniqueId STRING, message STRING"


def parse_ocpp_message(raw: str) -> Optional[Dict[str, Any]]:
    """Parse a raw OCPP message string."""
    try:
        msg = ast.literal_eval(raw)
        if len(msg) < 3:
            return None
        return {
            "action": msg[2],
            "payload": msg[3] if len(msg) > 3 else {},
            "uniqueId": msg[1]
        }
    except:
        return None


def extract_power_value(payload: Dict[str, Any]) -> Optional[float]:
    """Extract Power.Active.Import value from MeterValues payload."""
    for mv in payload.get("meterValue", []):
        for sv in mv.get("sampledValue", []):
            if sv.get("measurand") == "Power.Active.Import":
                try:
                    return float(sv["value"])
                except:
                    pass
    return None


def get_transaction_id(message: str) -> Optional[str]:
    """Extract transactionId from message."""
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            return msg[3].get("transactionId")
        return None
    except:
        return None


def parse_timestamp(message: str) -> Optional[str]:
    """Extract timestamp from message.
    For MeterValues: timestamp is in meterValue[0].timestamp
    For other messages: timestamp is in payload.timestamp
    """
    try:
        msg = ast.literal_eval(message)
        if len(msg) < 3:
            return None
        action = msg[2] if len(msg) > 2 else None
        payload = msg[3] if len(msg) > 3 and isinstance(msg[3], dict) else {}
        
        # For MeterValues, timestamp is nested in meterValue
        if action == "MeterValues":
            mv_list = payload.get("meterValue", [])
            if mv_list and len(mv_list) > 0:
                ts = mv_list[0].get("timestamp")
                if ts:
                    return ts.replace("Z", "+00:00")
        else:
            # For other message types, timestamp is at top level of payload
            ts = payload.get("timestamp")
            if ts:
                return ts.replace("Z", "+00:00")
        return None
    except:
        return None


def parse_action(message: str) -> Optional[str]:
    """Extract action from message."""
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 2:
            return msg[2]
        return None
    except:
        return None


def parse_power(message: str) -> Optional[float]:
    """Extract power value from message."""
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            return extract_power_value(msg[3])
        return None
    except:
        return None


def parse_status(message: str) -> Optional[str]:
    """Extract status from message."""
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            return msg[3].get("status")
        return None
    except:
        return None


def parse_reason(message: str) -> Optional[str]:
    """Extract reason from StopTransaction message."""
    try:
        msg = ast.literal_eval(message)
        if len(msg) > 3 and isinstance(msg[3], dict):
            return msg[3].get("reason")
        return None
    except:
        return None


def run_pipeline():
    """Run the main Spark streaming pipeline."""
    spark = SparkSession.builder \
        .appName("OCPPtoPostgres") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.postgresql:postgresql:42.7.3") \
        .getOrCreate()

    # Register UDFs
    get_transaction_id_udf = udf(get_transaction_id, StringType())
    parse_timestamp_udf = udf(parse_timestamp, StringType())
    parse_action_udf = udf(parse_action, StringType())
    parse_power_udf = udf(parse_power, FloatType())
    parse_status_udf = udf(parse_status, StringType())
    parse_reason_udf = udf(parse_reason, StringType())

    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ocpp.messages") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON messages from Kafka
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("data.*")

    # Extract fields from messages using UDFs
    messages_df = parsed_df \
        .withColumn("action", parse_action_udf(col("message"))) \
        .withColumn("transactionId", get_transaction_id_udf(col("message"))) \
        .withColumn("timestamp_str", parse_timestamp_udf(col("message"))) \
        .withColumn("power", parse_power_udf(col("message"))) \
        .withColumn("status", parse_status_udf(col("message"))) \
        .withColumn("reason", parse_reason_udf(col("message")))

    # Filter for valid messages with transactionId
    messages_df = messages_df.filter(
        (col("transactionId").isNotNull()) &
        (col("action").isin(["MeterValues", "StopTransaction", "StartTransaction"]))
    )

    # Convert timestamp string to TimestampType
    messages_df = messages_df.withColumn(
        "eventTime", to_timestamp(col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    )

    # Filter out messages with NULL eventTime
    messages_df = messages_df.filter(col("eventTime").isNotNull())

    # Use event-time with watermark for late data
    messages_with_watermark = messages_df.withWatermark("eventTime", "1 hour")

    # Group by transaction and aggregate
    # For startTime: use min of eventTime (smallest timestamp)
    # For endTime: use max of eventTime for StopTransaction only
    # For status: check if StopTransaction exists
    session_agg_df = messages_with_watermark.groupBy(
        "chargerId", "transactionId"
    ).agg(
        min("eventTime").alias("startTime"),
        max(when(col("action") == "StopTransaction", col("eventTime"))).alias("endTime"),
        count("*").alias("eventCount"),
        max(when(col("action") == "StopTransaction", col("status"))).alias("status"),
        max(when(col("action") == "StopTransaction", col("reason"))).alias("terminationReason"),
        sum("power").alias("powerSum"),
        count("power").alias("powerCount"),
        max(when(col("action") == "MeterValues", lit(1)).otherwise(lit(0))).alias("hasMeterValues"),
        max(when(col("action") == "StopTransaction", lit(1)).otherwise(lit(0))).alias("hasStopTransaction")
    )

    # Calculate derived fields
    result_df = session_agg_df \
        .withColumn("status", 
            when((col("hasMeterValues") > 0) & (col("hasStopTransaction") > 0), lit("ended"))
             .when(col("hasMeterValues") > 0, lit("active"))
             .otherwise(lit(None))
        ) \
        .withColumn("sessionId", concat_ws("_", col("chargerId"), date_format(col("startTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))) \
        .withColumn("stationId", col("chargerId")) \
        .withColumn("duration", 
            when(col("endTime").isNotNull(), 
                (unix_timestamp(col("endTime")) - unix_timestamp(col("startTime"))).cast("int")
            ).otherwise(lit(None))
        ) \
        .withColumn("totalEnergyConsumed", 
            when((col("powerCount") > 0) & (col("duration").isNotNull()), 
                round(col("powerSum") / col("powerCount") * (col("duration") / 3600), 3)
            ).otherwise(lit(None))
        ) \
        .select(
            "sessionId", "stationId", "status", 
            col("startTime"),
            col("endTime"),
            "duration", "totalEnergyConsumed", "eventCount", "terminationReason"
        )

    # Write to PostgreSQL
    def write_to_postgres(batch_df: DataFrame, batch_id: Optional[int]) -> None:
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/ev_coorp") \
                .option("dbtable", "charger_session") \
                .option("user", "ev_user") \
                .option("password", "ev_password") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
        except Exception as e:
            error_msg = str(e)
            if "duplicate key value violates unique constraint" in error_msg:
                # Extract the sessionId from the error message
                import re
                match = re.search(r'Key \("sessionId"\)=\(([^)]+)\)', error_msg)
                if match:
                    session_id = match.group(1)
                    print(f"Row with sessionId={session_id} already exists - skipping insert")
                else:
                    print(f"Duplicate key violation (sessionId not parsed from error): {error_msg}")
            else:
                # Re-raise other exceptions
                raise

    query = result_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", "./spark-checkpoints") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run_pipeline()
