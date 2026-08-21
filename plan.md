# Plan: Realtime (Kafka) + History (PostgreSQL) Charging Session Architecture

## Objective
Separate charging session data into two optimized storage layers:
- **Realtime**: Active/in-progress sessions **ONLY** in a **compacted Kafka topic** (`ocpp.active`)
- **History**: Completed sessions in a **PostgreSQL table** (`ocpp.history`)

## Core Behavior
- **Kafka topic `ocpp.active` contains ONLY active sessions** - no completed sessions
- **Kafka topic `ocpp.active.raw` contains ONLY active session messages** - no completed session messages
- When StopTransaction or RemoteStopTransaction is detected:
  1. Session is written to PostgreSQL `ocpp.history` table
  2. A **tombstone** (null value) is sent to `ocpp.active` with key=`sessionId`
  3. A **tombstone** (null value) is sent to `ocpp.active.raw` with key=`sessionId`
  4. Kafka compaction automatically removes that key from both topics
- Result: Only active sessions remain in both Kafka topics

This avoids SQL write contention while leveraging existing Kafka infrastructure.

---

## Architecture Purpose

### ocpp.active
- **Purpose**: Stateful overview of active sessions (latest state per session)
- **Use case**: Quick realtime monitoring - "show me all active sessions"
- **Key**: `sessionId`

### ocpp.active.raw  
- **Purpose**: Debugging/detailed view - append-based log of all messages for active sessions
- **Use case**: Investigate unexpected behavior - "show me all messages for charger6 in the last hour"
- **Key**: `sessionId`
- **Relationship to ocpp.active**: ocpp.active.raw is the detailed log; ocpp.active is the stateful summary

### ocpp.history (PostgreSQL)
- **Purpose**: Permanent storage of completed sessions for analysis
- **Use case**: Historical analysis, reporting, long-term storage

### ocpp.messages (existing)
- **Purpose**: Raw message archive with longer retention
- **Use case**: Historical debugging beyond the 3-day window of ocpp.active.raw

---

## Current State Analysis

### Existing Architecture
- Single `charger_session` table (`postgres/schema/charger_session.py`)
- Status field: "active" (has MeterValues, no StopTransaction) or "ended" (has StopTransaction)
- Spark streaming pipeline (`spark/scripts/spark_kafka_to_postgres.py`) writes to single PostgreSQL table
- Sessionization: group by `chargerId + transactionId`
- Kafka topic: `ocpp.messages` (raw messages, created by `kafka/scripts/create_topics.py`)

### Session Lifecycle in OCPP Data
1. **StartTransaction**: Begins session (`transactionId`, `meterStart`, `timestamp`, `idTag`)
2. **MeterValues**: Periodic updates (`transactionId`, `connectorId`, power/energy readings)
3. **StopTransaction**: Ends session (`transactionId`, `meterStop`, `timestamp`, `reason`)
4. **RemoteStopTransaction**: Remote stop trigger (`transactionId`)

A session is **active** when it has MeterValues but NO StopTransaction/RemoteStopTransaction.
A session is **completed** when StopTransaction or RemoteStopTransaction arrives.

---

## Architecture Overview

```
OCPP Messages (raw)
    │
    ▼
ocpp.messages (Kafka, long retention e.g. 30 days)
    │
    ▼
Spark Streaming
    │
    +──────────────────────────+──────────────────────────+
    │                          │                          │          
    ▼                          ▼                          ▼          
ocpp.active.raw           ocpp.active                ocpp.history
(Kafka, compact)          (Kafka, compact)           (PostgreSQL)
ACTIVE ONLY                ACTIVE ONLY                 COMPLETED ONLY
key: sessionId            key: sessionId              
value: normalized msg     value: session state        
retention: 3 days         
```

### Data Flow for Completed Sessions:
```
StopTransaction detected
    │
    +─────────────────+─────────────────+
    │                 │                 │
    ▼                 ▼                 ▼
Write to       Tombstone to       Tombstone to
ocpp.history   ocpp.active        ocpp.active.raw
(PostgreSQL)    (removes key)      (removes key)
```

### Why Kafka for Realtime?

| Concern | SQL Table | Compacted Kafka Topic |
|---------|-----------|---------------------|
| Write Contention | Row locks, blocking | No locks, partitioned |
| Update Frequency | High overhead | Optimized for streams |
| Read Pattern | SQL flexible | Stream or latest lookup |
| Infrastructure | Have it | **Already have it** |
| Scalability | Vertical | Horizontal |

---

## Implementation Plan

### Phase 1: Create Kafka Topics

#### 1.1 Update `kafka/scripts/create_topics.py`

Modify the existing `create_kafka_topic()` function to create two new topics.

**Topic 1: `ocpp.active.raw`**
- **Purpose**: Normalized/readable version of `ocpp.messages` - all processed messages for ACTIVE SESSIONS ONLY
- `cleanup.policy=compact` - Enables tombstone-based removal of completed sessions
- `retention.ms=259200000` (3 days) - Short retention for debugging active issues
- `num_partitions=10` (match or exceed `ocpp.messages` partitions)
- `replication_factor=1` (dev) / `3` (production)
- **Key**: `sessionId` (string: `chargerName + startTime`) - All messages for a session share the same key
- **Value**: JSON with `{stationId, timestamp, action, value}`
- **Behavior**: When StopTransaction/RemoteStopTransaction detected, send tombstone with key=`sessionId` → Kafka compaction removes all messages for that session

**Topic 2: `ocpp.active`**
- **Purpose**: Store ONLY active/in-progress charging sessions with latest state
- `cleanup.policy=compact` - Only keep latest value per key; null values (tombstones) remove keys
- `num_partitions=10` (scale based on expected concurrent sessions)
- `replication_factor=1` (dev) / `3` (production)
- **Key**: `sessionId` (string: `chargerName + startTime`)
- **Value**: JSON with latest session state (power, energy, SoC, timestamps, etc.)
- **Null value**: Tombstone that removes the session from the topic

**Code to add:**
```python
# In kafka/scripts/create_topics.py, update create_kafka_topic():

def create_kafka_topic():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    
    # Define all topics to create
    topics = [
        # Existing raw OCPP messages topic
        ("ocpp.messages", 1, 1, {}),
        
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
                topic_configs=configs
            )
            admin_client.create_topics([new_topic])
            print(f"Created Kafka topic: {topic_name}")
```

#### 1.2 `ocpp.active.raw` Topic Schema

**Purpose**: Debugging/detailed view - append-based log of all messages for **active sessions only**

**Message Format:**
```json
{
  "stationId": "charger6",
  "timestamp": "2025-08-26T23:59:57.599Z",
  "action": "MeterValues",
  "value": {
    "power": 20.7,
    "energy": 30850.8733,
    "voltage": 587.2,
    "soc": 67,
    ...
  }
}
```

**Key**: `sessionId` (`chargerName + startTime` from first MeterValues or StartTransaction)

**Behavior**:
- All messages from `ocpp.messages` are parsed and written here in normalized format
- Only messages from **active sessions** are written
- When a StopTransaction/RemoteStopTransaction is detected:
  - Session is written to `ocpp.history` PostgreSQL table
  - **Tombstone (null value) is sent to `ocpp.active.raw` with key=`sessionId`**
  - Kafka compaction automatically removes all messages for that session from the topic
- Result: `ocpp.active.raw` contains ONLY messages from active sessions, keeping it clean for debugging

#### 1.3 `ocpp.active` Topic Schema

**Purpose**: Track ONLY active/ongoing charging sessions with latest state. Compacted topic - only latest value per key is retained.

**Key**: `sessionId` (string: `chargerName + startTime`)

**Message Format (Value):**
```json
{
  "sessionId": "charger6_2025-08-26T23:59:57.599Z",
  "stationId": "charger6",
  "transactionId": "txn123",
  "status": "active" | "pending",
  "startTime": "2025-08-26T23:59:57.599Z",
  "lastSeen": "2025-08-27T00:05:00.000Z",
  "duration": 302,
  "energyConsumedSoFar": 1.5,
  "runningCount": 42
}
```

**Field Definitions:**

| Field | Type | Behavior | Description |
|-------|------|----------|-------------|
| `sessionId` | string | Set once | `chargerName + startTime`. Unique identifier for the session |
| `stationId` | string | Set once | Charger name (e.g., "charger6") |
| `transactionId` | string | Set once | OCPP transaction ID |
| `status` | string | Set once, then tombstoned | `"active"` if at least one MeterValues exists. `"pending"` if no MeterValues yet. Tombstoned when StopTransaction received |
| `startTime` | timestamp | Set once | Earliest timestamp of all MeterValues, or from StartTransaction |
| `lastSeen` | timestamp | **Stateful - updated** | Latest timestamp of all MeterValues. Updated with each new MeterValues |
| `duration` | integer | **Stateful - updated** | `lastSeen - startTime` in seconds. Recalculated with each update |
| `energyConsumedSoFar` | float | **Stateful - updated** | Average `Power.Active.Import` * duration (in kWh). Recalculated with each update |
| `runningCount` | integer | **Stateful - updated** | Current count of MeterValue events for this session. Incremented with each update |

**Session Lifecycle in `ocpp.active`:**
1. **StartTransaction arrives** → New row created with `status: "pending"`, `startTime` from message, other stateful fields initialized
2. **First MeterValues arrives** → `status` changes to `"active"`, stateful fields begin updating
3. **Subsequent MeterValues** → `lastSeen`, `duration`, `energyConsumedSoFar`, `runningCount` are updated
4. **StopTransaction arrives** → Session written to `ocpp.history`, tombstone sent with key=`sessionId` → row removed from topic

---

### Phase 2: Create History Table Schema

#### 2.1 New File: `postgres/schema/ocpp_history.py`

Only one new SQL table needed (realtime goes to Kafka, not SQL).

**Table name**: `ocpp.history` (matches `ocpp.messages`, `ocpp.active.raw`, and `ocpp.active` naming)

**Key fields**: sessionId, stationId, transactionId, startTime, endTime, duration, terminationReason, totalEnergyConsumed, avgPower, maxPower, idTag, connectorId, meterStart, meterStop, socStart, socEnd, voltageAvg

#### 2.2 Update `postgres/scripts/init_db.py`

Create `ocpp.history` table with performance indexes on: stationId, transactionId, startTime, endTime, terminationReason

---

### Phase 3: Update Spark Streaming Pipeline

#### 3.1 Modify `spark/scripts/spark_kafka_to_postgres.py`

**Add parsing functions:**
- `parse_meter_start` - Extract meterStart from StartTransaction
- `parse_meter_stop` - Extract meterStop from StopTransaction
- `parse_id_tag` - Extract idTag from StartTransaction
- `parse_connector_id` - Extract connectorId
- `parse_energy_register` - Extract Energy.Active.Import.Register
- `parse_soc` - Extract SoC
- `parse_voltage` - Extract Voltage
- `parse_normalized_message` - Extract sessionId, stationId, timestamp, action, value for `ocpp.active.raw`

**Split processing into 4 branches:**

1. **Normalized messages** → Write to `ocpp.active.raw` Kafka topic
   - **Key**: `sessionId` (from chargerId + startTime)
   - **Value**: `{stationId, timestamp, action, value}`
   - All messages from active sessions are written here

2. **Active sessions** → Write latest state to `ocpp.active` Kafka topic (compacted)
   - **Key**: `sessionId`
   - **Value**: Full session state with all fields (sessionId, stationId, transactionId, status, startTime, lastSeen, duration, energyConsumedSoFar, runningCount)
   - Each MeterValues for an active session updates this topic with latest state

3. **Completed sessions** → Write to PostgreSQL `ocpp.history` table (append-only)
   - Full session record with all final values including meterStart, meterStop, idTag, etc.

4. **Tombstones** → Send null value to BOTH Kafka topics
   - Send null value to `ocpp.active` with key=`sessionId`
   - Send null value to `ocpp.active.raw` with key=`sessionId`
   - Kafka compaction automatically removes the key from both topics
   - Ensures only active sessions remain in both Kafka topics

**Detailed Behavior:**

For each message from `ocpp.messages`:
- Parse and determine if it's part of an active or completed session
- For active sessions:
  - Write normalized message to `ocpp.active.raw` (key=sessionId)
  - Update state in `ocpp.active` (key=sessionId)
- When StopTransaction/RemoteStopTransaction detected:
  1. Write complete session to `ocpp.history` PostgreSQL
  2. Send tombstone to `ocpp.active` (key=sessionId)
  3. Send tombstone to `ocpp.active.raw` (key=sessionId)
  4. Kafka compaction removes the session from both topics

---

### Phase 4: Realtime Data Consumption

#### 4.1 Query Kafka Directly (Recommended)

**Use existing Kafka infrastructure** (Grafana Kafka plugin or other consumers) to query:
- `ocpp.active` → Get latest state of all active sessions
- `ocpp.active.raw` → Get detailed message history for specific active sessions
- `ocpp.messages` → Get raw historical data beyond 3-day window

**Rationale for direct Kafka querying vs FastAPI:**
- No new service to maintain
- Leverages existing infrastructure
- Real-time data without additional latency
- Kafka plugin for Grafana already available

---

## File Changes Summary

### New Files to Create:
1. `postgres/schema/ocpp_history.py` - History table schema (`ocpp.history`)

### Files to Modify:
1. `kafka/scripts/create_topics.py` - Add `ocpp.active.raw` (compact, 3-day retention) and `ocpp.active` (compact) topics
2. `postgres/scripts/init_db.py` - Create `ocpp.history` table + indexes
3. `spark/scripts/spark_kafka_to_postgres.py` - Split into 4 branches with tombstone emission to BOTH Kafka topics

### Files to Deprecate:
- `postgres/schema/charger_session.py` - No longer needed (replaced by `ocpp.history`)

---

## Data Retention Strategy

| Storage | Retention | Purpose |
|---------|-----------|---------|
| `ocpp.messages` | Long (e.g., 30 days) | Raw message archive for historical debugging |
| `ocpp.active.raw` | 3 days | Detailed active session messages for recent debugging |
| `ocpp.active` | N/A (compact) | Latest state of active sessions only |
| `ocpp.history` (PostgreSQL) | Permanent | Completed sessions for analysis |

---

## Example Queries

### Realtime (via Kafka direct queries)

**Active sessions overview:**
```bash
# Query ocpp.active topic for all active sessions
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ocpp.active --from-beginning
```

**Detailed debugging for a specific station:**
```bash
# Query ocpp.active.raw for messages from a specific session
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic ocpp.active.raw --from-beginning \
  --property print.key=true --property key.separator=":" \
  | grep "charger6_2025-08-26"
```

### History (PostgreSQL `ocpp.history`)
```sql
-- Energy by station (last 30 days)
SELECT stationId, COUNT(*), SUM(totalEnergyConsumed)
FROM ocpp.history 
WHERE startTime >= NOW() - INTERVAL '30 days'
GROUP BY stationId;

-- Termination reasons
SELECT terminationReason, COUNT(*), AVG(totalEnergyConsumed)
FROM ocpp.history 
GROUP BY terminationReason;

-- Detailed session analysis
SELECT * FROM ocpp.history 
WHERE stationId = 'charger6' 
AND startTime >= NOW() - INTERVAL '7 days';
```

---

## Dependencies
- No new dependencies
- Uses existing: confluent-kafka, PySpark, SQLModel, PostgreSQL

---

## Implementation Notes

### Why ocpp.active.raw uses compact policy with tombstones:
1. **Clean debugging view**: Only active session messages are visible, no clutter from completed sessions
2. **Efficient removal**: Tombstone + compaction automatically removes entire session when completed
3. **Session-level grouping**: Using `sessionId` as key groups all messages from same session together
4. **Short retention**: 3 days is sufficient for debugging active issues; historical data is in `ocpp.messages`

### Why both Kafka topics use sessionId as key:
1. **Consistency**: Same key strategy across both topics
2. **Tombstone efficiency**: Single tombstone removes session from both topics
3. **Query alignment**: Consumers can correlate between stateful (ocpp.active) and detailed (ocpp.active.raw) views

### Duplicate Data Rationale:
- **PostgreSQL `ocpp.history`**: Source of truth for completed sessions, permanent storage, SQL queryable
- **Kafka `ocpp.messages`**: Raw archive with longer retention for historical debugging
- **Overlap is intentional**: Different access patterns (SQL vs stream) justify duplication
- **Trade-off**: Storage cost vs query flexibility

---

## Testing Strategy (TDD Workflow)

**Principles:** Write tests FIRST, see them fail, then implement logic to make them pass (Red-Green-Refactor cycle).

### Workflow Per Section:
```
For each component:
1. Write unit tests → RUN → FAIL (Red)
2. Write minimal implementation → RUN → PASS (Green)
3. Refactor if needed → RUN → PASS (Green)
4. Move to next component
```

---

### Phase 0: Test Infrastructure (Do This First)

#### 0.1 Docker Test Environment
**Files to create:**
- `docker-compose-test.yml` - Kafka + PostgreSQL for testing

**Test first:**
```bash
# test_docker_setup.py
def test_kafka_running():
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    topics = admin.list_topics(timeout=5).topics
    assert "ocpp.messages" in topics  # Should fail initially

def test_postgres_running():
    conn = psycopg2.connect("postgresql://ev_user:ev_password@localhost:5432/ev_coorp_test")
    assert conn is not None  # Should fail initially
```

**Then implement:**
- Create `docker-compose-test.yml` with test versions of Kafka, PostgreSQL
- Run `docker-compose -f docker-compose-test.yml up -d`

---

### Phase 1: Kafka Topics (TDD)

#### 1.1 Test Topic Creation
**File:** `tests/kafka/test_topics.py`

```python
# RED: Write tests first
def test_ocpp_active_topic_exists():
    """ocpp.active topic should exist with compact policy"""
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic_config = admin.describe_topics(["ocpp.active"]).topics["ocpp.active"]
    assert topic_config.num_partitions == 10
    assert topic_config.topic_config["cleanup.policy"] == "compact"

def test_ocpp_active_raw_topic_exists():
    """ocpp.active.raw topic should exist with compact policy and 3-day retention"""
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    topic_config = admin.describe_topics(["ocpp.active.raw"]).topics["ocpp.active.raw"]
    assert topic_config.num_partitions == 10
    assert topic_config.topic_config["cleanup.policy"] == "compact"
    assert topic_config.topic_config["retention.ms"] == "259200000"

# GREEN: Then implement create_topics.py with correct configs
```

#### 1.2 Test Tombstone Behavior
**File:** `tests/kafka/test_tombstones.py`

```python
# RED: Write tests first
def test_tombstone_removes_from_ocpp_active():
    """Sending tombstone should remove session from ocpp.active"""
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "test", "auto.offset.reset": "earliest"})
    
    session_id = "charger1_2025-01-01T10:00:00Z"
    
    # Produce a message
    producer.produce("ocpp.active", key=session_id, value={"stationId": "charger1"})
    producer.flush()
    
    # Verify message exists
    consumer.subscribe(["ocpp.active"])
    msg = consumer.poll(timeout=5)
    assert msg is not None
    assert msg.key() == session_id
    
    # Produce tombstone
    producer.produce("ocpp.active", key=session_id, value=None)
    producer.flush()
    
    # GREEN: After implementation, this should pass
    # Wait for compaction (may need to configure min.compaction.lag.ms low for tests)
    # Then verify message is gone
    consumer.seek(TopicPartition("ocpp.active", 0, 0))
    msg = consumer.poll(timeout=5)
    assert msg is None  # Should fail until tombstone logic implemented

def test_tombstone_removes_from_ocpp_active_raw():
    """Sending tombstone should remove ALL messages for session from ocpp.active.raw"""
    # Similar test for ocpp.active.raw
    # Should fail until implementation complete
```

**Then implement:**
- Update `kafka/scripts/create_topics.py` with correct topic configurations
- Ensure tombstones work with compact policy

---

### Phase 2: PostgreSQL Schema (TDD)

#### 2.1 Test Table Schema
**File:** `tests/postgres/test_schema.py`

```python
# RED: Write tests first
def test_ocpp_history_table_exists():
    """ocpp.history table should exist with all required fields"""
    engine = create_engine("postgresql://ev_user:ev_password@localhost:5432/ev_coorp_test")
    inspector = inspect(engine)
    assert "ocpp.history" in inspector.get_table_names()
    
    columns = inspector.get_columns("ocpp.history")
    required_fields = ["sessionId", "stationId", "transactionId", "startTime", "endTime", 
                      "duration", "terminationReason", "totalEnergyConsumed", 
                      "meterStart", "meterStop", "idTag"]
    actual_fields = [col["name"] for col in columns]
    for field in required_fields:
        assert field in actual_fields  # Should fail until schema implemented

def test_ocpp_history_indexes():
    """ocpp.history should have indexes on key fields"""
    engine = create_engine("postgresql://ev_user:ev_password@localhost:5432/ev_coorp_test")
    inspector = inspect(engine)
    indexes = inspector.get_indexes("ocpp.history")
    indexed_columns = set()
    for idx in indexes:
        indexed_columns.update(idx["column_names"])
    
    assert "stationId" in indexed_columns
    assert "transactionId" in indexed_columns
    assert "startTime" in indexed_columns
    # Should fail until indexes implemented
```

**Then implement:**
- Create `postgres/schema/ocpp_history.py` with full schema
- Update `postgres/scripts/init_db.py` to create table and indexes

---

### Phase 3: Spark Pipeline (TDD) - Core Implementation

#### 3.1 Test Parsing Functions
**File:** `tests/spark/test_parsers.py`

```python
# RED: Write tests first (these will fail until parsers implemented)

def test_parse_meter_start():
    msg = '[2, "321", "StartTransaction", {"transactionId": "txn123", "meterStart": 1000, "timestamp": "2025-01-01T10:00:00Z"}]'
    assert parse_meter_start(msg) == 1000

def test_parse_meter_stop():
    msg = '[2, "322", "StopTransaction", {"transactionId": "txn123", "meterStop": 1500, "timestamp": "2025-01-01T10:05:00Z"}]'
    assert parse_meter_stop(msg) == 1500

def test_parse_id_tag():
    msg = '[2, "321", "StartTransaction", {"idTag": "RFID123", "timestamp": "2025-01-01T10:00:00Z"}]'
    assert parse_id_tag(msg) == "RFID123"

def test_parse_connector_id():
    msg = '[2, "321", "StartTransaction", {"connectorId": 1, "timestamp": "2025-01-01T10:00:00Z"}]'
    assert parse_connector_id(msg) == 1

def test_parse_soc():
    msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Battery.SOC", "value": 75}]}]}]'
    assert parse_soc(msg) == 75

def test_parse_voltage():
    msg = '[2, "321", "MeterValues", {"meterValue": [{"sampledValue": [{"measurand": "Voltage", "value": 230}]}]}]'
    assert parse_voltage(msg) == 230

def test_parse_normalized_message():
    msg = '[2, "321", "MeterValues", {"power": 22.5}, "2025-01-01T10:01:00Z"]'
    result = parse_normalized_message(msg)
    assert result["stationId"] == "charger1"  # From chargerId field
    assert result["timestamp"] == "2025-01-01T10:01:00Z"
    assert result["action"] == "MeterValues"
    assert result["value"]["power"] == 22.5
```

**Then implement:**
- Add all 8 parsing functions to `spark/scripts/spark_kafka_to_postgres.py`

---

#### 3.2 Test Pipeline Branches
**File:** `tests/spark/test_pipeline.py`

```python
# RED: Write tests first

def test_active_session_in_kafka_not_postgres():
    """Active sessions (no StopTransaction) should be in Kafka only, not PostgreSQL"""
    # Setup: Send StartTransaction + MeterValues (no StopTransaction)
    # Assert: Session exists in ocpp.active and ocpp.active.raw
    # Assert: Session does NOT exist in ocpp.history
    pass  # Implement test logic

def test_completed_session_in_postgres_not_kafka():
    """Completed sessions should be in PostgreSQL and removed from Kafka"""
    # Setup: Send StartTransaction + MeterValues + StopTransaction
    # Assert: Session exists in ocpp.history
    # Assert: Session does NOT exist in ocpp.active
    # Assert: Session does NOT exist in ocpp.active.raw
    pass  # Implement test logic

def test_session_state_updates():
    """Active session state should update with each MeterValues"""
    # Setup: Send StartTransaction + multiple MeterValues
    # Assert: ocpp.active has latest lastSeen, duration, energyConsumedSoFar, runningCount
    pass  # Implement test logic

def test_tombstone_sent_on_completion():
    """StopTransaction should trigger tombstones to both Kafka topics"""
    # Setup: Send StopTransaction
    # Assert: Tombstone (null value) sent to ocpp.active with key=sessionId
    # Assert: Tombstone (null value) sent to ocpp.active.raw with key=sessionId
    pass  # Implement test logic
```

**Then implement:**
- Split pipeline into 4 branches in `spark/scripts/spark_kafka_to_postgres.py`

---

#### 3.3 Test Data Integrity
**File:** `tests/spark/test_data_integrity.py`

```python
# RED: Write tests first

def test_all_fields_populated():
    """Completed sessions in PostgreSQL should have all required fields"""
    session = get_latest_session_from_postgres()
    assert session.sessionId is not None
    assert session.transactionId is not None
    assert session.stationId is not None
    assert session.startTime is not None
    assert session.endTime is not None
    assert session.meterStart is not None
    assert session.meterStop is not None
    assert session.duration is not None
    assert session.totalEnergyConsumed is not None
    # Should fail until all fields are written

def test_energy_calculation():
    """totalEnergyConsumed should be calculated correctly"""
    session = get_session_from_postgres("txn123")
    # Verify calculation: avgPower * duration / 3600
    expected = (session.powerSum / session.powerCount) * (session.duration / 3600)
    assert abs(session.totalEnergyConsumed - expected) < 0.001
```

**Then implement:**
- Verify all fields are correctly populated in PostgreSQL write logic
- Verify calculations are correct

---

### Phase 4: End-to-End Tests

**File:** `tests/e2e/test_full_flow.py`

```python
# RED: Write tests first

def test_complete_session_lifecycle():
    """Full session from StartTransaction to StopTransaction should flow correctly"""
    session_id = "charger1_2025-01-01T10:00:00Z"
    
    # 1. StartTransaction arrives
    send_kafka_message("ocpp.messages", session_id, StartTransaction(session_id, meterStart=1000))
    
    # Assert: Session appears in ocpp.active (pending) and ocpp.active.raw
    assert session_in_kafka("ocpp.active", session_id)
    assert session_in_kafka("ocpp.active.raw", session_id)
    assert not session_in_postgres(session_id)
    
    # 2. MeterValues arrive
    send_kafka_message("ocpp.messages", session_id, MeterValues(session_id, power=22.5))
    
    # Assert: Session state updates in ocpp.active (active)
    session = get_from_kafka("ocpp.active", session_id)
    assert session["status"] == "active"
    assert session["lastSeen"] is not None
    
    # 3. StopTransaction arrives
    send_kafka_message("ocpp.messages", session_id, StopTransaction(session_id, meterStop=1500))
    
    # Assert: Session removed from Kafka, written to PostgreSQL
    assert not session_in_kafka("ocpp.active", session_id)
    assert not session_in_kafka("ocpp.active.raw", session_id)
    assert session_in_postgres(session_id)
    
    # Assert: PostgreSQL has complete data
    pg_session = get_from_postgres(session_id)
    assert pg_session.meterStart == 1000
    assert pg_session.meterStop == 1500
    assert pg_session.totalEnergyConsumed is not None
```

---

### Test Fixtures

**File:** `tests/fixtures/ocpp_messages.py`

```python
# Sample OCPP message generators for testing

def start_transaction(chargerId, transactionId, meterStart=0, idTag=None):
    return f'[2, "{chargerId}", "StartTransaction", {{"transactionId": "{transactionId}", "meterStart": {meterStart}, "idTag": "{idTag}"}}]'

def meter_values(chargerId, transactionId, power, energy, soc=None, voltage=None):
    soc_part = f', "Battery.SOC": {soc}' if soc else ''
    voltage_part = f', "Voltage": {voltage}' if voltage else ''
    return f'[2, "{chargerId}", "MeterValues", {{"transactionId": "{transactionId}", "power": {power}, "energy": {energy}{soc_part}{voltage_part}}}]'

def stop_transaction(chargerId, transactionId, meterStop, reason="EVDriverDisconnected"):
    return f'[2, "{chargerId}", "StopTransaction", {{"transactionId": "{transactionId}", "meterStop": {meterStop}, "reason": "{reason}"}}]'

def remote_stop_transaction(chargerId, transactionId):
    return f'[2, "{chargerId}", "RemoteStopTransaction", {{"transactionId": "{transactionId}"}}]'
```

---

### Test Execution

**Run tests:**
```bash
# Start test infrastructure
docker-compose -f docker-compose-test.yml up -d

# Create test topics and tables
python kafka/scripts/create_topics.py
python postgres/scripts/init_db.py

# Run tests in TDD order
pytest tests/kafka/test_topics.py -v           # Phase 1: Topics
pytest tests/postgres/test_schema.py -v     # Phase 2: Schema  
pytest tests/spark/test_parsers.py -v        # Phase 3: Parsers
pytest tests/spark/test_pipeline.py -v       # Phase 3: Pipeline
pytest tests/spark/test_data_integrity.py -v # Phase 3: Validation
pytest tests/e2e/test_full_flow.py -v        # Phase 4: E2E

# Cleanup
docker-compose -f docker-compose-test.yml down
```

**TDD Cycle for Each Phase:**
1. Write tests for the phase
2. Run tests → **RED** (fail)
3. Implement the phase
4. Run tests → **GREEN** (pass)
5. Refactor if needed
6. Commit working code

---

## Estimated Effort
- Topic creation: 15 min
- Schema: 30 min
- Spark pipeline: 3-4 hours
- Testing: 2-3 hours (TDD adds time but improves quality)
- **Total: ~6-9 hours**

---

## Next Steps
1. Start with Phase 0 (test infrastructure)
2. Follow TDD workflow for each phase
3. Commit after each phase passes tests
4. Proceed to implementation of the full architecture