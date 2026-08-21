# Hivemind Data Engineering Challenge

Consider a company called "EV Coorp." in the fleet management sector that operates EV charging infrastructure and is encountering several challenges related to data processing and analytics. These challenges include difficulties in understanding the operational status of their infrastructure, which is compounded by manual analysis processes that are both time-consuming and prone to errors. Additionally, they are facing issues with growing data volumes that their current systems are unable to manage effectively. There is also a pressing need for both real-time monitoring and historical insights, which their current setup does not adequately provide. Same applies for the absence of ad hoc analysis capabilities. Furthermore, the data they collect is fragmented over different data sources and their current solution fails to deliver clear business value on time.

As a consulting company, our role is to assist them in modernizing their data capabilities. This modernization will enable better decision-making and enhance operational efficiency.

In your role, you will be joining our team to assist in the design and construction of a data processing solution. Your responsibilities include analyzing the available data to determine the insights that can be extracted. You are also tasked with designing a system architecture that is meeting their requirements and allows easy extraction of fields that can be used build the insights you identified in the data earlier. Finally, you need to build a working prototype to demonstrate the value we can deliver based on the data we got from them. This is where all pieces come together.

The sample data we got from their charging infrastructure includes operational messages following the [OCPP protocol](https://openchargealliance.org/protocols/open-charge-point-protocol/#OCPP1.6):
- Infrastructure status and health indicators
- Session lifecycle events
- Real-time operational metrics
- System diagnostics and notifications

Because of the size the data, it is split into two files:
- [`ocpp-data-many-chargers.txt`](ocpp-data-many-chargers.txt)
- [`ocpp-data-many-days.txt`](ocpp-data-many-days.txt)

This dataset represents typical patterns you'd encounter in EV charging infrastructure between charge points and the grid. You find in the [`ocpp-data-many-chargers.txt`](ocpp-data-many-chargers.txt) file, here is an extract of the first rows:
```
charger6 : [2,"ef51a638-0e05-4a9d-be52-594ada28f153","MeterValues",{"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:57.599Z","sampledValue":[{"value":"37.3","context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","location":"Outlet","unit":"A"},{"value":"30850.8733","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Register","location":"Outlet","unit":"kWh"},{"value":"0.0469","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"kWh"},{"value":"20.7","context":"Sample.Periodic","format":"Raw","measurand":"Power.Active.Import","location":"Outlet","unit":"kW"},{"value":"587.2","context":"Sample.Periodic","format":"Raw","measurand":"Voltage","location":"Outlet","unit":"V"},{"value":"67","context":"Sample.Periodic","format":"Raw","measurand":"SoC","location":"EV","unit":"Percent"},{"value":"200","context":"Sample.Periodic","format":"Raw","measurand":"Current.Offered","location":"Outlet","unit":"A"},{"value":"60000","context":"Sample.Periodic","format":"Raw","measurand":"Power.Offered","location":"Outlet","unit":"W"}]}]}]
charger6 : [3,"ef51a638-0e05-4a9d-be52-594ada28f153",{}]
charger10 : [2,"2e71389f-174a-44ef-a11d-6fa4db1b75a2","MeterValues",{"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:59.865Z","sampledValue":[{"value":"198.5","context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","location":"Outlet","unit":"A"},{"value":"13135.6161","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Register","location":"Outlet","unit":"kWh"},{"value":"0.3119","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"kWh"},{"value":"124.8","context":"Sample.Periodic","format":"Raw","measurand":"Power.Active.Import","location":"Outlet","unit":"kW"},{"value":"628.6","context":"Sample.Periodic","format":"Raw","measurand":"Voltage","location":"Outlet","unit":"V"},{"value":"39","context":"Sample.Periodic","format":"Raw","measurand":"SoC","location":"EV","unit":"Percent"},{"value":"200","context":"Sample.Periodic","format":"Raw","measurand":"Current.Offered","location":"Outlet","unit":"A"},{"value":"150000","context":"Sample.Periodic","format":"Raw","measurand":"Power.Offered","location":"Outlet","unit":"W"}]}]}]
charger10 : [3,"2e71389f-174a-44ef-a11d-6fa4db1b75a2",{}]
charger8 : [2,"b5460d35-4848-4ae8-a9a1-4a7424db277c","Heartbeat",{ }]
charger8 : [3,"b5460d35-4848-4ae8-a9a1-4a7424db277c",{"currentTime":"2025-08-27T00:00:03.233944Z"}]
charger3 : [2,"6ae647ad-6a4c-430f-afd4-f3281c396dcb","Heartbeat",{ }]
charger3 : [3,"6ae647ad-6a4c-430f-afd4-f3281c396dcb",{"currentTime":"2025-08-27T00:00:04.186104Z"}]
charger6 : [2,"5621ea49-3c69-4f20-a0f3-4c56c45fce34","StatusNotification",{"connectorId":2,"errorCode":"NoError","info":"","status":"Available","timestamp":"2025-08-27T00:00:03.015Z","vendorId":"charger brand Technology","vendorErrorCode":""}]
```

# Deliverables

The success of this project will be determined by several key criteria.

1. **Working Solution**: A data processing system that transforms raw operational data into actionable insights
2. **Architecture Documentation**: Clear explanation of your design choices and how the system addresses the client's needs
3. **Demonstration**: A way to actually showcase the solution's capabilities and validate that it works as intended

While the solution doesn't need to be fully production-ready, document any shortcuts, technical debt, or production concerns that need addressing before deployment. Even though it might not required to be production ready right now, we still expect a portable deployment path that supports both the local development and can be adapted for future production deployment, or deployments to different environments. Make sure to take this into acccount from the architecture design perspective.

The solution should effectively process the data rows found in the `ocpp-data-*.txt` files, offering accessible methods for querying and inspecting the processed data. It should enable data aggregation across various dimensions such as time and chargers and potentially multiple sites in the future.

Consider this potential output structure:
```json
{
  "sessionId": "charger6_2025-08-26T23:59:57.599Z",
  "stationId": "charger6", 
  "status": "active",
  "startTime": "2025-08-26T23:59:57.599Z",
  "endTime": null,
  "duration": 457,
  "totalEnergyConsumed": {"value": 12.5, "unit": "kWh"},
  "eventCount": 23
}
```
The structure above is just one example - you're free to use any output format that serves the analytical needs: JSON, CSV, Parquet, Delta Tables, Iceberg, database tables, dashboards, or any other format that demonstrates your solution effectively. The output fields are also just an example. Feel free to add more or skip any if you think it's not relevant, maybe termination reason?

# Notes
- Choose technologies and approaches that best fit the problem - we value thoughtful technical decisions and a good balance between simplicity and performance and pragmatism
- Consider how to determine operational status in incomplete or inconsistent scenarios
- To calculate the energy consumption you would have to calculate the integral of the power curve over time, but for simplicity you can safely approximate this by the average of the power curve and multiply it by the duration of the session to create a simple estimate. e.g. 5kW * 1h = 5kWh. Look for `Power.Active.Import`.

# Run

## Quick Start

The fastest way to get everything running:

```bash
# 1. Install dependencies
uv sync

# 2. Start infrastructure and initialize everything
make init

# 3. Run all tests
make test

# 4. Clean up when done
make cleanup-all  # Cleans data but keeps containers running
make clean        # Stops and removes containers
```

## Detailed Setup

### Python Environment

```bash
# Install dependencies
uv sync

# Verify installation
uv run python --version
uv run pytest --version
```

### Docker Infrastructure

The project uses Docker Compose to manage services:
- **PostgreSQL** (TimescaleDB) - Port 5432
- **Kafka** - Port 9092
- **Zookeeper** - Port 2181
- **Schema Registry** - Port 8081
- **Grafana** - Port 3000

```bash
# Start all services
make start

# Or manually
docker compose down
docker compose up -d postgres zookeeper kafka schema-registry

# Check service status
make ps

# View logs
make logs
```

### Grafana

Grafana is created as part of `docker compose up -d`
- **URL**: `http://localhost:3000`
- **Username**: `admin`
- **Password**: `admin`

Grafana is pre-configured with PostgreSQL and Kafka data sources.

### Initialize Infrastructure

#### Option 1: Using Make (Recommended)

```bash
# Full initialization (Docker + DB + Kafka + sample data)
make init

# Light initialization (no sample data)
make init-light

# Initialize and run tests
make init-test
```

#### Option 2: Manual Initialization

```bash
# Initialize PostgreSQL database (creates tables and indexes)
make db-init
# or
uv run python postgres/scripts/init_db.py

# Initialize Kafka topics and schemas
make kafka-init
# or
uv run python kafka/scripts/create_topics.py

# Generate sample data (optional)
uv run python kafka/scripts/ocpp_producer.py

# Start consumer to process messages to PostgreSQL (run in background)
uv run python postgres/scripts/kafka_to_postgres.py &

# Start Spark streaming pipeline (run in background)
uv run python spark/scripts/spark_kafka_to_postgres.py &
```

### Start Spark Pipeline

```bash
# Start the Spark streaming pipeline
make consume-data
# or
uv run python spark/scripts/spark_kafka_to_postgres.py

# The pipeline:
# - Reads from Kafka topic: ocpp.messages
# - Writes to Kafka topics: ocpp.active, ocpp.active.raw
# - Writes to PostgreSQL table: ocpp.history
```

## Testing

### Run Tests

All tests use isolated test infrastructure with `_test` suffixed resources:
- Kafka topics: `ocpp.messages_test`, `ocpp.active_test`, `ocpp.active.raw_test`
- PostgreSQL table: `ocpp_history_test`

```bash
# Run all tests
make test

# Run specific test categories
make test-unit     # Unit tests (no infrastructure required)
make test-kafka    # Kafka tests
make test-postgres # PostgreSQL tests
make test-spark    # Spark tests
make test-e2e      # End-to-end tests

# Run with verbose output
uv run pytest tests/ -v

# Run a specific test file
uv run pytest tests/e2e/test_full_flow.py -v

# Run with test coverage
uv run pytest tests/ --cov=postgres --cov=kafka --cov-report=term
```

### Test Infrastructure

The test infrastructure is automatically created when running pytest:

```bash
# Manually create test infrastructure (if needed)
uv run python tests/setup_test_infra.py

# Or let pytest create it automatically via conftest.py
uv run pytest tests/
```

## Data Cleanup

### Cleanup Data (Keep Containers Running)

When you need to reset data between test runs without stopping containers:

```bash
# Clean up main project environment (ocpp.messages, ocpp.active, ocpp.active.raw, charger_session)
make cleanup

# Clean up test environment (ocpp.messages_test, ocpp.active_test, ocpp.active.raw_test, ocpp_history_test)
make cleanup-test

# Clean up both environments
make cleanup-all
```

#### Using Scripts Directly

```bash
# Python script
python scripts/cleanup.py              # Clean main (default)
python scripts/cleanup.py --test       # Clean test
python scripts/cleanup.py --all        # Clean both

# Shell script
./scripts/cleanup.sh
./scripts/cleanup.sh --test
./scripts/cleanup.sh --all
```

What cleanup does:
- **PostgreSQL**: Truncates all data from tables (keeps table schemas)
- **Kafka**: Consumes and removes all messages from topics (keeps topics)

### Stop Infrastructure

```bash
# Stop and remove containers
make clean

# Or manually
docker compose down

# Stop containers and remove volumes (full cleanup)
docker compose down -v
```

## Usage Examples

### Example 1: Development Workflow

```bash
# Start fresh
make clean
make init-light

# Start Spark pipeline in background
uv run python spark/scripts/spark_kafka_to_postgres.py

# Produce test messages
uv run python kafka/scripts/ocpp_producer.py

# Query PostgreSQL to see processed data
PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp
-- Select from ocpp.history table
SELECT * FROM ocpp.history LIMIT 10;

# Clean up and start over
make cleanup
```

### Example 2: Running Tests

```bash
# Full test run
make init-test

# Or step by step
make start
sleep 30  # Wait for services
make db-init
make kafka-init
uv run pytest tests/ -v

# Clean up test data between runs
make cleanup-test
uv run pytest tests/ -v
```

### Example 3: Working with Test Environment

```bash
# Initialize test infrastructure
make start
uv run python tests/setup_test_infra.py

# Run a specific test
uv run pytest tests/e2e/test_full_flow.py::TestSessionLifecycle::test_active_session_in_kafka_not_postgres -v

# Clean up test data
make cleanup-test
```

## Environment Variables

You can customize connections using environment variables:

```bash
# PostgreSQL
export DATABASE_URL=postgresql://user:pass@host:port/db

# Kafka
export KAFKA_BROKER=host:port

# Schema Registry
export SCHEMA_REGISTRY_URL=http://host:port
```

## Troubleshooting

### Docker Services Not Starting

```bash
# Check Docker is running
docker --version

# Check container status
docker compose ps

# Check logs
docker compose logs

# Try cleaning up completely
docker compose down -v
docker compose up -d
```

### PostgreSQL Connection Issues

```bash
# Test connection
PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "SELECT 1"

# Wait for PostgreSQL to be ready (may take 30-60 seconds)
sleep 30
```

### Kafka Connection Issues

```bash
# Test Kafka connection
uv run python -c "from confluent_kafka.admin import AdminClient; print(AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5))"

# Check Zookeeper is running
docker compose ps | grep zookeeper
```

### Port Conflicts

Default ports used:
- PostgreSQL: 5432
- Zookeeper: 2181
- Kafka: 9092
- Schema Registry: 8081
- Grafana: 3000

If you have conflicts, stop existing services or modify `docker-compose.yml`.

### Python Dependencies

```bash
# Reinstall dependencies
uv sync --dev

# Check specific package
uv run python -c "import confluent_kafka; print(confluent_kafka.__version__)"
```

## Project Structure

```
.
├── docker-compose.yml          # Docker service definitions
├── pyproject.toml              # Python dependencies
├── Makefile                    # Common commands
├── scripts/
│   ├── init_all.py             # Full initialization script
│   ├── init_all.sh             # Shell wrapper for initialization
│   ├── cleanup.py              # Data cleanup script
│   └── cleanup.sh              # Shell wrapper for cleanup
├── kafka/
│   └── scripts/
│       ├── create_topics.py    # Create Kafka topics and schemas
│       └── ocpp_producer.py     # Produce sample OCPP messages
├── postgres/
│   └── scripts/
│       ├── init_db.py          # Create PostgreSQL tables
│       └── kafka_to_postgres.py # Consume from Kafka, write to PostgreSQL
├── spark/
│   └── scripts/
│       └── spark_kafka_to_postgres.py  # Spark streaming pipeline
├── tests/
│   ├── conftest.py             # Test fixtures
│   ├── setup_test_infra.py     # Manual test infrastructure setup
│   └── ...                     # Test files
└── grafana/                    # Grafana configuration
```

## Architecture Overview

### Data Flow

```
OCPP Messages (Kafka: ocpp.messages)
         ↓
┌─────────────────────────────────────┐
│  Spark Streaming Pipeline             │
│  (spark_kafka_to_postgres.py)         │
└─────────────────┬───────────────────┘
                  ↓
┌─────────────────┴───────────────────┐
│  Kafka Topics                          │
│  - ocpp.active (compacted, latest)     │
│  - ocpp.active.raw (compacted, 3day)  │
└─────────────────┬───────────────────┘
                  ↓
┌─────────────────┴───────────────────┐
│  PostgreSQL Tables                    │
│  - ocpp.history (completed sessions)  │
│  - charger_session (session state)    │
└──────────────────────────────────────┘
```

### Test Environment (Isolated)

```
Test Messages (Kafka: ocpp.messages_test)
         ↓
┌─────────────────────────────────────┐
│  Test PostgreSQL Table                 │
│  - ocpp_history_test                  │
└──────────────────────────────────────┘
```

Test infrastructure uses `_test` suffixed resources to ensure complete isolation from production data.