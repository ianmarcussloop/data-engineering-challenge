#!/bin/bash
# Test runner script for TDD workflow
# Run tests in phases: they should FAIL first (RED), then PASS after infrastructure is created (GREEN)

set -e

KAFKA_BROKER="localhost:9092"
POSTGRES_URL="postgresql://ev_user:ev_password@localhost:5432/ev_coorp"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_success() {
    echo -e "${GREEN}✓${NC} $1"
}

echo_failure() {
    echo -e "${RED}✗${NC} $1"
}

echo_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

echo_phase() {
    echo ""
    echo -e "${YELLOW}========================================${NC}"
    echo -e "${YELLOW}  $1${NC}"
    echo -e "${YELLOW}========================================${NC}"
    echo ""
}

# Check if a Kafka topic exists
kafka_topic_exists() {
    local topic="$1"
    python3 -c "
from confluent_kafka.admin import AdminClient
import sys
admin = AdminClient({'bootstrap.servers': '$KAFKA_BROKER'})
topics = admin.list_topics(timeout=5).topics
sys.exit(0 if '$topic' in topics else 1)
" 2>/dev/null
    return $?
}

# Check if a PostgreSQL table exists
pg_table_exists() {
    local table="$1"
    python3 -c "
import psycopg2
import sys
try:
    conn = psycopg2.connect('$POSTGRES_URL', connect_timeout=5)
    cursor = conn.cursor()
    cursor.execute(\"SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '$table'\")
    result = cursor.fetchone()
    conn.close()
    sys.exit(0 if result else 1)
except:
    sys.exit(1)
" 2>/dev/null
    return $?
}

echo_phase "PHASE 0: Infrastructure Verification"
echo "Testing that Docker containers (Kafka, PostgreSQL) are running..."
python3 -m pytest tests/test_docker_setup.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 1: Kafka Topics (Test)"
echo "Testing test Kafka topics - these will FAIL if Docker is not running,"
echo "then PASS once conftest.py auto-creates them."
echo ""
python3 -m pytest tests/kafka/test_topics.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 1.2: Tombstone Behavior"
echo "Testing tombstone behavior on test topics."
python3 -m pytest tests/kafka/test_tombstones.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 2: PostgreSQL Schema (Test)"
echo "Testing test PostgreSQL table - these will FAIL if Docker is not running,"
echo "then PASS once conftest.py auto-creates them."
echo ""
python3 -m pytest tests/postgres/test_schema.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 3.1: Spark Parsing Functions"
echo "Testing parsing functions - these test the functions directly, no infrastructure needed"
python3 -m pytest tests/spark/test_parsers.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 3.2: Spark Pipeline Integration"
python3 -m pytest tests/spark/test_pipeline.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 3.3: Data Integrity"
echo "Testing data integrity - these will FAIL until Spark pipeline processes data"
python3 -m pytest tests/spark/test_data_integrity.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "PHASE 4: End-to-End Tests"
echo "Testing complete session lifecycle - these will FAIL until entire pipeline is working"
python3 -m pytest tests/e2e/test_full_flow.py -v --tb=short 2>&1 | grep -E "(PASSED|FAILED|ERROR|test_)" || true

echo ""
echo_phase "SUMMARY"
echo "========================================"
echo "TDD Workflow:"
echo "1. Tests FAIL (RED) - infrastructure doesn't exist"
echo "2. Create infrastructure with scripts"
echo "3. Tests PASS (GREEN) - infrastructure exists"
echo ""
echo "To create all infrastructure:"
echo "  docker compose down"
echo "  docker compose up -d --build"
echo "  uv run python kafka/scripts/create_topics.py"
echo "  uv run python postgres/scripts/init_db.py"
echo "  uv run python spark/scripts/spark_kafka_to_postgres.py &"
echo ""
echo "Then run all tests:"
echo "  python3 -m pytest tests/ -v"
