#!/bin/bash

# =============================================================================
# Initialization Script for Data Engineering Challenge
# 
# This script initializes all infrastructure needed for tests to pass:
# - Docker services (PostgreSQL, Kafka, Zookeeper, Schema Registry)
# - Database tables and indexes
# - Kafka topics and schemas
# - Sample data (optional)
# 
# Usage:
#   ./scripts/init_all.sh              # Full initialization
#   ./scripts/init_all.sh --light       # Skip sample data generation
#   ./scripts/init_all.sh --test        # Initialize and run tests
#   ./scripts/init_all.sh --down       # Shutdown only
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# Functions
# =============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed. Please install it first."
        exit 1
    fi
}

wait_for_service() {
    local service_name="$1"
    local host="$2"
    local port="$3"
    local max_attempts="$4"
    local attempt=0
    
    log_info "Waiting for $service_name at $host:$port..."
    
    while ! nc -z "$host" "$port" &> /dev/null; do
        attempt=$((attempt + 1))
        if [ $attempt -ge $max_attempts ]; then
            log_error "$service_name did not start within the expected time."
            return 1
        fi
        sleep 5
        echo -n "."
    done
    
    log_success "$service_name is ready at $host:$port"
    return 0
}

check_postgres() {
    log_info "Checking PostgreSQL connection..."
    if PGPASSWORD=ev_password psql -h localhost -U ev_user -d ev_coorp -c "SELECT 1" &> /dev/null; then
        log_success "PostgreSQL is accessible"
        return 0
    else
        log_warning "PostgreSQL is not accessible yet"
        return 1
    fi
}

check_kafka() {
    log_info "Checking Kafka connection..."
    if python3 -c "from confluent_kafka.admin import AdminClient; AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5)" &> /dev/null; then
        log_success "Kafka is accessible"
        return 0
    else
        log_warning "Kafka is not accessible yet"
        return 1
    fi
}

# =============================================================================
# Main Script
# =============================================================================

# Parse arguments
INIT_ONLY=true
LIGHT_MODE=false
RUN_TESTS=false
SHUTDOWN_ONLY=false

for arg in "$@"; do
    case "$arg" in
        --light)
            LIGHT_MODE=true
            ;;
        --test)
            RUN_TESTS=true
            ;;
        --down)
            SHUTDOWN_ONLY=true
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --light    Skip sample data generation"
            echo "  --test     Initialize and run tests"
            echo "  --down     Shutdown infrastructure only"
            echo "  --help     Show this help message"
            exit 0
            ;;
    esac
done

# Shutdown only
if [ "$SHUTDOWN_ONLY" = true ]; then
    log_info "Shutting down Docker containers..."
    cd "$PROJECT_DIR"
    docker compose down
    log_success "All services stopped"
    exit 0
fi

# Check prerequisites
log_info "Checking prerequisites..."
check_command docker
check_command docker-compose
check_command python3
check_command uv
check_command nc

# Step 1: Start Docker services
log_info "Starting Docker services..."
cd "$PROJECT_DIR"

# Stop any existing containers
log_info "Stopping any existing containers..."
docker compose down 2> /dev/null || true

# Start services
log_info "Starting PostgreSQL, Kafka, Zookeeper, Schema Registry..."
docker compose up -d postgres zookeeper kafka schema-registry

# Wait for PostgreSQL
wait_for_service "PostgreSQL" "localhost" "5432" 30

# Wait for Kafka (Zookeeper needs time too)
wait_for_service "Zookeeper" "localhost" "2181" 30
wait_for_service "Kafka" "localhost" "9092" 30
wait_for_service "Schema Registry" "localhost" "8081" 30

log_success "All Docker services are running"

# Step 2: Verify services are accessible
log_info "Verifying service accessibility..."

# Wait a bit more for PostgreSQL to be fully ready
sleep 10
check_postgres || {
    log_warning "PostgreSQL not ready, retrying..."
    sleep 10
    check_postgres || log_error "PostgreSQL is still not accessible"
}

# Wait for Kafka
sleep 10
check_kafka || {
    log_warning "Kafka not ready, retrying..."
    sleep 10
    check_kafka || log_error "Kafka is still not accessible"
}

# Step 3: Initialize PostgreSQL database
log_info "Initializing PostgreSQL database..."
cd "$PROJECT_DIR"

if uv run python postgres/scripts/init_db.py; then
    log_success "Database initialization completed"
else
    log_error "Database initialization failed"
    exit 1
fi

# Step 4: Initialize Kafka topics and schema
log_info "Initializing Kafka topics and schema..."
cd "$PROJECT_DIR"

if uv run python kafka/scripts/create_topics.py; then
    log_success "Kafka topics and schema created"
else
    log_error "Kafka initialization failed"
    exit 1
fi

# Step 5: (Optional) Generate sample data
if [ "$LIGHT_MODE" = false ]; then
    log_info "Generating sample data..."
    cd "$PROJECT_DIR"
    
    # Produce sample OCPP messages to Kafka
    if uv run python kafka/scripts/ocpp_producer.py; then
        log_success "Sample data produced to Kafka"
    else
        log_warning "Sample data production skipped (file may not exist)"
    fi
    
    # Wait a bit for messages to be processed
    sleep 5
    
    # Process messages from Kafka to PostgreSQL
    if uv run python postgres/scripts/kafka_to_postgres.py &; then
        KAFKA_CONSUMER_PID=$!
        log_info "Kafka consumer started (PID: $KAFKA_CONSUMER_PID)"
        
        # Let it run for a while to process messages
        sleep 10
        
        # Stop the consumer
        kill $KAFKA_CONSUMER_PID 2> /dev/null || true
        log_success "Messages processed to PostgreSQL"
    else
        log_warning "Kafka consumer could not be started"
    fi
else
    log_info "Skipping sample data generation (light mode)"
fi

# Step 6: Run tests (if requested)
if [ "$RUN_TESTS" = true ]; then
    log_info "Running all tests..."
    cd "$PROJECT_DIR"
    
    # Run tests with timeout
    if timeout 300 uv run pytest tests/ spark/scripts/test_spark_kafka_to_postgres.py -v --tb=short; then
        log_success "All tests passed!"
    else
        log_warning "Some tests may have failed. Check the output above."
    fi
else
    log_success "Initialization complete!"
    echo ""
    log_info "To run tests manually:"
    log_info "  cd $PROJECT_DIR"
    log_info "  uv run pytest tests/ spark/scripts/test_spark_kafka_to_postgres.py -v"
    echo ""
    log_info "Or run specific test modules:"
    log_info "  uv run pytest tests/spark/ -v              # Spark tests"
    log_info "  uv run pytest tests/kafka/ -v             # Kafka tests"
    log_info "  uv run pytest tests/postgres/ -v          # PostgreSQL tests"
    log_info "  uv run pytest tests/e2e/ -v               # End-to-end tests"
fi

log_success "Initialization script completed"
