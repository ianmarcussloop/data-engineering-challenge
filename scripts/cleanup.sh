#!/bin/bash

# =============================================================================
# Cleanup Script for Data Engineering Challenge
# 
# This script cleans up either the test environment or the main project environment
# by truncating all data from PostgreSQL tables and emptying all Kafka topics.
# 
# Usage:
#   ./scripts/cleanup.sh              # Clean up main project environment
#   ./scripts/cleanup.sh --test        # Clean up test environment
#   ./scripts/cleanup.sh --main        # Clean up main project environment (explicit)
#   ./scripts/cleanup.sh --all         # Clean up both environments
#   ./scripts/cleanup.sh --help        # Show help
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

# =============================================================================
# Main Script
# =============================================================================

# Parse arguments
ENVIRONMENT="main"

for arg in "$@"; do
    case "$arg" in
        --test)
            ENVIRONMENT="test"
            ;;
        --main)
            ENVIRONMENT="main"
            ;;
        --all)
            ENVIRONMENT="all"
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --test     Clean up test environment only"
            echo "  --main     Clean up main project environment (default)"
            echo "  --all      Clean up both main and test environments"
            echo "  --help     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $arg"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check prerequisites
log_info "Checking prerequisites..."
check_command python3
check_command uv

# Run the cleanup script
log_info "Running cleanup for $ENVIRONMENT environment..."
cd "$PROJECT_DIR"

case "$ENVIRONMENT" in
    "test")
        uv run python scripts/cleanup.py --test
        ;;
    "main")
        uv run python scripts/cleanup.py --main
        ;;
    "all")
        uv run python scripts/cleanup.py --all
        ;;
esac

if [ $? -eq 0 ]; then
    log_success "Cleanup completed successfully!"
else
    log_error "Cleanup failed!"
    exit 1
fi
