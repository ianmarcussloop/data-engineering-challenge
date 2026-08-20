#!/usr/bin/env python3
"""
Initialization Script for Data Engineering Challenge

This script initializes all infrastructure needed for tests to pass:
- Docker services (PostgreSQL, Kafka, Zookeeper, Schema Registry)
- Database tables and indexes
- Kafka topics and schemas
- Sample data (optional)

Usage:
    python scripts/init_all.py              # Full initialization
    python scripts/init_all.py --light     # Skip sample data generation
    python scripts/init_all.py --test       # Initialize and run tests
    python scripts/init_all.py --down       # Shutdown only
    python scripts/init_all.py --help       # Show help
"""

import argparse
import os
import sys
import subprocess
import time
import socket
from pathlib import Path
from typing import Optional, Tuple

# Colors for output
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    NC = '\033[0m'  # No Color


class InitScript:
    """Main initialization script class."""
    
    def __init__(self):
        self.project_dir = Path(__file__).parent.parent.absolute()
        self.scripts_dir = Path(__file__).parent.absolute()
        
    def log_info(self, message: str) -> None:
        """Log an info message."""
        print(f"{Colors.BLUE}[INFO]{Colors.NC} {message}")
    
    def log_success(self, message: str) -> None:
        """Log a success message."""
        print(f"{Colors.GREEN}[SUCCESS]{Colors.NC} {message}")
    
    def log_warning(self, message: str) -> None:
        """Log a warning message."""
        print(f"{Colors.YELLOW}[WARNING]{Colors.NC} {message}")
    
    def log_error(self, message: str) -> None:
        """Log an error message."""
        print(f"{Colors.RED}[ERROR]{Colors.NC} {message}")
    
    def check_command(self, command: str) -> bool:
        """Check if a command is available."""
        try:
            subprocess.run(
                ["which", command],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            return True
        except subprocess.CalledProcessError:
            self.log_error(f"{command} is not installed. Please install it first.")
            return False
    
    def check_port(self, host: str, port: int, timeout: float = 1.0) -> bool:
        """Check if a port is open."""
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, ConnectionRefusedError):
            return False
    
    def wait_for_service(
        self, 
        service_name: str, 
        host: str, 
        port: int, 
        max_attempts: int = 30,
        delay: float = 5.0
    ) -> bool:
        """Wait for a service to become available."""
        self.log_info(f"Waiting for {service_name} at {host}:{port}...")
        
        for attempt in range(max_attempts):
            if self.check_port(host, port):
                self.log_success(f"{service_name} is ready at {host}:{port}")
                return True
            
            if attempt < max_attempts - 1:
                print(".", end="", flush=True)
                time.sleep(delay)
        
        self.log_error(f"{service_name} did not start within the expected time.")
        return False
    
    def check_postgres(self) -> bool:
        """Check if PostgreSQL is accessible."""
        self.log_info("Checking PostgreSQL connection...")
        try:
            result = subprocess.run(
                ["psql", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp", "-c", "SELECT 1"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                self.log_success("PostgreSQL is accessible")
                return True
            else:
                self.log_warning("PostgreSQL is not accessible yet")
                return False
        except Exception:
            self.log_warning("PostgreSQL is not accessible yet")
            return False
    
    def check_kafka(self) -> bool:
        """Check if Kafka is accessible."""
        self.log_info("Checking Kafka connection...")
        try:
            result = subprocess.run(
                ["python3", "-c", 
                 "from confluent_kafka.admin import AdminClient; "
                 "AdminClient({'bootstrap.servers':'localhost:9092'}).list_topics(timeout=5)"],
                capture_output=True,
                timeout=10
            )
            if result.returncode == 0:
                self.log_success("Kafka is accessible")
                return True
            else:
                self.log_warning("Kafka is not accessible yet")
                return False
        except Exception:
            self.log_warning("Kafka is not accessible yet")
            return False
    
    def run_command(self, command: str, cwd: Optional[str] = None) -> bool:
        """Run a command and return success status."""
        self.log_info(f"Running: {' '.join(command)}")
        try:
            result = subprocess.run(
                command,
                cwd=cwd,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return True
            else:
                self.log_error(f"Command failed: {result.stderr}")
                return False
        except Exception as e:
            self.log_error(f"Command error: {e}")
            return False
    
    def run_uv_command(self, script_path: str) -> bool:
        """Run a Python script using uv."""
        cmd = ["uv", "run", "python", script_path]
        return self.run_command(cmd, cwd=str(self.project_dir))
    
    def start_docker_services(self) -> bool:
        """Start Docker services."""
        self.log_info("Starting Docker services...")
        
        # Stop any existing containers
        self.log_info("Stopping any existing containers...")
        subprocess.run(
            ["docker", "compose", "down"],
            cwd=str(self.project_dir),
            capture_output=True
        )
        
        # Start services
        self.log_info("Starting PostgreSQL, Kafka, Zookeeper, Schema Registry...")
        result = subprocess.run(
            ["docker", "compose", "up", "-d", "postgres", "zookeeper", "kafka", "schema-registry"],
            cwd=str(self.project_dir),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            self.log_error(f"Failed to start services: {result.stderr}")
            return False
        
        # Print container status
        print(result.stdout)
        return True
    
    def initialize_database(self) -> bool:
        """Initialize PostgreSQL database."""
        self.log_info("Initializing PostgreSQL database...")
        return self.run_uv_command("postgres/scripts/init_db.py")
    
    def initialize_kafka(self) -> bool:
        """Initialize Kafka topics and schema."""
        self.log_info("Initializing Kafka topics and schema...")
        return self.run_uv_command("kafka/scripts/create_topics.py")
    
    def generate_sample_data(self) -> bool:
        """Generate sample data."""
        self.log_info("Generating sample data...")
        
        # Produce sample OCPP messages to Kafka
        if not self.run_uv_command("kafka/scripts/ocpp_producer.py"):
            self.log_warning("Sample data production failed")
            return False
        
        self.log_success("Sample data produced to Kafka")
        
        # Wait a bit for messages to be processed
        time.sleep(5)
        
        # Process messages from Kafka to PostgreSQL (run in background)
        self.log_info("Processing messages to PostgreSQL...")
        proc = subprocess.Popen(
            ["uv", "run", "python", "postgres/scripts/kafka_to_postgres.py"],
            cwd=str(self.project_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Let it run for a while
        time.sleep(10)
        
        # Stop the consumer
        proc.terminate()
        proc.wait(timeout=5)
        
        self.log_success("Messages processed to PostgreSQL")
        return True
    
    def run_tests(self) -> bool:
        """Run all tests."""
        self.log_info("Running all tests...")
        
        cmd = [
            "uv", "run", "pytest",
            "tests/",
            "spark/scripts/test_spark_kafka_to_postgres.py",
            "-v",
            "--tb=short"
        ]
        
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_dir),
                timeout=600  # 10 minute timeout
            )
            
            if result.returncode == 0:
                self.log_success("All tests passed!")
                return True
            else:
                self.log_warning("Some tests may have failed. Check the output above.")
                return False
        except subprocess.TimeoutExpired:
            self.log_warning("Tests timed out after 10 minutes")
            return False
    
    def shutdown(self) -> bool:
        """Shutdown Docker containers."""
        self.log_info("Shutting down Docker containers...")
        result = subprocess.run(
            ["docker", "compose", "down"],
            cwd=str(self.project_dir),
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            self.log_success("All services stopped")
            return True
        else:
            self.log_error(f"Failed to stop services: {result.stderr}")
            return False
    
    def run(self, args: argparse.Namespace) -> None:
        """Main run method."""
        # Shutdown only
        if args.down:
            self.shutdown()
            return
        
        # Check prerequisites
        self.log_info("Checking prerequisites...")
        required_commands = ["docker", "docker-compose", "python3", "uv"]
        for cmd in required_commands:
            if not self.check_command(cmd):
                sys.exit(1)
        
        # Start Docker services
        if not self.start_docker_services():
            sys.exit(1)
        
        # Wait for services
        services = [
            ("PostgreSQL", "localhost", 5432, 30),
            ("Zookeeper", "localhost", 2181, 30),
            ("Kafka", "localhost", 9092, 30),
            ("Schema Registry", "localhost", 8081, 30),
        ]
        
        for service_name, host, port, max_attempts in services:
            if not self.wait_for_service(service_name, host, port, max_attempts):
                sys.exit(1)
        
        self.log_success("All Docker services are running")
        
        # Additional wait for services to be fully ready
        self.log_info("Verifying service accessibility...")
        time.sleep(10)
        
        if not self.check_postgres():
            self.log_warning("PostgreSQL not ready, retrying...")
            time.sleep(10)
            if not self.check_postgres():
                self.log_error("PostgreSQL is still not accessible")
                sys.exit(1)
        
        time.sleep(5)
        if not self.check_kafka():
            self.log_warning("Kafka not ready, retrying...")
            time.sleep(10)
            if not self.check_kafka():
                self.log_error("Kafka is still not accessible")
                sys.exit(1)
        
        # Initialize database
        if not self.initialize_database():
            self.log_error("Database initialization failed")
            sys.exit(1)
        
        # Initialize Kafka
        if not self.initialize_kafka():
            self.log_error("Kafka initialization failed")
            sys.exit(1)
        
        # Generate sample data (unless light mode)
        if not args.light:
            if not self.generate_sample_data():
                self.log_warning("Sample data generation failed (continuing anyway)")
        else:
            self.log_info("Skipping sample data generation (light mode)")
        
        # Run tests (if requested)
        if args.test:
            if not self.run_tests():
                sys.exit(1)
        else:
            self.log_success("Initialization complete!")
            print()
            self.log_info("To run tests manually:")
            self.log_info(f"  cd {self.project_dir}")
            self.log_info("  uv run pytest tests/ spark/scripts/test_spark_kafka_to_postgres.py -v")
            print()
            self.log_info("Or run specific test modules:")
            self.log_info("  uv run pytest tests/spark/ -v              # Spark tests")
            self.log_info("  uv run pytest tests/kafka/ -v             # Kafka tests")
            self.log_info("  uv run pytest tests/postgres/ -v          # PostgreSQL tests")
            self.log_info("  uv run pytest tests/e2e/ -v               # End-to-end tests")
        
        self.log_success("Initialization script completed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Initialize infrastructure for Data Engineering Challenge"
    )
    parser.add_argument(
        "--light",
        action="store_true",
        help="Skip sample data generation"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Initialize and run tests"
    )
    parser.add_argument(
        "--down",
        action="store_true",
        help="Shutdown infrastructure only"
    )
    
    args = parser.parse_args()
    
    script = InitScript()
    script.run(args)


if __name__ == "__main__":
    main()
