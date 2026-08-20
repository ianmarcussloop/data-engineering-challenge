"""Create production PostgreSQL schema for testing."""

import psycopg2
from sqlalchemy import create_engine
from sqlmodel import SQLModel

DB_URL = "postgresql://ev_user:ev_password@localhost:5432/ev_coorp"

conn = psycopg2.connect(DB_URL, connect_timeout=5)
cursor = conn.cursor()

# Create schema
cursor.execute("CREATE SCHEMA IF NOT EXISTS ocpp")
conn.commit()
print("✓ Created ocpp schema")

# Import the model after schema is created
from postgres.schema.ocpp_history import OcppHistory

# Create the table using SQLModel
engine = create_engine(DB_URL)
SQLModel.metadata.create_all(engine)
print("✓ Created ocpp.history table")

# Create indexes
indexes = [
    ("idx_ocpp_history_stationId", "stationId"),
    ("idx_ocpp_history_transactionId", "transactionId"),
    ("idx_ocpp_history_startTime", "startTime"),
    ("idx_ocpp_history_endTime", "endTime"),
    ("idx_ocpp_history_terminationReason", "terminationReason")
]

for index_name, column in indexes:
    cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON ocpp.history ({column})")
    conn.commit()
    print(f"✓ Created index {index_name}")

# Verify
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'ocpp'")
tables = cursor.fetchall()
print(f"\n✓ Schema 'ocpp' contains tables: {[t[0] for t in tables]}")

conn.close()
print("\n✓ Production schema setup complete!")
