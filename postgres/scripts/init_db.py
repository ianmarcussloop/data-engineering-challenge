import os
from sqlmodel import create_engine, SQLModel
from sqlalchemy import text
from postgres.schema.charger_session import ChargerSession
from postgres.schema.ocpp_history import OcppHistory
from dotenv import load_dotenv

# Load environment variables (optional)
load_dotenv()

# Database connection URL (from environment or hardcoded)
DB_URL = os.getenv("DATABASE_URL", "postgresql://ev_user:ev_password@localhost:5432/ev_coorp")

# Create SQLModel engine
engine = create_engine(DB_URL)

# Create the table
def create_tables():
    # Create ocpp schema for potential future use
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS ocpp"))
        conn.commit()
    
    SQLModel.metadata.create_all(engine)
    
    # Verify the table was created
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name = 'ocpp.history'
        """))
        if result.fetchone() is None:
            print("Warning: ocpp.history table was not created")
        else:
            print("ocpp.history table created successfully")
    
    # Enable TimescaleDB extension and hypertable
    # with engine.connect() as conn:
        # Enable TimescaleDB extension
        # conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        # Create hypertable for time-based partitioning
        # conn.execute("SELECT create_hypertable('charger_session', 'startTime');")
        # conn.commit()
    print("Tables created successfully!")

if __name__ == "__main__":
    create_tables()