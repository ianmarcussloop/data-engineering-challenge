from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel

class ChargerSession(SQLModel, table=True):
    __tablename__ = "charger_session"

    # Primary key: charger name + timestamp
    sessionId: str = Field(
        primary_key=True,
        description="Charger name + timestamp (e.g., 'charger6_2025-08-26T23:59:57.599Z')"
    )

    # Required fields
    stationId: str = Field(
        description="Charger name (e.g., 'charger6')",
        index=True  # Index for faster queries on stationId
    )

    # Status logic:
    # - "active" if at least one MeterValues exists
    # - "ended" if at least one MeterValues and a StopTransaction exist
    # - null if no MeterValues exist
    status: Optional[str] = Field(
        default=None,
        description="Status of the session: 'active', 'ended', or null"
    )

    # terminationReason logic:
    # - null if status = "active" or status = null
    # - `reason` field from StopTransaction if status = "ended"
    terminationReason: Optional[str] = Field(
        default=None,
        description="Reason for session termination (from StopTransaction)"
    )

    # startTime: smallest timestamp of all MeterValues
    startTime: datetime = Field(
        description="Timestamp of the first MeterValues message for the session"
    )

    # endTime: timestamp of StopTransaction or null
    endTime: Optional[datetime] = Field(
        default=None,
        description="Timestamp of StopTransaction (null if no StopTransaction exists)"
    )

    # duration: endTime - startTime (in seconds) or null if endTime is null
    duration: Optional[int] = Field(
        default=None,
        description="Duration of the session in seconds (null if endTime is null)"
    )

    # totalEnergyConsumed: Average Power.Active.Import * duration (in kWh)
    totalEnergyConsumed: Optional[float] = Field(
        default=None,
        description="Total energy consumed in kWh (Average Power.Active.Import * duration)"
    )

    # eventCount: total number of actions for this stationId
    eventCount: int = Field(
        default=0,
        description="Total number of OCPP actions for this session"
    )