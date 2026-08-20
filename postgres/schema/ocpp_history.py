from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel


class OcppHistory(SQLModel, table=True):
    __tablename__ = "ocpp.history"

    # Key fields
    sessionId: str = Field(
        primary_key=True,
        description="Charger name + startTime (e.g., 'charger6_2025-08-26T23:59:57.599Z')"
    )

    stationId: str = Field(
        description="Charger name (e.g., 'charger6')",
        index=True
    )

    transactionId: str = Field(
        description="OCPP transaction ID",
        index=True
    )

    startTime: datetime = Field(
        description="Timestamp of the first MeterValues or StartTransaction",
        index=True
    )

    endTime: datetime = Field(
        description="Timestamp of StopTransaction or RemoteStopTransaction",
        index=True
    )

    duration: int = Field(
        description="Duration of the session in seconds (endTime - startTime)"
    )

    terminationReason: Optional[str] = Field(
        default=None,
        description="Reason for session termination (from StopTransaction)",
        index=True
    )

    totalEnergyConsumed: Optional[float] = Field(
        default=None,
        description="Total energy consumed in kWh"
    )

    avgPower: Optional[float] = Field(
        default=None,
        description="Average power during the session"
    )

    maxPower: Optional[float] = Field(
        default=None,
        description="Maximum power during the session"
    )

    idTag: Optional[str] = Field(
        default=None,
        description="ID tag from StartTransaction"
    )

    connectorId: Optional[int] = Field(
        default=None,
        description="Connector ID from StartTransaction"
    )

    meterStart: Optional[int] = Field(
        default=None,
        description="Meter start value from StartTransaction"
    )

    meterStop: Optional[int] = Field(
        default=None,
        description="Meter stop value from StopTransaction"
    )

    socStart: Optional[float] = Field(
        default=None,
        description="State of Charge at start (percentage)"
    )

    socEnd: Optional[float] = Field(
        default=None,
        description="State of Charge at end (percentage)"
    )

    voltageAvg: Optional[float] = Field(
        default=None,
        description="Average voltage during the session"
    )

    # Additional metadata
    eventCount: int = Field(
        default=0,
        description="Total number of OCPP messages for this session"
    )
