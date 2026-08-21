from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel


class OcppHistory(SQLModel, table=True):
    __tablename__ = "ocpp.history"
    __table_args__ = {"extend_existing": True}

    # Key fields
    session_id: str = Field(
        sa_column_kwargs={"name": "sessionId"},
        primary_key=True,
        description="Charger name + startTime (e.g., 'charger6_2025-08-26T23:59:57.599Z')"
    )

    station_id: str = Field(
        sa_column_kwargs={"name": "stationId"},
        description="Charger name (e.g., 'charger6')",
        index=True
    )

    transaction_id: str = Field(
        sa_column_kwargs={"name": "transactionId"},
        description="OCPP transaction ID",
        index=True
    )

    start_time: datetime = Field(
        sa_column_kwargs={"name": "startTime"},
        description="Timestamp of the first MeterValues or StartTransaction",
        index=True
    )

    end_time: datetime = Field(
        sa_column_kwargs={"name": "endTime"},
        description="Timestamp of StopTransaction or RemoteStopTransaction",
        index=True
    )

    duration: int = Field(
        description="Duration of the session in seconds (endTime - startTime)"
    )

    termination_reason: Optional[str] = Field(
        sa_column_kwargs={"name": "terminationReason"},
        default=None,
        description="Reason for session termination (from StopTransaction)",
        index=True
    )

    total_energy_consumed: Optional[float] = Field(
        sa_column_kwargs={"name": "totalEnergyConsumed"},
        default=None,
        description="Total energy consumed in kWh"
    )

    avg_power: Optional[float] = Field(
        sa_column_kwargs={"name": "avgPower"},
        default=None,
        description="Average power during the session"
    )

    max_power: Optional[float] = Field(
        sa_column_kwargs={"name": "maxPower"},
        default=None,
        description="Maximum power during the session"
    )

    id_tag: Optional[str] = Field(
        sa_column_kwargs={"name": "idTag"},
        default=None,
        description="ID tag from StartTransaction"
    )

    connector_id: Optional[int] = Field(
        sa_column_kwargs={"name": "connectorId"},
        default=None,
        description="Connector ID from StartTransaction"
    )

    meter_start: Optional[int] = Field(
        sa_column_kwargs={"name": "meterStart"},
        default=None,
        description="Meter start value from StartTransaction"
    )

    meter_stop: Optional[int] = Field(
        sa_column_kwargs={"name": "meterStop"},
        default=None,
        description="Meter stop value from StopTransaction"
    )

    soc_start: Optional[float] = Field(
        sa_column_kwargs={"name": "socStart"},
        default=None,
        description="State of Charge at start (percentage)"
    )

    soc_end: Optional[float] = Field(
        sa_column_kwargs={"name": "socEnd"},
        default=None,
        description="State of Charge at end (percentage)"
    )

    voltage_avg: Optional[float] = Field(
        sa_column_kwargs={"name": "voltageAvg"},
        default=None,
        description="Average voltage during the session"
    )

    # Additional metadata
    event_count: int = Field(
        sa_column_kwargs={"name": "eventCount"},
        default=0,
        description="Total number of OCPP messages for this session"
    )
