"""OCPP message generators for testing."""


def start_transaction(chargerId: str, transactionId: str, meterStart: int = 0, idTag: str = None, timestamp: str = None, connectorId: int = None) -> str:
    """Generate a StartTransaction OCPP message."""
    id_tag_str = f', "idTag": "{idTag}"' if idTag else ''
    connector_str = f', "connectorId": {connectorId}' if connectorId is not None else ''
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    
    return f'[2, "{chargerId}", "StartTransaction", {{ "transactionId": "{transactionId}", "meterStart": {meterStart}{id_tag_str}{connector_str}{ts_str} }}]'


def meter_values(chargerId: str, transactionId: str, power: float, energy: float = None, soc: float = None, voltage: float = None, timestamp: str = None) -> str:
    """Generate a MeterValues OCPP message."""
    energy_str = f', "energy": {energy}' if energy is not None else ''
    soc_str = f', "Battery.SOC": {soc}' if soc is not None else ''
    voltage_str = f', "Voltage": {voltage}' if voltage is not None else ''
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    
    return f'[2, "{chargerId}", "MeterValues", {{ "transactionId": "{transactionId}", "power": {power}{energy_str}{soc_str}{voltage_str}{ts_str} }}]'


def stop_transaction(chargerId: str, transactionId: str, meterStop: int, reason: str = "EVDriverDisconnected", timestamp: str = None) -> str:
    """Generate a StopTransaction OCPP message."""
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    return f'[2, "{chargerId}", "StopTransaction", {{ "transactionId": "{transactionId}", "meterStop": {meterStop}, "reason": "{reason}"{ts_str} }}]'


def remote_stop_transaction(chargerId: str, transactionId: str, timestamp: str = None) -> str:
    """Generate a RemoteStopTransaction OCPP message."""
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    return f'[2, "{chargerId}", "RemoteStopTransaction", {{ "transactionId": "{transactionId}"{ts_str} }}]'


# Example usage:
if __name__ == "__main__":
    # A complete session lifecycle
    session_id_base = "2025-08-18T10:00:00.000Z"
    
    msg1 = start_transaction("charger1", "txn001", meterStart=1000, idTag="RFID123", timestamp="2025-08-18T10:00:00.000Z")
    msg2 = meter_values("charger1", "txn001", power=22.5, energy=1050, soc=50.0, voltage=230, timestamp="2025-08-18T10:01:00.000Z")
    msg3 = meter_values("charger1", "txn001", power=22.5, energy=1060, soc=52.0, voltage=230, timestamp="2025-08-18T10:02:00.000Z")
    msg4 = stop_transaction("charger1", "txn001", meterStop=1100, reason="EVDriverDisconnected", timestamp="2025-08-18T10:05:00.000Z")
    
    print("StartTransaction:", msg1)
    print("MeterValues:", msg2)
    print("MeterValues:", msg3)
    print("StopTransaction:", msg4)
