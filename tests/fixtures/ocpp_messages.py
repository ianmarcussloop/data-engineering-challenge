"""OCPP message generators for testing."""
import json


def start_transaction(chargerId: str, transactionId: str, meterStart: int = 0, idTag: str = None, timestamp: str = None, connectorId: int = None, wrap_for_kafka: bool = False) -> str:
    """Generate a StartTransaction OCPP message.
    
    Args:
        chargerId: The charger identifier
        transactionId: The transaction identifier
        meterStart: Starting meter value
        idTag: Optional RFID tag identifier
        timestamp: Optional ISO 8601 timestamp
        connectorId: Optional connector identifier
        wrap_for_kafka: If True, wrap in Kafka message format with chargerId, uniqueId, message fields
    
    Returns:
        Raw OCPP message string, or JSON-wrapped message if wrap_for_kafka=True
    """
    id_tag_str = f', "idTag": "{idTag}"' if idTag else ''
    connector_str = f', "connectorId": {connectorId}' if connectorId is not None else ''
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    
    ocpp_msg = f'[2, "{chargerId}", "StartTransaction", {{ "transactionId": "{transactionId}", "meterStart": {meterStart}{id_tag_str}{connector_str}{ts_str} }}]'
    
    if wrap_for_kafka:
        return json.dumps({"chargerId": chargerId, "uniqueId": transactionId, "message": ocpp_msg})
    return ocpp_msg


def meter_values(chargerId: str, transactionId: str, power: float, energy: float = None, soc: float = None, voltage: float = None, timestamp: str = None, wrap_for_kafka: bool = False) -> str:
    """Generate a MeterValues OCPP message.
    
    Args:
        chargerId: The charger identifier
        transactionId: The transaction identifier
        power: Power value in kW
        energy: Optional energy value in Wh
        soc: Optional State of Charge percentage
        voltage: Optional voltage value
        timestamp: Optional ISO 8601 timestamp
        wrap_for_kafka: If True, wrap in Kafka message format with proper OCPP structure
    
    Returns:
        Raw OCPP message string (flat format), or JSON-wrapped message with proper OCPP structure if wrap_for_kafka=True
    """
    if wrap_for_kafka:
        # For Kafka/pipeline: generate proper OCPP structure with meterValue and sampledValue arrays
        sampled_values = []
        
        sampled_values.append({
            "value": str(power),
            "measurand": "Power.Active.Import",
            "context": "Sample.Periodic",
            "format": "Raw",
            "location": "Outlet",
            "unit": "kW"
        })
        
        if energy is not None:
            sampled_values.append({
                "value": str(energy),
                "measurand": "Energy.Active.Import.Register",
                "context": "Sample.Periodic",
                "format": "Raw",
                "location": "Outlet",
                "unit": "Wh"
            })
        
        if soc is not None:
            sampled_values.append({
                "value": str(soc),
                "measurand": "Battery.SOC",
                "context": "Sample.Periodic",
                "format": "Raw",
                "location": "Outlet",
                "unit": "Percent"
            })
        
        if voltage is not None:
            sampled_values.append({
                "value": str(voltage),
                "measurand": "Voltage",
                "context": "Sample.Periodic",
                "format": "Raw",
                "location": "Outlet",
                "unit": "V"
            })
        
        payload = {
            "transactionId": transactionId,
            "meterValue": [{
                "timestamp": timestamp if timestamp else "",
                "sampledValue": sampled_values
            }]
        }
        ocpp_msg = f'[2, "{chargerId}", "MeterValues", {json.dumps(payload)}]'
        return json.dumps({"chargerId": chargerId, "uniqueId": transactionId, "message": ocpp_msg})
    else:
        # For direct use by other tests: flat format (backward compatible)
        energy_str = f', "energy": {energy}' if energy is not None else ''
        soc_str = f', "Battery.SOC": {soc}' if soc is not None else ''
        voltage_str = f', "Voltage": {voltage}' if voltage is not None else ''
        ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
        
        ocpp_msg = f'[2, "{chargerId}", "MeterValues", {{ "transactionId": "{transactionId}", "power": {power}{energy_str}{soc_str}{voltage_str}{ts_str} }}]'
        return ocpp_msg


def stop_transaction(chargerId: str, transactionId: str, meterStop: int, reason: str = "EVDriverDisconnected", timestamp: str = None, wrap_for_kafka: bool = False) -> str:
    """Generate a StopTransaction OCPP message.
    
    Args:
        chargerId: The charger identifier
        transactionId: The transaction identifier
        meterStop: Ending meter value
        reason: Stop reason (default: EVDriverDisconnected)
        timestamp: Optional ISO 8601 timestamp
        wrap_for_kafka: If True, wrap in Kafka message format with chargerId, uniqueId, message fields
    
    Returns:
        Raw OCPP message string, or JSON-wrapped message if wrap_for_kafka=True
    """
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    ocpp_msg = f'[2, "{chargerId}", "StopTransaction", {{ "transactionId": "{transactionId}", "meterStop": {meterStop}, "reason": "{reason}"{ts_str} }}]'
    
    if wrap_for_kafka:
        return json.dumps({"chargerId": chargerId, "uniqueId": transactionId, "message": ocpp_msg})
    return ocpp_msg


def remote_stop_transaction(chargerId: str, transactionId: str, timestamp: str = None, wrap_for_kafka: bool = False) -> str:
    """Generate a RemoteStopTransaction OCPP message.
    
    Args:
        chargerId: The charger identifier
        transactionId: The transaction identifier
        timestamp: Optional ISO 8601 timestamp
        wrap_for_kafka: If True, wrap in Kafka message format with chargerId, uniqueId, message fields
    
    Returns:
        Raw OCPP message string, or JSON-wrapped message if wrap_for_kafka=True
    """
    ts_str = f', "timestamp": "{timestamp}"' if timestamp else ''
    ocpp_msg = f'[2, "{chargerId}", "RemoteStopTransaction", {{ "transactionId": "{transactionId}"{ts_str} }}]'
    
    if wrap_for_kafka:
        return json.dumps({"chargerId": chargerId, "uniqueId": transactionId, "message": ocpp_msg})
    return ocpp_msg


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
