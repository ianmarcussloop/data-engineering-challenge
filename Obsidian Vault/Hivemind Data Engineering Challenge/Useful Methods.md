
| Method Name          | Parameters                | Description                                                                                                                                                          |
| -------------------- | ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| getAllChargers       | None                      | returns a list of all of the active (?) charger names available in the dataset (e.g. ["charger1", "charger2",...]) (list(str))                                       |
| getPayloadForCharger | chargerName (str)         | returns the request object payload (e.g. {"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:57.599Z","samp...) (json)                   |
| getAllActionTypes    | None                      | returns all action types available in the dataset (e.g. ["MeterValues", "Heartbeat", ...]) (list(str))                                                               |
| requestHasResponse   | callRequest (CallRequest) | checks if the given CallRequest object has a corresponding and successful CallResponse object for the same uniqueId. Returns True if successful, False if not (bool) |

UDTs

| User Defined Type | Type                    | Description                                |
| ----------------- | ----------------------- | ------------------------------------------ |
| CallRequest       | `[int, str, str, json]` | `[messageType, uniqueId, action, payload]` |
| CallResponse      | `[int, str, json]`      | `[messageType, uniqueId, payload]`         |

