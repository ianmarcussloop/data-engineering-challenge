*Source: Mistral Vibe AI*

2 is for Call Request, 3 is for Call Response. The charger sends a Call Request to our system (2), and if the Request is successfully received, the system (?) sends back a call response to the charger(?) that the information was successfully received. This is verified by an empty json response: `{}`

*Schema*

| Call Request (2)                           |
| ------------------------------------------ |
| `[messageType, uniqueId, action, payload]` |
| **Call Response (3)**                      |
| `[messageType, uniqueId, payload]`         |

*Legend*

**Call Request (2)**

| Type        | Examples                                                                                                                                                                                                       |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| messageType | 2 for Call Request, 3 for Call Response                                                                                                                                                                        |
| uniqueId    | "ef51a638-0e05-4a9d-be52-594ada28f153"                                                                                                                                                                         |
| action      | - "MeterValues"<br>- "Heartbeat"<br>- "StatusNotification"<br>- "Authorize"<br>- and more (?)<br>- how to dynamically handle all? can we be sure we only get some actions (6-12) or will we handle many? (30+) |
| payload     | {"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:57.599Z","samp...                                                                                                              |
**Call Response (3)**

| Type        | Examples                                                                                                                                                                                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| messageType | 2 for Call Request, 3 for Call Response                                                                                                                                                                                                                       |
| uniqueId    | "ef51a638-0e05-4a9d-be52-594ada28f153"                                                                                                                                                                                                                        |
| payload     | - `{}` for successful Request received (?)<br><br>- So if we have a Call Request for a uniqueId, but don't have a Call Response for the corresponding uniqueId, we can assume something went wrong with the data reception and start doing error handling (?) |
