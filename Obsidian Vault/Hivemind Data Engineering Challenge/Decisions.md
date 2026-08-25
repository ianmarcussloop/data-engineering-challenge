- Schema Registry included because it validates every message based on the schema (Currently in JSON format for ease of MVP creation, had bugs)
	- Allows easier schema changes post-deployment
- Add Kafka plugin to Grafana instead of having ksql? (plugin doesn't exist, AI hallucination)
- We have `uv run python postgres/scripts/kafka_to_postgres.py` for small datasets and `uv run python spark/scripts/spark_kafka_to_postgres.py` for larger ones
- abandon avro schema for now to get spark working, implement later if enough time
- can we rely on schema-less json because our data is following ocpp and we can more reasonably expect it to adhere to its predefined schema?
- I will use a compacted kafka topic for monitoring ongoing real-time charging sessions, as doing this with a sql table would cause huge slowdowns caused by blocking from the constant table upserts, which would conflict with the user trying to read the table in a useful way
- Is there a use case from the user where they want to join data from in-progress active kafka charging sessions and completed sessions in postgres? because current architecture does not allow this, a bridge would need to be built or base architecture would need to be changed to enable this, if this is something of use to consider implementing.

- Conisderation: If charging sessions complete more often than expected, then the postgres table will be transaction blocked and unusable. In this case I would only update the table at less frequent intervals to keep it usable.

- AI was unable to identify port issue for test vs production docker containers but I was :)