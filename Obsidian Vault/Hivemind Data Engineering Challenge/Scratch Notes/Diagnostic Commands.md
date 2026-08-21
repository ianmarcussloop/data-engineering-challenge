| Check                  | Command                                                                                   | Container         |
| ---------------------- | ----------------------------------------------------------------------------------------- | ----------------- |
| List Kafka topics      | kafka-topics --bootstrap-server kafka:29092 --list                                        | kafka             |
| Consume Kafka messages | kafka-console-consumer --bootstrap-server kafka:29092 --topic your_topic --from-beginning | kafka             |
| List PostgreSQL tables | psql -U ev_user -d ev_coorp -c "\dt"                                                      | ev_coorp_postgres |
| View table schema      | psql -U ev_user -d ev_coorp -c "\d your_table"                                            | ev_coorp_postgres |
| Count records          | psql -U ev_user -d ev_coorp -c "SELECT COUNT(*) FROM your_table;"                         | ev_coorp_postgres |
| PostgreSQL health      | pg_isready -U ev_user -d ev_coorp                                                         | ev_coorp_postgres |
| Kafka broker status    | kafka-broker-api-versions --bootstrap-server kafka:29092                                  | kafka             |