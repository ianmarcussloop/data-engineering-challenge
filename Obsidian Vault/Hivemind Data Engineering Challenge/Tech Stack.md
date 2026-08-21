
| Component          | Technology             | Justification                                                             |
| ------------------ | ---------------------- | ------------------------------------------------------------------------- |
| Ingestion          | Kafka Consumer         | Real-time                                                                 |
| Processing         | Python                 |                                                                           |
| Short Term Storage | Kafka Topics           | Quick ad-hoc analysis? How would that work?                               |
| Long Term Storage  | Postgres + TimescaleDB | Postgres open source and robust + Timescale optimises for timeseries data |
| **Toolkit**        |                        |                                                                           |
| Host?              | Kubernetes             | Hosts architecture to be performant                                       |
| Spin up?           | Docker                 | Makes SQL database management easier                                      |
| Deployment         | Terraform              | Idempotent project deployment                                             |
