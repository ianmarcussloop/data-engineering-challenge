- Kafka chosen for real-time insights capability
- Postgres for well known sql accessibility for historical insights
- Spark for data growth scalability
- Test suite to guardrail agentic development
- Agentic development to accelerate MVP delivery 
- Grafana for dashboard viewing functionality






Technical Debt / Shortcuts / Production Concerns
- *.txt -> `ocpp.messages` without spark for ease of development
- Spark pipeline speed optimisation
- sql-like viewing layer for kafka topics (not currently filterable)


Next Steps
- "determine operational status in incomplete or inconsistent scenarios"
- cleanup superfluous code
- manual data verification