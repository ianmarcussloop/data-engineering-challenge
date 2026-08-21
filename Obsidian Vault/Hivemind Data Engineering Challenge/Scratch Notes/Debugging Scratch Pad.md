# kafka  

# read topic messages  
docker exec -it schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ocpp.messages \
  --from-beginning \
  --property schema.registry.url=http://schema-registry:8081
# postgres

psql postgresql://ev_user:ev_password@localhost:5432/ev_coorp -c "SELECT * FROM charger_session LIMIT 10;"

SELECT count(*) FROM charger_session;