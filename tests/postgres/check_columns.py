import psycopg2

conn = psycopg2.connect('postgresql://ev_user:ev_password@localhost:5432/ev_coorp', connect_timeout=5)
cursor = conn.cursor()

# Use parameterized query
cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", ('ocpp.history',))
for row in cursor.fetchall():
    print(f'{row[0]}: {row[1]}')

# Check indexes
print("\nIndexes:")
cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = %s", ('ocpp.history',))
for row in cursor.fetchall():
    print(f'  {row[0]}')

conn.close()
