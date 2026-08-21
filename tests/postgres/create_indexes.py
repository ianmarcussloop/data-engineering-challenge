import psycopg2

conn = psycopg2.connect('postgresql://ev_user:ev_password@localhost:5432/ev_coorp', connect_timeout=5)
cursor = conn.cursor()

# Create missing indexes on "ocpp.history" table
indexes = [
    'idx_ocpp.history_startTime',
    'idx_ocpp.history_endTime', 
    'idx_ocpp.history_terminationReason'
]

for index_name in indexes:
    # Map index name to column name
    col_map = {
        'startTime': 'startTime',
        'endTime': 'endTime',
        'terminationReason': 'terminationReason'
    }
    col = index_name.split('_')[-1]
    col_proper = col_map.get(col, col)
    
    print(f"Creating index {index_name} on column {col_proper}")
    cursor.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "ocpp.history" ("{col_proper}")')
    conn.commit()

# Verify
cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = %s", ('ocpp.history',))
indexes_created = [row[0] for row in cursor.fetchall()]
print(f"\nIndexes on ocpp.history: {indexes_created}")

conn.close()
