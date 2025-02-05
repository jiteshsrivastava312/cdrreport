import psycopg2
import pandas as pd
from io import StringIO
import datetime

# PostgreSQL connection details
db_config = {
    "host": "192.168.220.29",
    "database": "nguccreports",
    "user": "postgres",
    "password": "Avis!123",
    "port": "5432"
}

# Query to fetch the report with names instead of IDs
query = """
SELECT * 
FROM nr_conn_cdr 
WHERE recordentrydate >= '2020-03-01' 
AND recordentrydate <= '2020-03-31';
"""
try:
    # Connect to PostgreSQL and fetch data
    conn = psycopg2.connect(**db_config)
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Save to a CSV file in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Create the dynamic filename with current date and time
    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = f"/tmp/cdr_report_202003{current_time}.csv"

    # Save the CSV file locally in /tmp/
    with open(file_path, 'w') as f:
        f.write(csv_buffer.getvalue())

    print(f"Report saved locally as {file_path}")

except Exception as e:
    print(f"Error: {e}")
