# Query to fetch the report
import psycopg2
import pandas as pd
from io import StringIO
import datetime

# PostgreSQL connection details
db_config = {
    "host": "192.168.160.33",
    "database": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "port": "5433"
}

# Query to fetch the report with names instead of IDs
query = """
SELECT 
    recordentrydate, 
    camp.name AS campaign,
    u.name AS agent,
    '' AS agentfullname,
    s.name AS skill,
    callstartdate, 
    calltype, 
    accountcode, 
    q.name AS qname,
    l.name AS listname,
    uniqueid, 
    phonenumber AS phone_number, 
    callduration AS call_duration, 
    talkduration AS talk_duration, 
    holdduration AS hold_duration, 
    acwduration AS acw_duration, 
    queueduration AS queue_duration, 
    setupduration AS setup_duration, 
    0 AS preview_duration, 
    agentringduration AS agentring_duration, 
    tg.name AS trunkgroup,
    dnis, 
    '' AS dnis_name,
    callstatus, 
    disconnby AS disconnectedby, 
    dispoid AS disposition_type,
    d.name AS disposition
FROM cr_conn_cdr c
LEFT JOIN ct_campaign camp ON c.campid = camp.id
LEFT JOIN ct_user u ON c.agentid = u.id
LEFT JOIN ct_skill s ON c.skillid = s.id
LEFT JOIN ct_campaign_queue q ON c.qid = q.id
LEFT JOIN ct_servertrunkgroups tg ON c.trunkid = tg.id
LEFT JOIN ct_dispositions d ON c.dispoid = d.id
LEFT JOIN ct_list l ON c.listid = l.listid
WHERE recordentrydate >= CURRENT_DATE - INTERVAL '1 DAY';
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
    file_path = f"/tmp/cdr_report_{current_time}.csv"

    # Save the CSV file locally in /tmp/
    with open(file_path, 'w') as f:
        f.write(csv_buffer.getvalue())

    print(f"Report saved locally as {file_path}")

except Exception as e:
    print(f"Error: {e}")




