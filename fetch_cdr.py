# Query to fetch the report
import psycopg2
import pandas as pd
from io import StringIO
import datetime

# PostgreSQL connection details
db_config = {
    "host": "192.168.160.229",
    "database": "verve",
    "user": "postgres",
    "password": "Avis!123",
    "port": "5433"
}

# Query to fetch the report with names instead of IDs
query = """
SELECT
    camp.name AS campaign,
    u.name AS agent,
    s.name AS skill,
    callstartdate,
    recordentrydate AS callenddate,
    calltype,
    accountcode,
    q.name AS qname,
    l.name AS listname,
    uniqueid,
    phonenumber AS phone_number,
    TO_CHAR(make_interval(secs => callduration), 'HH24:MI:SS') AS call_duration,
    TO_CHAR(make_interval(secs => talkduration), 'HH24:MI:SS') AS talk_duration,
    TO_CHAR(make_interval(secs => holdduration), 'HH24:MI:SS') AS hold_duration,
    TO_CHAR(make_interval(secs => acwduration), 'HH24:MI:SS') AS acw_duration,
    TO_CHAR(make_interval(secs => queueduration), 'HH24:MI:SS') AS queue_duration,
    TO_CHAR(make_interval(secs => setupduration), 'HH24:MI:SS') AS setup_duration,
    TO_CHAR(make_interval(secs => agentringduration), 'HH24:MI:SS') AS agentring_duration,
    tg.name AS trunkgroup,
    dnis,
    callstatus,
    disconnby AS disconnectedby,
    COALESCE(d.type, c.dispoid::TEXT) AS disposition_type,
    COALESCE(d.name, c.dispoid::TEXT) AS disposition,
    hangupcause
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




