import pyodbc
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import time
import os
import json

# Load configuration from config.json
CONFIG_FILE = 'config.json'
with open(CONFIG_FILE, 'r') as config_file:
    config = json.load(config_file)

# MSSQL Connection Details (Using Windows Authentication)
MSSQL_CONN_STR = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={config['mssql']['server']};"
    f"DATABASE={config['mssql']['database']};"
    f"Trusted_Connection=yes;"
)

# InfluxDB Connection Details
INFLUXDB_URL = config['influxdb']['url']
INFLUXDB_TOKEN = config['influxdb']['token']
INFLUXDB_ORG = config['influxdb']['org']
INFLUXDB_BUCKET = config['influxdb']['bucket']

# Query to Fetch New Data from MSSQL
SQL_QUERY = """
SELECT id, application_id, peer_id, protocol, source_id, targed_id, slot_id, is_group,
       avg_rssi, duration, frame_count, protocol_subtype, source_type, site_id,
       CONVERT(varchar, start, 126) AS start,
       CONVERT(varchar, finish, 126) AS finish
FROM dbo.Messages
WHERE id > ?
"""

LAST_ID_FILE = "last_id.json"

def load_last_id():
    if os.path.exists(LAST_ID_FILE):
        with open(LAST_ID_FILE, 'r') as file:
            return json.load(file).get('last_id', 0)
    return 0

def save_last_id(last_id):
    with open(LAST_ID_FILE, 'w') as file:
        json.dump({'last_id': last_id}, file)

def fetch_new_data_from_mssql(last_id):
    connection = pyodbc.connect(MSSQL_CONN_STR)
    cursor = connection.cursor()
    cursor.execute(SQL_QUERY, (last_id,))
    
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    return [dict(zip(columns, row)) for row in rows]

def write_to_influxdb(data):
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    for row in data:
        point = Point("messages")
        point.time(datetime.fromisoformat(row["start"]))
        point.tag("peer_id", row["peer_id"])
        point.tag("ts", row["slot_id"] + 1)
        point.tag("source_id", row["source_id"])
        point.tag("target_id", row["targed_id"])
        point.field("peer_id", row["peer_id"])
        point.field("ts", row["slot_id"] + 1)
        point.field("source_id", row["source_id"])
        point.field("target_id", row["targed_id"])
        point.field("duration", row["duration"] / 10000)
        point.field("frame_count", row["frame_count"])
        point.field("avg_rssi", row["avg_rssi"])
        point.field("protocol", row["protocol"])
        point.field("protocol_subtype", row["protocol_subtype"])

        if row["protocol"] == 1:
            if row["protocol_subtype"] == 101:
                point.field("msg_type", "Registration - Online")
                point.tag("msg_type", "Registration - Online")
            elif row["protocol_subtype"] == 102:
                point.field("msg_type", "Registration - Ack Success")
                point.tag("msg_type", "Registration - Ack Success")
            elif row["protocol_subtype"] == 105:
                point.field("msg_type", "Registration - Query")
                point.tag("msg_type", "Registration - Query")
        elif row["protocol"] == 4:
            if row["protocol_subtype"] == 301:
                point.field("msg_type", "Text Message")
                point.tag("msg_type", "Text Message")
        elif row["protocol"] == 16:
            if row["protocol_subtype"] == 1:
                point.field("msg_type", "System - Delivery Confirmed")
                point.tag("msg_type", "System - Delivery Confirmed")
            elif row["protocol_subtype"] == 613:
                point.field("msg_type", "System - Start of Interference")
                point.tag("msg_type", "System - Start of Interference")
            elif row["protocol_subtype"] == 614:
                point.field("msg_type", "System - End of Interference")
                point.tag("msg_type", "System - End of Interference")
        elif row["protocol"] == 32:
            if row["protocol_subtype"] == 702:
                point.field("msg_type", "Voice - Group Call")
                point.tag("msg_type", "Voice - Group Call")
            elif row["protocol_subtype"] == 703:
                point.field("msg_type", "Voice - Private Call")
                point.tag("msg_type", "Voice - Private Call")


        point.field("start", row["start"])
        point.field("finish", row["finish"])

        print(point)
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    
    client.close()

def main():
    last_id = load_last_id()
    while True:
        print("Fetching new data from MSSQL...")
        data = fetch_new_data_from_mssql(last_id)
        if data:
            print(f"Fetched {len(data)} new rows.")
            write_to_influxdb(data)
            last_id = max(row['id'] for row in data)
            save_last_id(last_id)
            print("Data successfully written to InfluxDB and last_id updated.")
        else:
            print("No new data found.")
        
        time.sleep(10)  # Wait for 10 seconds before checking for new data

if __name__ == '__main__':
    main()
