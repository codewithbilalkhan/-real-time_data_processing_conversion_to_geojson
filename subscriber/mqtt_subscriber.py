import paho.mqtt.client as mqtt
import duckdb
import json
import time

# Connect to DuckDB
conn = duckdb.connect('new_data.duckdb')

# Create table if not exists
conn.execute("""
CREATE TABLE IF NOT EXISTS device_data (
    deviceno VARCHAR,
    speed INTEGER,
    latitude DOUBLE,
    longitude DOUBLE
)
""")


client = mqtt.Client()

def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("asset")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        print(data)
        save_data_in_duckdb(data)
    except Exception as e:
        print("Error processing message:", e)

def save_data_in_duckdb(data):
    try:
        conn.execute("INSERT INTO device_data VALUES (?, ?, ?, ?)",
                     (data[0]['deviceno'], data[0]['speed'], data[0]['latitude'], data[0]['longitude']))
        print("Data saved to DuckDB")
    except Exception as e:
        print("Error saving data to DuckDB:", e)

client.on_connect = on_connect
client.on_message = on_message

client.connect("broker.hivemq.com", 1883, 60)
client.loop_forever()
