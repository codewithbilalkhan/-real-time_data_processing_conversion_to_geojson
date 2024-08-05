import paho.mqtt.client as mqtt
import json
import time

# MQTT Client setup
client = mqtt.Client()
client.connect("broker.hivemq.com", 1883, 60)

def publish_message():
    msg = [{
        "deviceno": "123456",
        "speed": 50,
        "latitude": 17.789,
        "longitude": 78.654
    }]
    msg_string = json.dumps(msg)
    print(msg_string)
    client.publish('asset', msg_string)

while True:
    publish_message()
    time.sleep(5)
