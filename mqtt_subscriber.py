import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
import time
import os
from datetime import datetime
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# =========================
# HEALTHCHECK SERVER (RENDER FIX)
# =========================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def start_health_server():
    # Render expects the service to bind to PORT (default 10000)
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"Healthcheck server running on port {port}")
    server.serve_forever()

# Start health server in background
threading.Thread(target=start_health_server, daemon=True).start()


# =========================
# MQTT SETTINGS
# =========================

USERNAME = os.getenv("MQTT_USER")
PASSWORD = os.getenv("MQTT_PASS")
HOST = "automaatio.cloud.shiftr.io"
PORT = 1883
TOPIC = "automaatio"

# =========================
# MONGODB SETTINGS
# =========================

MONGO_URI = os.getenv("MONGO_URI")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["person_counter"]
collection = db["counts"]

# =========================
# MQTT CALLBACKS
# =========================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(TOPIC)
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        print("Received:", payload)

        data = json.loads(payload)

        # Add timestamp if missing
        if "timestamp" not in data:
            data["timestamp"] = datetime.utcnow()

        collection.insert_one(data)
        print("Saved to MongoDB")

    except Exception as e:
        print("Error processing message:", e)

# =========================
# MQTT CLIENT SETUP
# =========================

client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)

client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT, keepalive=60)
client.loop_start()

# =========================
# KEEP SCRIPT RUNNING
# =========================

try:
    print("MQTT subscriber running...")
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("Disconnecting...")
    client.loop_stop()
    client.disconnect()
