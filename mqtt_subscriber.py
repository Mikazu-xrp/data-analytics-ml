import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
import time
import os
from datetime import datetime, UTC
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
    port = int(os.getenv("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"[Healthcheck] Running on port {port}")
    server.serve_forever()

threading.Thread(target=start_health_server, daemon=True).start()


# =========================
# MQTT SETTINGS
# =========================

USERNAME = os.getenv("MQTT_USER")
PASSWORD = os.getenv("MQTT_PASS")
HOST = "automaatio.cloud.shiftr.io"
PORT = 1883
TOPIC = "automaatio"

print("[MQTT] USERNAME:", USERNAME)
print("[MQTT] PASSWORD loaded:", PASSWORD is not None)


# =========================
# MONGODB SETTINGS
# =========================

MONGO_URI = os.getenv("MONGO_URI")
print("[MongoDB] URI:", MONGO_URI)

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["person_counter"]
    collection = db["counts"]
    print("[MongoDB] Connected successfully")
except Exception as e:
    print("[MongoDB] Connection error:", e)


# =========================
# MQTT CALLBACKS
# =========================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] Connected successfully")
        client.subscribe(TOPIC)
        print(f"[MQTT] Subscribed to topic: {TOPIC}")
    else:
        print(f"[MQTT] Connection failed with code {rc}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print("[MQTT] Raw payload:", payload)

    try:
        data = json.loads(payload)
    except Exception as e:
        print("[ERROR] JSON decode failed:", e)
        return

    try:
        if "timestamp" not in data:
            data["timestamp"] = datetime.now(UTC)

        result = collection.insert_one(data)
        print("[MongoDB] Inserted document ID:", result.inserted_id)

    except Exception as e:
        print("[MongoDB ERROR] Insert failed:", e)


# =========================
# MQTT CLIENT SETUP
# =========================

client = mqtt.Client()
client.username_pw_set(USERNAME, PASSWORD)

client.on_connect = on_connect
client.on_message = on_message

print("[MQTT] Connecting to broker...")
client.connect(HOST, PORT, keepalive=60)
client.loop_start()


# =========================
# KEEP SCRIPT RUNNING
# =========================

print("[System] MQTT subscriber running...")

try:
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("[System] Disconnecting...")
    client.loop_stop()
    client.disconnect()
