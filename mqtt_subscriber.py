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
# MQTT SETTINGS (MATCH TEACHER)
# =========================

HOST = "automaatio.cloud.shiftr.io"
PORT = 1883
TOPIC = "automaatio/#"   # IMPORTANT: subscribe to ALL subtopics

USERNAME = "automaatio"
PASSWORD = "Z0od2PZF65jbtcXu"

print("[MQTT] Using username:", USERNAME)


# =========================
# MONGODB SETTINGS
# =========================

MONGO_URI = os.getenv("MONGO_URI")
print("[MongoDB] URI:", MONGO_URI)

mongo_client = MongoClient(MONGO_URI)


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

    # Try to parse JSON
    try:
        obj = json.loads(payload)
    except Exception as e:
        print("[ERROR] JSON decode failed:", e)
        return

    # Expected fields from ESP32:
    # db_name, coll_name, id, person count, DateTime

    dbname = obj.get("db_name")
    collname = obj.get("coll_name")

    if not dbname or not collname:
        print("[ERROR] Missing db_name or coll_name in message")
        return

    # Add timestamp like teacher's code
    obj["DateTime"] = datetime.now().strftime("%d %b %Y %H:%M:%S")

    # Insert into correct DB + collection
    db = mongo_client[dbname]
    coll = db[collname]

    try:
        result = coll.insert_one(obj)
        print(f"[MongoDB] Inserted into {dbname}.{collname}, id: {result.inserted_id}")
    except Exception as e:
        print("[MongoDB ERROR] Insert failed:", e)


# =========================
# MQTT CLIENT SETUP
# =========================

client = mqtt.Client(client_id="python-subscriber", clean_session=True)
client.username_pw_set(USERNAME, PASSWORD)

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
