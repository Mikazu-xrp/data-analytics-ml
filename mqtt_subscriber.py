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
# MQTT SETTINGS (FROM TASK)
# =========================

HOST = "automaatio.cloud.shiftr.io"
PORT = 1883
TOPIC = "automaatio"
USERNAME = "automaatio"
PASSWORD = "Z0od2PZF65jbtcXu"

print("[MQTT] USERNAME:", USERNAME)


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

    try:
        data = json.loads(payload)
    except Exception as e:
        print("[ERROR] JSON decode failed:", e)
        return

    # Odotettu formaatti:
    # {
    #   "db_name": "data_ml",
    #   "coll_name": "p_count",
    #   "id": "aiot",
    #   "person count": 13,
    #   "DateTime": "13 Jan 2026 9:36:7"
    # }

    db_name = data.get("db_name", "data_ml")
    coll_name = data.get("coll_name", "p_count")

    db = mongo_client[db_name]
    collection = db[coll_name]

    # Normalisoidaan kentät
    person_count = data.get("person count")
    dt_str = data.get("DateTime")

    # Yritetään parsia DateTime, mutta ei kaadeta jos ei onnistu
    dt_parsed = None
    if dt_str:
        try:
            dt_parsed = datetime.strptime(dt_str, "%d %b %Y %H:%M:%S")
        except Exception as e:
            print("[WARN] Failed to parse DateTime, storing as string:", e)

    doc = {
        "source_id": data.get("id"),
        "person_count": person_count,
        "datetime_raw": dt_str,
        "datetime_parsed": dt_parsed,
        "ingested_at": datetime.now(UTC),
    }

    try:
        result = collection.insert_one(doc)
        print(f"[MongoDB] Inserted into {db_name}.{coll_name}, id: {result.inserted_id}")
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
