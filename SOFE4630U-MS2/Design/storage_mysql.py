from google.cloud import pubsub_v1
import json
import glob
import os
import mysql.connector
from datetime import datetime, UTC
from queue import Queue
import threading
import time

# ---------------------------
# Load GCP credentials
# ---------------------------
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# ---------------------------
# Pub/Sub configuration
# ---------------------------
project_id = "integral-vim-486413-k2"
topic_name = "smartMeterReadings"
sub_name = "smartMeterReadings-sub"

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(project_id, topic_name)
subscription_path = subscriber.subscription_path(project_id, sub_name)

# Create subscription if it does not exist
try:
    subscriber.create_subscription(
        name=subscription_path,
        topic=topic_path
    )
except Exception:
    pass  # already exists

# ---------------------------
# Thread-safe queue
# ---------------------------
message_queue = Queue()

print("Listening to Pub/Sub and storing messages in MySQL...\n")

# ---------------------------
# Pub/Sub callback (NO DB)
# ---------------------------
def callback(message):
    try:
        record = json.loads(message.data.decode("utf-8"))
        message_queue.put(record)
        message.ack()
    except Exception as e:
        print("Callback error:", e)
        message.nack()

# ---------------------------
# Single DB worker thread
# ---------------------------
def db_worker():
    db = mysql.connector.connect(
        host="34.130.128.40",
        user="usr",
        password="sofe4630u",
        database="Readings",
        autocommit=True
    )
    cursor = db.cursor()

    while True:
        record = message_queue.get()
        try:
            cursor.execute(
                "INSERT INTO labels (data, received_at) VALUES (%s, %s)",
                (json.dumps(record), datetime.now(UTC))
            )
            print("Stored message in MySQL")
        except Exception as e:
            print("DB error:", e)
        finally:
            message_queue.task_done()

# Start DB worker
threading.Thread(target=db_worker, daemon=True).start()

# Start subscriber
subscriber.subscribe(subscription_path, callback=callback)

# Keep main thread alive
while True:
    time.sleep(5)
