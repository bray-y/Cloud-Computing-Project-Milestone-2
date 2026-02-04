from google.cloud import pubsub_v1
import csv
import json
import glob
import os

# Load service account credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set project and topic
project_id = "integral-vim-486413-k2"
topic_name = "smartMeterReadings"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing records to {topic_path}")

# Read CSV and publish each row
with open("Labels.csv", newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        # Convert row (dict) to JSON
        message_json = json.dumps(row)
        message_bytes = message_json.encode("utf-8")

        print("Publishing:", row)
        future = publisher.publish(topic_path, message_bytes)
        future.result()

print("All records published.")
