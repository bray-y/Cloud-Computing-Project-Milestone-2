# consumer_labels.py

import mysql.connector
import json
import time

db = mysql.connector.connect(
    host="34.130.128.40",
    user="usr",
    password="sofe4630u",
    database="Readings"
)
cursor = db.cursor(dictionary=True)

print("Consumer reading stored data...\n")

while True:
    cursor.execute(
        "SELECT * FROM labels WHERE processed = FALSE LIMIT 5"
    )
    rows = cursor.fetchall()

    if not rows:
        time.sleep(5)
        continue

    for row in rows:
        record = json.loads(row["data"])

        print("Processing stored record:")
        for key, value in record.items():
            print(f"  {key}: {value}")
        print("-" * 30)

        cursor.execute(
            "UPDATE labels SET processed = TRUE WHERE ID = %s",
            (row["ID"],)
        )
        db.commit()

    time.sleep(2)
