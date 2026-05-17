import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

DATA_PATH = os.environ.get("DATA_PATH", "/app/data/oshawa_weather_2024.csv")
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "raw-data"
SEND_INTERVAL = float(os.environ.get("SEND_INTERVAL", "1.0"))

FIELD_MAP = {
    "Temp (°C)": "temp_c",
    "Dew Point Temp (°C)": "dew_point_c",
    "Rel Hum (%)": "rel_hum_pct",
    "Wind Spd (km/h)": "wind_spd_kmh",
    "Wind Dir (10s deg)": "wind_dir_deg",
    "Stn Press (kPa)": "stn_press_kpa",
}


def create_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            print("Waiting for Kafka broker...")
            time.sleep(2)


def parse_row(row: dict) -> dict | None:
    timestamp = row.get("Date/Time (LST)", "").strip()
    if not timestamp:
        return None

    message = {"timestamp": timestamp}
    for csv_col, json_key in FIELD_MAP.items():
        val = (row.get(csv_col) or "").strip()
        if csv_col == "Temp (°C)" and val == "":
            return None
        try:
            message[json_key] = float(val) if val else None
        except ValueError:
            message[json_key] = None

    return message


def main():
    producer = create_producer()
    sent = 0

    with open(DATA_PATH, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = parse_row(row)
            if message is None:
                continue

            producer.send(TOPIC, value=message)
            sent += 1
            print(f"[{sent}] Sent: {message['timestamp']} | Temp: {message['temp_c']}°C")
            time.sleep(SEND_INTERVAL)

    producer.flush()
    producer.close()
    print(f"\nDone. Sent {sent} messages to '{TOPIC}'")


if __name__ == "__main__":
    main()
