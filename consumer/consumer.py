import json
import time
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = "predictions"


def create_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="prediction-printer",
            )
            print(f"Connected to Kafka at {KAFKA_BROKER}")
            print(f"Listening on topic '{TOPIC}'...\n")
            return consumer
        except NoBrokersAvailable:
            print("Waiting for Kafka broker...")
            time.sleep(2)


def main():
    consumer = create_consumer()

    for message in consumer:
        data = message.value
        timestamp = data.get("timestamp", "?")
        current = data.get("current_temp_c", "?")
        predicted = data.get("predicted_next_temp_c", "?")

        if isinstance(current, (int, float)) and isinstance(predicted, (int, float)):
            delta = predicted - current
            sign = "+" if delta >= 0 else ""
            print(
                f"[{timestamp}] "
                f"Current: {current}°C → "
                f"Predicted next hour: {predicted}°C "
                f"(delta: {sign}{delta:.1f}°C)"
            )
        else:
            print(f"[{timestamp}] Current: {current}°C → Predicted: {predicted}°C")


if __name__ == "__main__":
    main()
