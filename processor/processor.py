import os
import json
import faust
import joblib
import pandas as pd

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/model/model.pkl")

app = faust.App(
    "temperature-predictor",
    broker=f"kafka://{KAFKA_BROKER}",
    value_serializer="json",
)

raw_data_topic = app.topic("raw-data", value_type=bytes)
predictions_topic = app.topic("predictions", value_type=bytes)

model = None
temp_buffer = []

FEATURE_ORDER = [
    "temp_c", "dew_point_c", "rel_hum_pct",
    "wind_spd_kmh", "stn_press_kpa",
    "temp_lag_1", "temp_lag_2", "temp_lag_3",
]


@app.task
async def on_started():
    global model
    print(f"Loading model from {MODEL_PATH}...")
    model = joblib.load(MODEL_PATH)
    print("Model loaded successfully.")


@app.agent(raw_data_topic)
async def predict(stream):
    async for event in stream:
        if isinstance(event, bytes):
            data = json.loads(event)
        else:
            data = event

        temp_c = data.get("temp_c")
        if temp_c is None:
            continue

        temp_buffer.append(temp_c)

        if len(temp_buffer) >= 4:
            lag_1 = temp_buffer[-2]
            lag_2 = temp_buffer[-3]
            lag_3 = temp_buffer[-4]
        else:
            lag_1 = temp_c
            lag_2 = temp_c
            lag_3 = temp_c

        features = {
            "temp_c": temp_c,
            "dew_point_c": data.get("dew_point_c") or 0.0,
            "rel_hum_pct": data.get("rel_hum_pct") or 0.0,
            "wind_spd_kmh": data.get("wind_spd_kmh") or 0.0,
            "stn_press_kpa": data.get("stn_press_kpa") or 101.3,
            "temp_lag_1": lag_1,
            "temp_lag_2": lag_2,
            "temp_lag_3": lag_3,
        }

        feature_df = pd.DataFrame([features], columns=FEATURE_ORDER)
        predicted = float(model.predict(feature_df)[0])

        result = {
            "timestamp": data.get("timestamp", ""),
            "current_temp_c": temp_c,
            "predicted_next_temp_c": round(predicted, 2),
            "features_used": features,
        }

        await predictions_topic.send(value=json.dumps(result).encode("utf-8"))

        delta = predicted - temp_c
        sign = "+" if delta >= 0 else ""
        print(
            f"[{data.get('timestamp')}] "
            f"Current: {temp_c}°C → Predicted: {predicted:.1f}°C "
            f"({sign}{delta:.1f}°C)"
        )


if __name__ == "__main__":
    app.main()
