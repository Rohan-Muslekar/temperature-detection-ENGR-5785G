# Real-Time Temperature Prediction — ENGR 5785G Assignment 1

A real-time streaming application using Apache Kafka that replays hourly Oshawa/Toronto weather data, predicts next-hour temperature using a pre-trained Random Forest model via Faust, and displays predictions live.

## Dataset

**Source:** Environment and Climate Change Canada — [climate.weather.gc.ca](https://climate.weather.gc.ca)  
**Station:** Toronto City Centre (Station ID 48549, Climate ID 6158359)  
**Period:** 2024 (hourly observations, 8784 rows)  
**Features used:** Temperature, Dew Point, Relative Humidity, Wind Speed, Station Pressure, plus 3 lagged temperature values (t-1, t-2, t-3)

## Streams Library

**Faust** (`faust-streaming`) — the Python equivalent of Kafka Streams. The processor uses `@app.agent()` to consume from the `raw-data` topic, run inference, and produce to the `predictions` topic.

## ML Model

- **Algorithm:** Random Forest Regressor (scikit-learn, 100 trees)
- **Task:** Predict next-hour temperature given current weather conditions
- **Training:** Offline on full 2024 dataset (80/20 chronological split)

### Performance (Test Set)

| Metric | Value |
|--------|-------|
| MAE | 0.48 °C |
| RMSE | 0.68 °C |
| R² | 0.9875 |

## Architecture

```
Producer (CSV → JSON) → [raw-data topic] → Faust Processor (ML) → [predictions topic] → Consumer (print)
```

All services run in Docker containers orchestrated by Docker Compose with a single KRaft Kafka broker (no Zookeeper).

## Setup & Run

### Prerequisites

- Docker and Docker Compose

### Quick Start

```bash
# 1. Train the model (one-time, if model.pkl not present)
pip install pandas scikit-learn joblib
python model/train.py

# 2. Start the full pipeline
docker compose up --build
```

### Run Individual Components

```bash
# Start only Kafka
docker compose up kafka

# Start producer
docker compose up producer

# Start processor
docker compose up processor

# Start consumer
docker compose up consumer
```

### Stop

```bash
docker compose down
```

## Project Structure

```
├── docker-compose.yml          # Orchestrates all services
├── producer/
│   ├── Dockerfile
│   ├── producer.py             # Reads CSV, publishes to raw-data at 1 msg/sec
│   └── requirements.txt
├── processor/
│   ├── Dockerfile
│   ├── processor.py            # Faust agent: consume → predict → publish
│   └── requirements.txt
├── consumer/
│   ├── Dockerfile
│   ├── consumer.py             # Reads predictions, prints formatted output
│   └── requirements.txt
├── model/
│   ├── train.py                # Offline training script
│   └── model.pkl               # Trained Random Forest model
├── data/
│   └── oshawa_weather_2024.csv # Hourly weather data
└── README.md
```

## Components

| Component | Description | Kafka Topic |
|-----------|-------------|-------------|
| Producer | Replays CSV rows as JSON events at ~1/sec | → `raw-data` |
| Processor | Faust agent runs Random Forest prediction | `raw-data` → `predictions` |
| Consumer | Prints formatted prediction results | ← `predictions` |

## Video Demo

[Video Demo](https://drive.google.com/file/d/1CgbnI-u3Pr61qvG5bJdNxvU6Tu2ckMKT/view?usp=sharing)
