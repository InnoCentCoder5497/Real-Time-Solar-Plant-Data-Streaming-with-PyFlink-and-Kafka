# Real-Time Solar Plant Data Streaming with PyFlink and Kafka
## 📘 Overview

This side project simulates real-time streaming of solar plant data using Python, Apache Kafka, and Apache Flink. The aim is to build an end-to-end streaming pipeline that mimics telemetry from solar power plants, with generator and environmental data ingested, processed, and made ready for downstream analytics or dashboards (e.g., Grafana).

## 🛠️ Components
- Python Simulation Scripts: Each script instance acts as a simulated solar plant. These producers emit two types of data:
    - Generator Metrics
    - Weather Metrics

- Apache Kafka (Dockerized): Acts as the message broker. Two Kafka topics are used:
    - generator-topic
    - weather-topic

- Apache Flink with PyFlink (Dockerized): A single PyFlink job consumes from both Kafka topics, performs basic transformations and enrichments, and writes to stdout or a downstream sink (e.g., file, database, or dashboard integration).

## Architecture
![Image](imgs/architecture.jpg)

## 🧪 Use Cases
- Test Flink data processing patterns like joins, windowing, or alerts.
- Emulate real-world streaming workloads for learning or demo purposes.
- Feed a Grafana dashboard using processed metrics for live monitoring.

## Steps
- **Download flink connectors**

    Download flink connectors for Kafka to consume data from Kafka topics. Use the below code to get connectors:
    ```bash
    mkdir -p flink-connectors
    cd flink-connectors
    curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.0.0-1.17/flink-connector-kafka-3.0.0-1.17.jar
    curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar
    ```
- **Download data and rename for plant id (optional)**

    The dataset is taken from publicaly available Kaggle Datasets. This data has been gathered at two solar power plants in India over a 34 day period. It has two pairs of files - each pair has one power generation dataset and one sensor readings dataset. The power generation datasets are gathered at the inverter level - each inverter has multiple lines of solar panels attached to it. The sensor data is gathered at a plant level - single array of sensors optimally placed at the plant.
    
    [Solar Power Generation Data](https://www.kaggle.com/datasets/anikannal/solar-power-generation-data)

- **Start all services in Docker**

    Run the following to start-up all services in docker
    ```bash
    docker-compose up
    ```

- **Topic creation on kafka**

    Create Kafka-topics for generator and weather data streams
    - Generator Topic

        ```bash
        docker exec kafka ./opt/bitnami/kafka/bin/kafka-topics.sh --create --topic generator-topic --bootstrap-server localhost:9092
        ```

    - Weather Topic

        ```bash
        docker exec kafka ./opt/bitnami/kafka/bin/kafka-topics.sh --create --topic weather-topic --bootstrap-server localhost:9092
        ```

- **Start producer job**

    Start the producer job
    ```bash
    poetry run start-producer --plant_id 4135001
    ```

- **Start consumer job**

    Submit the Consumer Job
    ```bash
    docker exec jobmanager flink run --python /opt/flink/jobs/kafka-reader.py
    ```