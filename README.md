# Real-Time Traffic Data Stream Ingestion and Processing

## 1. Development Report

### 1.1 Infrastructure and Kafka Initialization
The foundation of this streaming pipeline was established using Docker Compose to host the Kafka broker and the Kafka UI in a local environment. Once the containers were running, the messaging architecture was initialized by creating two distinct Kafka topics: `traffic-telemetry` for the incoming raw data stream and `traffic-window-results` for the processed outputs. Both topics were explicitly configured with a single partition. This single-partition setup was chosen for local testing to ensure sequential data flow and to prevent Apache Flink's event-time clock from stalling due to idle partition tracking.


### 1.2 Real-Time Data Ingestion and Simulation
The source data for this project comes from the Austin Open Data API. Since this dataset consists of static historical records, a Python producer script was developed to simulate a continuous, real-time stream. As the script reads the historical JSON payloads, it replaces the static timestamps with the live system time before publishing each record to the `traffic-telemetry` topic.

To thoroughly test the pipeline's ability to handle out-of-order events, a simulation mechanism was added within the ingestion script. The producer intentionally delays the timestamp of every 10th record by 8 seconds. This provides a practical way to observe how the downstream stream processor handles network latency and late-arriving data.


### 1.3 Flink Processor Initialization and Dashboard Setup
The stream processing layer was built using a PyFlink script running Flink SQL. The execution environment was configured to run locally while actively exposing Flink's embedded Web Dashboard on port 8082. This approach allowed for a stable connection to the local Kafka broker while fulfilling the requirement to visually monitor the job's health, task distribution, and memory usage through the Flink console.


### 1.4 Data Preprocessing and Windowed Aggregation
Directly aggregating raw data streams often leads to inaccurate results due to upstream anomalies. To prevent this, a continuous data quality filter was designed to be applied directly within the Flink SQL `WHERE` clause. This preprocessing stage drops records missing critical sensor IDs, rejects negative vehicle counts, and applies an upper-bound threshold to discard extreme numerical outliers that usually indicate hardware malfunctions.

For the core aggregation, the initial project scope suggested a 10-minute tumbling window. However, an analysis of the source dataset revealed that the raw records are already pre-aggregated into 15-minute intervals. To maintain data integrity and avoid temporal aliasing, the Flink tumbling window was adjusted to exactly 15 minutes. 

To manage the simulated network latency, a bounded-out-of-orderness watermark defined as `count_date - INTERVAL '10' SECOND` was integrated. This mechanism forces Flink to hold the 15-minute window open for an additional 10 seconds, capturing and reconciling late-arriving events before finalizing the window's calculation.


### 1.5 Final Output and Verification
Due to how a tumbling window operates, the Flink job retains processed data in memory until the global event-time clock surpasses the full 15-minute interval plus the 10-second watermark allowance. Once the producer emitted a record with a timestamp crossing this threshold, Flink successfully evaluated the window block. The pipeline then automatically flushed the calculated moving totals of vehicle counts, grouped by sensor ID, into the destination sink.


---

## 2. Repository Structure

    Q2-Real-Time-Stream-Ingestion/
    │   .env
    │   .gitignore
    │   docker-compose.yml
    │   init-kafka.sh
    │   README.md
    │
    ├───data/
    │       sample_camera_data.json
    │
    ├───flink-job/
    │       flink-sql-connector-kafka-1.17.1.jar
    │       requirements.txt
    │       traffic_processor.py
    │
    ├───tests/
    │       test_producer.py
    │
    └───producer/
            producer.py
            requirements.txt
            test_producer.py

---

## 3. Setup and Execution Guide

### Prerequisites
* **Docker & Docker Compose**
* **Python 3.8, 3.9, or 3.10** (Strictly required; PyFlink 1.17 is incompatible with Python 3.11+)
* **Java 11** (Required by Apache Flink)

### Step 1: Initialize Infrastructure
Start the Kafka broker and Kafka UI in the background:

    docker-compose down
    docker-compose up -d

Wait 15 seconds for the broker to initialize, then provision the necessary topics:

    docker exec -it kafka kafka-topics --create --topic traffic-telemetry --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
    docker exec -it kafka kafka-topics --create --topic traffic-window-results --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

### Step 2: Run Unit Tests
Before launching the pipeline, verify the producer's data casting and timestamp formatting logic:

    cd tests
    python -m unittest test_producer.py

### Step 3: Start the Flink Processor
Navigate to the Flink job directory, install dependencies, and launch the stream processor:(Run this in seperate terminal)

    cd flink-job
    py -3.10 -m venv venv
    .\venv\Scripts\activate
    pip install "setuptools<70.0.0"
    pip install -r requirements.txt
    python traffic_processor.py

    
### Step 4: Start the Data Producer
In a separate terminal, install the producer dependencies and execute the data ingestion script to begin publishing to the telemetry topic:

    cd producer
    pip install -r requirements.txt
    python producer.py

---

## 4. Monitoring and Verification

* **Flink Dashboard:** Navigate to `http://localhost:8082` to view the active Flink pipeline running in real-time.
* **Kafka UI (Raw Stream):** Navigate to `http://localhost:8080` and inspect the `traffic-telemetry` topic to observe raw incoming JSON messages.
* **Kafka UI (Processed Aggregations):** Inspect the `traffic-window-results` topic. Because the pipeline utilizes a 15-minute tumbling window with a 10-second watermark, this topic will remain empty until the global event-time clock crosses the 15-minute boundary plus the 10-second skew. Upon crossing that threshold, the aggregated JSON payloads will flush into this topic automatically.
