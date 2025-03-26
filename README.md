# SKY INSIGHT

## Overview
Sky Insight is a data pipeline designed to process and analyze real-time social media posts from the Bluesky network. The project ingests posts via WebSocket, publishes them to a Kafka topic, and performs hashtag aggregation using Apache Flink, in order to find the most used hashtags given a time window. Aggregated results are stored in a PostgreSQL database for further analysis.

## Architecture
The project consists of the following components:

1. **WebSocket Client**:
   - Connects to the Bluesky WebSocket server to receive real-time posts.
   - Parses incoming messages into JSON format.

2. **Kafka Producer**:
   - Publishes parsed posts to a Kafka topic (`bluesky-raw-posts`).
   - Filters and extracts relevant fields from the posts.

3. **Apache Flink**:
   - Consumes posts from the Kafka topic.
   - Extracts hashtags from post text and performs windowed aggregation.
   - Writes aggregated hashtag counts to a PostgreSQL database.

4. **PostgreSQL**:
   - Stores aggregated hashtag counts for further analysis and visualization.

## Workflow
1. **Data Ingestion**:
   - The `WebSocketClient` connects to the Bluesky WebSocket server and streams posts in real-time.
   - Posts are parsed and passed to the `PostProducer`.

2. **Data Publishing**:
   - The `PostProducer` extracts relevant fields from the posts and publishes them to the Kafka topic `bluesky-raw-posts`.

3. **Data Processing**:
   - Apache Flink reads posts from the Kafka topic.
   - Hashtags are extracted using a custom user-defined table function (`ParseHastag`).
   - Hashtag counts are aggregated in 1-minute tumbling windows.

4. **Data Storage**:
   - Aggregated hashtag counts are written to the PostgreSQL database.

## Components
### 1. **WebSocket Client**
   - File: [`websocket_client.py`](analysis/src/producers/websocket_client.py)
   - Handles WebSocket connection and message parsing.

### 2. **Kafka Producer**
   - File: [`post_producer.py`](analysis/src/producers/post_producer.py)
   - Publishes posts to Kafka after filtering and formatting.

### 3. **Apache Flink Job**
   - File: [`hashtag_aggregation_job.py`](analysis/src/jobs/hashtag_aggregation_job.py)
   - Defines the Flink job for hashtag extraction and aggregation.

### 4. **Dockerized Environment**
   - File: [`docker-compose.yml`](analysis/docker-compose.yml)
   - Sets up the environment with Redpanda (Kafka), Flink, PostgreSQL, and PgAdmin.

## How to Run
1. **Install Dependencies**:
   - Install Python dependencies using `pip`:
     ```sh
     pip install -r analysis/requirements.txt
     ```

2. **Start Services**:
   - Use Docker Compose to start all services:
     ```sh
     docker compose up --build --remove-orphans  -d
     ```

3. **Run the WebSocket Producer**:
   - Start the `PostProducer` to ingest and publish posts:
     ```sh
     python analysis/src/producers/post_producer.py
     ```

4. **Run the Flink Job**:
    ```sh
    docker compose exec jobmanager ./bin/flink run -py /opt/src/jobs/hashtag_aggregation_job.py --pyFiles /opt/src -d
    ```
