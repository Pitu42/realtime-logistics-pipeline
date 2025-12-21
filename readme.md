## Real-time Logistics Pipeline

## Project Overview
Project for real-time data streaming and analytics.

## Architecture
- Kafka for message streaming
- MongoDB historical data and transaction
- Apache Beam for stream data processing
- SQL data warehouse for aggregation and analysis



## Development Progress
# Phase 1 - Kafka
- [X] Install Kafka docker image
- [X] Create Kafka producer
- [X] Create draft Kafka Consumer
# Phase 2 - NoSQL db
- [X] Install MongoDB docker image 
- [X] Kafka consumer: MongoDB
# Phase 3 - Streaming aggregation
- [X] Kafka consumer: Apache spark
    - [X] Create draft Apache spark stream aggregation
    - [X] Create draft item aggregation with pandas df
# Phase 4 - SQL/NoSQL whs
- [X] Decide if item data will be stored in SQL or NoSQL db (consistency is the most important, so maybe sql)
    - [X] if SQL: install postgressql docker image
- [X] Generate item data (price, lenght, width, height, weight)
# Phase 5 - Carrier API
- [X] Create carrier api:
    - [X] Post track and trace
    - [X] Post shipping events
# Phase 7 - OLAP
- [] Create a sql table based on mongodb for analytics
