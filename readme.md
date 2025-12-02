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
    - [ ] Create draft item aggregation with pandas df
# Phase 4 - SQL/NoSQL whs
- [ ] Decide if item data will be stored in SQL or NoSQL db (consistency is the most important, so maybe sql)
    - [ ] if SQL: install postgressql docker image
- [ ] Generate item data (price, lenght, width, height, weight)
# Phase 5 - Carrier API
- [ ] Create carrier api:
    - [ ] Post track and trace
    - [ ] Post shipping events
# Phase 6 - Carrier Integration
- [ ] New Producer: Apache beam -> Item_data
- [ ] New Consumer: Carrier Api -> Item_data
- [ ] New Producer: Carrier Api -> tracking_code
- [ ] New Consumer: MongoDB -> tracking_code (new db link order_id to tracking_code)
- [ ] New Producer: Carrier Api -> event code
# Phase 7 - OLAP
- [ ] Beam batch processing to sql whs for olap
