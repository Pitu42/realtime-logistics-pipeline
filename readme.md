## Real-time Logistics Pipeline

![pipeline_diagram](https://github.com/user-attachments/assets/f3e8fc99-8afe-4c34-8393-f1ba569def78)

Real-time logistics pipeline that ingests synthetic purchase orders, aggregates item dimensions/weight/cost into parcels using a streaming job, and forwards the aggregated parcel data to carrier APIs and analytics data warehouse. Built with Kafka, Apache Spark, MongoDB, PostgreSQL, and containerized microservices using Docker.

### Project overview 

This repository demonstrates an end-to-end, real-time streaming pipeline for last-mile logistics. Purchase orders are produced continuously, consumed by a streaming aggregator (Spark) which joins item master data from PostgreSQL to compute parcel dimensions, weight and total cost, and then forwards parcel requests using api to a carrier. Parcel data is stored in Postgresql for reporting and monitoring.

### Architecture

Kafka & Zookeeper: message broker for events between microservices.
purchase-producer: Python script that produces continuous online purchases orders json to Kafka.
spark: Spark streaming that consumes purchase_orders, joins with PostgreSQL item master, computes aggregated parcel info, and writes parcel data to Kafka.
api-tracking-request: Python script that sends post requests to carrier api.
carrier-tracker: FastAPI service that receives parcel requests and responds with the tracking number, saves data to MongoDB.
carrier-status: Python script that post parcel status updates.
status-receiver-api: FastAPI service that receives parcel status updates and saves them to MongoDB.
mongodb-inserter: Python script that consumes purchase_orders and inserts them into MongoDB.
postgres-inserter: Python script that generates synthetic item master data and inserts it into a PostgreSQL table, and batch insert MongoDB data into PostgreSQL analytic table.
MongoDB: stores purchase orders, parcel data, carrier responses and shipping events.
PostgreSQL: stores item master and analytics tables.

### Prerequisites

Docker and docker-compose installed.

### Installation

1. Clone the repository
```bash
git clone https://github.com/Pitu42/realtime-logistics-pipeline
cd realtime-logistics-pipeline
```
2. Start the pipeline:
```bash
sudo docker-compose up
```
3. View services logs:
```bash
sudo docker container logs <service-name>
```
4. Stop and remove containers:
```bash
sudo docker-compose down
```

### Available services

- zookeeper
- kafka
- mongodb
- mongo_inserter
- postgres
- spark
- producer
- api-tracking-request
- carrier-api
- status_receiver_api
- carrier_status
- postgres_inserter

### Example JSON payloads

Purchase order (producer output) `{ "order_id": "1", "timestamp": "2025-12-01T12:00:00Z", "zipcode": "1234AA", "items": [ {"item_id": 1, "quantity": 2}, {"item_id": 2, "quantity": 1} ] }`

Aggregated parcel (spark output) `{ "order_id": "1", "zipcode":"1234AA", "total_dimensions": 123, "total_weight": 40, "timestamp": "2025-12-01T12:00:05Z"}`

Carrier API response (stored in MongoDB) `{"order_id": "1", "tracking_number": "JVLG1234567890"}`

Carrier status response (stored in MongoDB) `{"order_id": "1", "tracking_number": "JVLG1234567890", "status": "delivered", "status_time": "2025-12-01T12:00:15Z" }`

### Pipeline outputs
The pipeline processes purchase orders in real-time and stores the data in two databases:

MongoDB Collections:
- orders_db.purchase_orders - Raw purchase order data from customers
- parcel_db.purchase_orders - Aggregated parcel data
- parcel_db.parcel_orders - Parcel data used by carrier
```bash
# exec commands inside MongoDB service
docker container exec -it mongodb mongosh
# switch to database
use orders_db
# view all entries in the collection
db.purchase_orders.find().pretty()
```
PostgreSQL Tables:
- item_data - Master item data
- order_analytics - Analytics data for operations.
```bash
# exec commands inside PostgreSQL service
docker container exec -it postgres psql -U postgres 
# View all contents of your table
SELECT * FROM order_analytics;
```
