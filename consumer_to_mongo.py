from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import datetime

# MongoDB connection
mongo_client = MongoClient('mongodb://admin:password@localhost:27017/')
db = mongo_client['orders_db']
orders_collection = db['purchase_orders']

# Kafka consumer
consumer = KafkaConsumer(
    'purchase-orders',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='mongodb-consumer'
)

print("MongoDB Consumer started...")
print("Listening for orders to save to MongoDB...")

try:
    for message in consumer:
        try:
            order = message.value
            
            # Insert into MongoDB
            result = orders_collection.insert_one(order)
            
            print(f"Saved order: {order['order_id']}, item lines: {len(order['item_id'])}"
            
        except Exception as e:
            print(f"Insert error: {e}")
            
except KeyboardInterrupt:
    print("Stopping MongoDB consumer")
finally:
    consumer.close()
    mongo_client.close()

