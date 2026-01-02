from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import datetime
import requests

url = 'http://carrier-api:8000/create-tracking'

# MongoDB connection
mongo_client = MongoClient('mongodb://admin:password@mongodb:27017/')
db = mongo_client['orders_db']
orders_collection = db['purchase_orders']

# Kafka consumer
consumer = KafkaConsumer(
    'parcel-data',
    bootstrap_servers=['kafka:29092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='api-post'
)

print("Kafka sending to api started...")

try:
    for message in consumer:
        try:
            order = message.value

            response = requests.post(url, json=order)

            if response.status_code == 200:
                print(f'json: {order}')
                print(f'saved: {response.content}')
                response_data = response.json()
                order_id = response_data["order_id"]
                tracking_number = response_data["tracking_number"]
                result = orders_collection.update_one(
                    {"order_id":order_id},
                    {
                        "$set": {
                            "tracking_number":tracking_number
                            }
                        }
                    )
                if result.matched_count > 0:
                    print(f"Updated order {order_id} with tracking {tracking_number}")
                else:
                    print(f"No order found with order_id: {order_id}")

        except Exception as e:
            print(f"Error: {e}")
            
except KeyboardInterrupt:
    print("Stopping MongoDB consumer")

finally:
    consumer.close()
    mongo_client.close()
