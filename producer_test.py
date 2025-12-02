from kafka import KafkaProducer
import json
import time
import random
import datetime
import numpy as np

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: x.encode('utf-8')
)

order_counter = 1  # Move this outside the function

def send_order():
    global order_counter  # Access the global counter
    try:
        order = str(order_counter)  # Convert to string since you're encoding as utf-8
        
        # Send to Kafka topic
        future = producer.send('producer_test', order)
        
        # Wait for confirmation
        result = future.get(timeout=10)
        
        print(f"Order {order} sent successfully!")
        print(f"Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset}")
        order_counter += 1  # Increment the counter
        
    except Exception as e:
        print(f"Error sending order: {e}")

if __name__ == "__main__":
    print("Starting Kafka Producer")
    
    try:
        while True:
            send_order()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
