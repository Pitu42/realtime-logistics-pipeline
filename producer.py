from kafka import KafkaProducer
import json
import time
import random
import datetime
import numpy as np

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

order_id = 1

# simulate purchase order
def create_purchase_order():
    order_qty = np.random.poisson(lam=1) + 1 # min order is 1
    order = {
        "order_id": order_id,
        "item_id": np.random.randint(1,101, size=order_qty).tolist(),
        "quantity": np.random.randint(1,5, size=order_qty).tolist(),
        "timestamp": datetime.datetime.now().isoformat()
    }
    
    return order

# Send payload to kafka
def send_order():
    try:
        order = create_purchase_order()
        
        # Send to Kafka topic
        future = producer.send('purchase-orders', order)
        
        # Wait for confirmation
        result = future.get(timeout=10)
        
        print(f"Order {order['order_id']} sent successfully!")
        print(f"Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset}")
        
    except Exception as e:
        print(f"Error sending order: {e}")

if __name__ == "__main__":
    print("Starting Kafka Producer")
    
    while True:
        send_order()
        order_id += 1
        time.sleep(1)
    
    producer.close()

