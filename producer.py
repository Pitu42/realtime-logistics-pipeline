from kafka import KafkaProducer
import json
import time
import random
import string
import datetime
import numpy as np

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'], #localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

order_id = 1

def create_zip_code():
    # 4 random int with leading zero + 2 random letters
    rand_int = str(random.randint(0, 9999)).zfill(4)
    rand_str = random.choice(string.ascii_letters).upper()
    return rand_int+rand_str+rand_str

# simulate purchase order
def create_purchase_order_json(order_id):
    num_items = random.randint(1,3)
    zipcode = create_zip_code()
    items = []

    for i in range(num_items):
        items.append({
            "item_id":random.randint(0,11),
            "quantity":random.randint(1,3)
        })
    return {
        "order_id": order_id,
        "zipcode": zipcode,
        "item_id": items,
        "timestamp": datetime.datetime.now().isoformat()
            }


# Send payload to kafka
def send_order(kafka_topic):
    try:
        #order = create_purchase_order()
        order = create_purchase_order_json(order_id)
        
        # Send to Kafka topic
        future = producer.send(kafka_topic, order)
        
        # Wait for confirmation
        result = future.get(timeout=10)
        
        print(f"Order {order['order_id']} sent successfully!")
        print(f"Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset}")
        
    except Exception as e:
        print(f"Error sending order: {e}")

if __name__ == "__main__":
    print("Starting Kafka Producer")
    
    while True:
        send_order('purchase-orders')
        #send_order('purchase-orders-histo')
        order_id += 1
        time.sleep(1)
    
    producer.close()

