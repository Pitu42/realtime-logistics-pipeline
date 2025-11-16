from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'purchase-orders',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Listening for messages")

try:
    for message in consumer:
        order = message.value
        print(f"Received order: {order['order_id']}")
        print(f"Items: {order['item_id']}")
        print(f"Quantities: {order['quantity']}")
        print(f"Timestamp: {order['timestamp']}")
        print("-" * 50)
        
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
