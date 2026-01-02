from pymongo import MongoClient
import time
import requests
import datetime
import random

url = 'http://status_receiver_api:8001/send-status'

mongo_client = MongoClient('mongodb://admin:password@mongodb:27017/')
db = mongo_client['parcel_db']
parcel_collection = db['parcel_orders']

# Wait for orders
time.sleep(30)

# Change the status of a random order
while True:
    # Check how many orders have no status
    na_count = parcel_collection.count_documents({"status": None})
    if na_count > 0:
        cursor = parcel_collection.aggregate([
            {"$match": {"status": None}},
            {"$sample": {"size": 1}}
        ])
        item = next(cursor, None)

        if item:
            print("Selected row:", item)
            print(f"Order ID: {item['order_id']}")
            print(f"Tracking Number: {item['tracking_number']}")
            
            random_status = random.random()
            status = 0

            if random_status >= 0.99:
                status = 'cancelled'
            elif random_status >= 0.95:
                status = 'not_delivered'
            else:
                status = 'delivered'

            result = parcel_collection.update_one(
                {"order_id": item["order_id"]},  # Filter by the order_id
                {"$set": {"status": status}}  # update status 
            )
            json_dict = {
                'order_id':item['order_id'],
                'tracking_number':item['tracking_number'],
                'status':status,
                'status_time':datetime.datetime.now().isoformat()
                    } 
            response = requests.post(url, json=json_dict)
            if result.modified_count > 0:
                print("Order updated successfully!")
            else:
                print("No order was updated")
        else:
            print("No order found")
    else:
        print("All orders have status")
    time.sleep(1)

