import psycopg2
import json
from datetime import datetime
import schedule
import time
from pymongo import MongoClient

mongo_client = MongoClient('mongodb://admin:password@mongodb:27017/')

# order data
orders_db = mongo_client['orders_db']
orders_collection = orders_db['purchase_orders']

# parcel data
parcel_db = mongo_client['parcel_db'] 
parcel_collection = parcel_db['purchase_orders']

DB_CONFIG = {
    "host": "postgres",
    "database": "item_data", 
    "user": "admin",
    "password": "password"
}

def setup_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_analytics (
            order_id INTEGER PRIMARY KEY,
            zipcode VARCHAR(20),
            items JSONB,
            order_timestamp TIMESTAMP,
            tracking_number VARCHAR(50),
            status VARCHAR(20),
            status_date TIMESTAMP
        )
    """)
    # last sync for orders and parcel
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS last_sync (
            sync_type VARCHAR(20) PRIMARY KEY,
            timestamp TIMESTAMP
        )
    """)
    
    conn.commit()
    cursor.close()
    conn.close()

def get_last_sync(sync_type):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT timestamp FROM last_sync WHERE sync_type = %s", (sync_type,))
    result = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if result:
        return result[0]
    else:
        return datetime.min

def update_last_sync(sync_type):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO last_sync (sync_type, timestamp) VALUES (%s, %s)
        ON CONFLICT (sync_type) DO UPDATE SET timestamp = EXCLUDED.timestamp
    """, (sync_type, datetime.now()))
    
    conn.commit()
    cursor.close()
    conn.close()

def sync_orders():
    last_sync = get_last_sync('orders')
    
    # select orders > last sync timestamp
    new_orders = list(orders_collection.find({
        'timestamp': {'$gt': last_sync.isoformat()}
    }))
    
    if not new_orders:
        print("No new orders updates found")
        return
        
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    for order in new_orders:
        cursor.execute("""
            INSERT INTO order_analytics (order_id, zipcode, items, order_timestamp, tracking_number)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            order['order_id'],
            order['zipcode'],
            json.dumps(order['item_id']),
            order['timestamp'],
            order.get('tracking_number') # tracking_number can be none 
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    update_last_sync('orders')
    print(f"New orders: {len(new_orders)}")

def join_parcel():
    last_sync = get_last_sync('parcel')
    
    # select parcel > last sync timestamp
    new_parcels = list(parcel_collection.find({
        'status_date': {'$gt': last_sync.isoformat()}
    }))
    
    if not new_parcels:
        print("No new parcel updates found")
        return
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    for parcel in new_parcels:
        cursor.execute("""
            UPDATE order_analytics 
            SET status = %s, status_date = %s
            WHERE order_id = %s and status is null
        """, (
            parcel['status'],
            parcel['status_date'],
            parcel['order_id']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    update_last_sync('parcel')
    
    print(f"New status update: {len(new_parcels)}")


def full_update():
    sync_orders()
    join_parcel()


schedule.every(5).minutes.do(full_update)

if __name__ == "__main__":
    setup_tables()
    
    print("Starting postgres update")
    full_update()
    
    while True:
        schedule.run_pending()
        time.sleep(60)

