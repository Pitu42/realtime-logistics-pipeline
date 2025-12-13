import psycopg2
import random
import numpy as np

# Connect to postgres docker
postgres_conn = psycopg2.connect(
    host="postgres", #localhost",
    user="admin",
    password="password",
    database="item_data"
        )

cur = postgres_conn.cursor()

# Create item data
prng = np.random.RandomState(42)

item_id = np.arange(0,10)
height = prng.randint(25,31,10)
length = prng.randint(20,25,10)
width = prng.randint(15,20,10)
dimensions = height * length * width
weight = prng.randint(1,5,10)
price = prng.randint(100,200,10)

# Drop table item master data if exists
cur.execute("DROP TABLE IF EXISTS itmd;")

# Create table
cur.execute("""CREATE TABLE itmd (
    item_id serial PRIMARY KEY,
    height integer,
    length integer,
    width integer,
    dimensions integer,
    weight integer,
    price integer
            );""")

# Insert Data
data_to_insert = list(zip(item_id.tolist(), height.tolist(), length.tolist(), width.tolist(), dimensions.tolist(), weight.tolist(), price.tolist()))

cur.executemany("""
    INSERT INTO itmd (item_id, height, length, width, dimensions, weight, price)
    VALUES(%s,%s,%s,%s,%s,%s,%s)
                """, data_to_insert)

postgres_conn.commit()

# Close connection
cur.close()
postgres_conn.close()
