import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
import json

import random 
import numpy as np

prng = np.random.RandomState(42)

item_id = np.arange(0,10)
height = prng.randint(25,31,10)
length = prng.randint(20,25,10)
width = prng.randint(15,20,10)
weight = prng.randint(1,5,10)
price = prng.randint(100,200,10)

consumer = KafkaConsumer(
    'purchase-orders',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='simple-beam'
)

# still need to calculate optimal dimensions
def calculate_total_dimensions(order_data):
    total_height = 0
    total_length = 0
    total_width = 0
    total_weight = 0
    total_price = 0
    for position, item_id in enumerate(order_data['item_id']):
        qty = order_data['quantity'][position]
        total_height += height[item_id]*qty
        total_length += length[item_id]*qty
        total_width += width[item_id]*qty
        total_weight += weight[item_id]*qty
        total_price += price[item_id]*qty
    return {
        'order_id':order_data['order_id'],
        'parcel_height':int(total_height),
        'parcel_length':int(total_length),
        'parcel_width':int(total_width),
        'parcel_weight':int(total_weight),
        'total_price':int(total_price)
            }

def simple_beam_processor():
    
    print("Apache Beam started")
    
    try:
        for message in consumer:
            # order_id, item_id (array), quantity (array), timestamp
            order_data = message.value
            print(f'message value:{order_data}')
            #print(f'order_id: {order_data['order_id']}, total height:{total_height}, total_length:{total_length}, total_width:{total_width}, total_weight:{total_weight}')

            
            # Super simple pipieline, just apply function 
            with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as p:
                result = (
                    p 
                    | 'CreateSingle' >> beam.Create([order_data])
                    | 'ParcelData' >> beam.Map(calculate_total_dimensions)
                    | 'Print' >> beam.Map(print)
                    )
    except KeyboardInterrupt:
        print("Stopping")
    finally:
        consumer.close()

if __name__ == '__main__':
    simple_beam_processor()

