import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    'purchase-orders',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='simple-beam'
)

def simple_beam_processor():
    
    print("Apache Beam started")
    
    try:
        for message in consumer:
            order_data = message.value
            
            # Process single message -> very simple aggregation. Order + 1
            with beam.Pipeline(options=PipelineOptions(['--runner=DirectRunner'])) as p:
                result = (
                    p 
                    | 'CreateSingle' >> beam.Create([order_data])
                    | 'IncrementID' >> beam.Map(lambda x: {
                        'original': x['order_id'],
                        'new': x['order_id'] + 1,
                        'beam_processed': True
                    })
                    | 'FormatResult' >> beam.Map(lambda x: 
                        f"Order: {x['original']} -> {x['new']}")
                    | 'Print' >> beam.Map(print)
                    )
    except KeyboardInterrupt:
        print("Stopping")
    finally:
        consumer.close()

if __name__ == '__main__':
    simple_beam_processor()
