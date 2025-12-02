from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, concat, lit

# Create Spark session for kafka
spark = SparkSession.builder \
    .appName("KafkaSteamProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

try:
    # kafka consumer
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "producer_test") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # data processing
    processed_df = df.selectExpr("CAST(value AS STRING) as message") \
        .select(
            col("message").alias("value")
        )

    processed_df = df.selectExpr("CAST(value AS STRING) as original_message") \
        .select(
            concat(
                lit("forwarded order is "), 
                col("original_message")
            ).alias("value")
        )

    # kafka producer
    query = processed_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "forwarded_topic") \
        .option("checkpointLocation", "/tmp/kafka_checkpoint") \
        .outputMode("append") \
        .trigger(processingTime='5 seconds') \
        .start()

    # print to console
    console_query = df.selectExpr("CAST(value AS STRING) as message") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()

    # wait to finish
    query.awaitTermination()

except Exception as e:
    print(f"Error: {e}")
finally:
    spark.stop()
