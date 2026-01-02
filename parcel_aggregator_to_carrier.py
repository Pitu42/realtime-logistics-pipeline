from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, concat, lit, from_json, udf, explode, to_timestamp, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType


# schemas
item_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("zipcode", StringType(), True),
    StructField("item_id", ArrayType(item_schema), True),
    StructField("timestamp", StringType(), True)
])

# Create spark session for kafka and add postgresql driver from website
spark = SparkSession.builder \
    .appName("KafkaSteamProcessing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.7.1") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

print("Loaded JARs:", spark.sparkContext.getConf().get("spark.jars", "None"))
print("Loaded packages:", spark.sparkContext.getConf().get("spark.jars.packages", "None"))

spark.sparkContext.setLogLevel("WARN")

# From ordered items: calculates parcel dimension and weight
def complete_aggregation2():
    try:
        # consumer
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "purchase-orders") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("kafka.session.timeout.ms", "30000") \
            .option("kafka.request.timeout.ms", "40000") \
            .load()

        # Item data from PostgreSQL
        item_df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/item_data") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "itmd") \
            .option("user", "admin") \
            .option("password", "password") \
            .load()

        parsed_df = df.selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), order_schema).alias("order_data")) \
            .select(
                col("order_data.order_id").alias("order_id"),
                col("order_data.zipcode").alias("zipcode"),
                col("order_data.item_id").alias("items"),
                col("order_data.timestamp").alias("timestamp")
            )

        exploded_df = parsed_df.select(
            col("order_id"),
            col("zipcode"),
            to_timestamp(col("timestamp")).cast("timestamp").alias("timestamp"),
            explode(col("items")).alias("item")
        ).select(
            col("order_id"),
            col("zipcode"),
            col("timestamp"),
            col("item.item_id").alias("item_id"),
            col("item.quantity").alias("quantity")
        )

        enriched_df = exploded_df.join(
            item_df.select("item_id", "dimensions", "weight"), 
            "item_id", 
            "left"
        ).withColumn(
            "total_item_volume", col("dimensions") * col("quantity")
        ).withColumn(
            "total_item_weight", col("weight") * col("quantity")
        )

        watermarked_df = enriched_df.withWatermark("timestamp", "2 minutes")

        aggregated_df = watermarked_df.groupBy("order_id","zipcode", "timestamp").agg(
            spark_sum("total_item_volume").alias("total_dimensions"),
            spark_sum("total_item_weight").alias("total_weight")
        )
        
        processed_df = aggregated_df.select(
            col("order_id").cast("string").alias("key"),
            to_json(
                struct(
                    col("order_id").alias("order_id"),
                    col("zipcode").alias("zipcode"),
                    col("total_dimensions").alias("dimensions"),
                    col("total_weight").alias("weight"),
                    col("timestamp").alias("creation_date")
                )
            ).alias("value")  
        )

        # producer
        query = processed_df \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "parcel-data") \
            .option("checkpointLocation", "/tmp/kafka_checkpoint_new") \
            .outputMode("append") \
            .trigger(processingTime='5 seconds') \
            .start()

        # print 
        console_query = processed_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='5 seconds') \
            .start()

        # wait to finish
        console_query.awaitTermination()
        query.awaitTermination()

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

complete_aggregation2()

