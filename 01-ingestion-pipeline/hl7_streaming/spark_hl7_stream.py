from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define schema for HL7 JSON structure
schema = StructType().add("hl7_raw", StringType())

# Create SparkSession with Kafka support
spark = SparkSession.builder \
    .appName("HL7-Kafka-Spark-Streaming") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hl7-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value (bytes) to string and parse JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to output folder as JSON
query = json_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "hl7_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

query.awaitTermination()
