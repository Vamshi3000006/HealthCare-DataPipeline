from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Define the schema of your HL7 JSON message
schema = StructType().add("hl7_raw", StringType())

# Initialize SparkSession with Delta support
spark = SparkSession.builder \
    .appName("HL7 Kafka to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read from Kafka
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hl7-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert value (bytes) to string and parse JSON
json_df = data.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# Write to output folder as Delta format
query = json_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", "hl7_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()

query.awaitTermination()
