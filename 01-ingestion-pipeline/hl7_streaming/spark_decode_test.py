from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Step 1: Create Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HL7 Decode Test") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()


# Step 2: Read from Kafka topic 'hl7-events'
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hl7-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Step 3: Cast Kafka `value` to string
decoded_df = data.withColumn("decoded_value", expr("CAST(value AS STRING)"))

# Step 4: Show raw decoded HL7 messages to console
query = decoded_df.select("decoded_value").writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
