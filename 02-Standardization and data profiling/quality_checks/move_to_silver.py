from pyspark.sql import SparkSession

# Step 1: Start Spark
spark = SparkSession.builder \
    .appName("Move Valid HL7 to Silver") \
    .getOrCreate()

# Step 2: Load HL7 Bronze data
bronze_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7_streaming"
df = spark.read.format("delta").load(bronze_path)

# Step 3: Filter valid records
valid_df = df.filter("patient_id IS NOT NULL")

# Step 4: Write to Silver zone
silver_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/silver/hl7"
valid_df.write.format("delta").mode("overwrite").save(silver_path)

print("✅ Valid HL7 records moved to Silver zone")

# Stop Spark
spark.stop()
