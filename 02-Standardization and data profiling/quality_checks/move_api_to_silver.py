from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Move CSV to Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Load Bronze CSV
bronze_path = r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/api"
df = spark.read.format("delta").load(bronze_path)

# Write to Silver as-is (no patient_id or age filtering)
silver_path = r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/silver_unified/api"
df.write.format("delta").mode("overwrite").save(silver_path)

print("✅ CSV Bronze → Silver completed.")

spark.stop()
