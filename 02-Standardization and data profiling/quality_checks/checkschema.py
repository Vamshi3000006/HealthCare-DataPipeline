from pyspark.sql import SparkSession

# Include Delta configs here!
spark = SparkSession.builder \
    .appName("Inspect CSV Bronze Schema") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to Bronze CSV
bronze_path = r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\silver\api"

# Load and inspect schema
df = spark.read.format("delta").load(bronze_path)
df.printSchema()
df.show(5)

spark.stop()
