from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Re-Ingest CSV to Bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

csv_path = r"C:\Users\sreej\Health Care Project\01-ingestion-pipeline\csv_ingestion\downloads\extracted"
bronze_path = r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\bronze\csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("delimiter", "|") \
    .csv(csv_path)

df.write.format("delta").mode("overwrite").save(bronze_path)

spark.stop()
