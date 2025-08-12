from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Debug API") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.option("multiline", "true").json("C:/Users/sreej/Health Care Project/01-ingestion-pipeline/api_ingestion/data")
df.printSchema()
df.show(5, truncate=False)

spark.stop()
