from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("HL7 Profiling") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read HL7 delta table
df = spark.read.format("delta").load("../01-ingestion-pipeline/hl7_streaming/hl7_output")

# Basic profiling
print("Schema:")
df.printSchema()

print("\nTotal Rows:", df.count())

print("\nNull value count per column:")
for col in df.columns:
    nulls = df.filter(df[col].isNull()).count()
    print(f"{col}: {nulls}")

print("\nSample Data:")
df.show(5, truncate=False)
