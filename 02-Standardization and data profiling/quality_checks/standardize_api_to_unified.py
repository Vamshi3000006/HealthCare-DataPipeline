from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import json

# Load config
with open("unify-config.json") as f:
    config = json.load(f)

input_path = config["mappings"]["api"]["output_path"]  # silver/api
output_path = "silver_unified/api"
column_mapping = {
    k: v for k, v in config["mappings"]["api"].items()
    if k not in ["path", "output_path"]
}

# Spark session
builder = SparkSession.builder \
    .appName("StandardizeAPI") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Read from silver
df = spark.read.format("delta").load(input_path)

# Rename columns
for source_col, standard_col in column_mapping.items():
    if source_col in df.columns:
        df = df.withColumnRenamed(source_col, standard_col)

# Write to silver_unified
df.write.format("delta").mode("overwrite").save(output_path)

spark.stop()
