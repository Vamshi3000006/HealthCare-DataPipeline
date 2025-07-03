import json
import sys
from pyspark.sql import SparkSession
import re


# Create Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("StandardizeColumns") \
    .getOrCreate()

# Load JSON config file
with open("unify-config.json", "r") as f:
    config = json.load(f)

# Get source type argument
source_type = sys.argv[1]  # e.g., 'csv'

# Extract config values
input_path = config["mappings"][source_type]["path"]
output_path = config["mappings"][source_type]["output_path"]
column_mapping = config["mappings"][source_type]["standard_column_names"]

# Read source data
df = spark.read.format("csv").option("header", "true").load(input_path)

# Rename columns to standard names
for standard_col, source_col in column_mapping.items():
    if source_col in df.columns:
        df = df.withColumnRenamed(source_col, standard_col)

# Write to Delta Lake format

df = df.toDF(*[re.sub(r"[ ,;{}()\n\t=]", "_", col) for col in df.columns])


df.write.format("delta").mode("overwrite").save(output_path)

# Stop Spark session
spark.stop()
