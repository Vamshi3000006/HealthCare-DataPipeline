from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import json
import sys

# ✅ Get the source type from command-line argument
source_type = sys.argv[1]

# ✅ Load unify-config.json
with open("../02-Standardization and data profiling/unify-config.json", "r") as f:
    config = json.load(f)

# ✅ Extract paths
input_path = config["mappings"][source_type]["path"]
output_path = config["mappings"][source_type]["output_path"]

# ✅ Configure Spark with Delta Lake support
builder = SparkSession.builder \
    .appName("StandardizeColumns") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ✅ Load and standardize the input data
if source_type == "csv":
    df = spark.read.option("header", True).csv(input_path)
    column_mapping = config["mappings"][source_type]["standard_column_names"]

elif source_type == "hl7":
    df = spark.read.format("delta").load(input_path)
    column_mapping = config["mappings"][source_type]["standard_column_names"]

elif source_type == "api":
    df = spark.read.option("multiline", "true").json(input_path)
    column_mapping = {
        k: v for k, v in config["mappings"][source_type].items()
        if k not in ["path", "output_path"]
    }

else:
    raise ValueError("Unsupported source type")

# ✅ Rename columns if present
for source_col, standard_col in column_mapping.items():
    if source_col in df.columns:
        df = df.withColumnRenamed(source_col, standard_col)

# ✅ Save as Delta Lake format
df.write.format("delta").mode("overwrite").save(output_path)

# ✅ Done
spark.stop()
