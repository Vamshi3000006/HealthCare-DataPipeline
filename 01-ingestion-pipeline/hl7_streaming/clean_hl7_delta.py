# clean_hl7_delta.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Clean HL7 Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Path to your HL7 Delta table
delta_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7_streaming"

# Overwrite table with empty DataFrame (same schema)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_path)

# Vacuum is optional, but keeps the storage clean
delta_table.delete()  # Removes all rows

# (Optional) Clean up files
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
delta_table.vacuum(0)

print("✅ Old binary records deleted from Delta table.")
