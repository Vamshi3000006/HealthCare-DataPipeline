from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("cleaning CSV") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
csv_input_path = r"C:\Users\sreej\Health_Care_Project\03-processing-delta-spark\bronze\csv"
csv_output_path = r"C:\Users\sreej\Health_Care_Project\03-processing-delta-spark\silver\csv"

df = spark.read.format("delta").load(csv_input_path)
threshold = 0.9
total_rows = df.count()
non_null_counts = df.select([
    count(when(col(c).isNotNull(), c)).alias(c) for c in df.columns
]).first().asDict()

columns_to_drop = [col_name for col_name, non_nulls in non_null_counts.items()
                   if (non_nulls / total_rows) < (1 - threshold)]

# Step 5: Drop those columns
df_cleaned = df.drop(*columns_to_drop)

print(f"Dropped {len(columns_to_drop)} columns due to >90% nulls:")
for c in columns_to_drop:
    print(f" - {c}")

df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("csv_output_path")

spark.stop()