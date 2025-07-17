from pyspark.sql import SparkSession
from great_expectations.data_context import get_context  # ✅ Correct for v1.5+


# ─── Step 1: Start Spark with Delta enabled ────────────────────────────────────
spark = (
    SparkSession.builder
        .appName("API Validation with Great Expectations")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

# ─── Step 2: Load Bronze/Silver API Delta table ───────────────────────────────
api_path = r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/silver/api"
df = spark.read.format("delta").load(api_path)

# ─── Step 3: Sample to Pandas for GE ──────────────────────────────────────────
# (you can adjust the .limit() or drop this if you want to validate everything in Spark later)
pandas_df = df.limit(1000).toPandas()

# ─── Step 4: Point GE at your scaffolded project ──────────────────────────────
ge_root = r"C:/Users/sreej/Health Care Project/02-Standardization and data profiling/great_expectations"
context = get_context()

# ─── Step 5: Turn that pandas batch into a Validator ─────────────────────────
validator = context.sources.pandas_default.read_dataframe(pandas_df)

# ─── Step 6: Define your expectations ─────────────────────────────────────────
validator.expect_column_values_to_not_be_null("approval_time")
validator.expect_column_values_to_be_in_type_list("usage", ["str", "object"])
validator.expect_column_values_to_be_in_type_list("warnings", ["str", "object"])

# ─── Step 7: Run validation & write dead‐letter if needed ────────────────────
results = validator.validate()

print("\n" + "="*60)
if not results["success"]:
    print("VALIDATION FAILED — writing bad records to dead_letter ")
    # filter Spark DF on the same null condition and write out
    bad = df.filter("approval_time IS NULL")
    if bad.count() > 0:
        bad.write.format("delta") \
           .mode("overwrite") \
           .save(r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/dead_letter/api")
else:
    print("VALIDATION PASSED — ready for Silver Unified layer")
print("="*60)

spark.stop()
