from pyspark.sql import SparkSession
import great_expectations as ge

# Step 1: Start Spark
spark = SparkSession.builder \
    .appName("API Validation with Great Expectations") \
    .getOrCreate()

# Step 2: Load silver/api data
api_path = r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/silver/api"
df = spark.read.format("delta").load(api_path)

# Convert to Pandas sample for GE
pandas_df = df.limit(1000).toPandas()

# Step 3: GE context and expectations
context = ge.get_context()
validator = context.sources.pandas_default.read_dataframe(pandas_df)

validator.expect_column_values_to_not_be_null("approval_time")
validator.expect_column_values_to_be_in_type_list("usage", ["str", "object"])
validator.expect_column_values_to_be_in_type_list("warnings", ["str", "object"])

# Step 4: Validate
results = validator.validate()

print("\n" + "="*60)
if not results["success"]:
    print(" VALIDATION FAILED — Some records are invalid.")
    invalid_df = df.filter("approval_time IS NULL")
    if invalid_df.count() > 0:
        print("  Writing bad records to dead letter...")
        invalid_df.write.format("delta").mode("overwrite") \
            .save("C:/Users/sreej/Health Care Project/03-processing-delta-spark/dead_letter/api")
else:
    print(" VALIDATION PASSED — Ready for Silver Unified layer.")
print("="*60)
if results["success"]:
    print("GE Validation Passed")
else:
    print("GE Validation Failed")


spark.stop()
