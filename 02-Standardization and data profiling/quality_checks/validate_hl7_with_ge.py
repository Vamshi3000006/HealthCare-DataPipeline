from pyspark.sql import SparkSession
import great_expectations as ge

# Step 1: Start Spark and load HL7 Bronze data
spark = SparkSession.builder \
    .appName("HL7 Validation with Great Expectations") \
    .getOrCreate()

bronze_path = r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\bronze\hl7_streaming"
df = spark.read.format("delta").load(bronze_path)

# Step 2: Convert to small Pandas batch for in-memory GE validation
pandas_df = df.limit(1000).toPandas()

# Step 3: Create in-memory GE context and Validator
context = ge.get_context()
validator = context.sources.pandas_default.read_dataframe(pandas_df)

# Step 4: Define expectations
validator.expect_column_values_to_not_be_null("patient_id")
validator.expect_column_values_to_be_between("age", min_value=0, max_value=120)

# Step 5: Validate and print result
results = validator.validate()

print("\n" + "="*60)
if not results["success"]:
    print("❌❌❌  VALIDATION FAILED — Some records are invalid.  ❌❌❌")
else:
    print("✅✅✅  VALIDATION PASSED — Ready for Silver layer.  ✅✅✅")
print("="*60 + "\n")

# Step 6: Optionally save bad rows to Delta dead-letter path
invalid_df = df.filter("patient_id IS NULL OR age < 0 OR age > 120")
if invalid_df.count() > 0:
    print("⚠ Writing bad rows to deadletter path...")
    invalid_df.write.format("delta") \
        .mode("overwrite") \
        .save("C:/Users/sreej/Health Care Project/03-processing-delta-spark/dead_letter/hl7")
else:
    print("✅ No bad rows to move to deadletter.")

# Step 7: Stop Spark
spark.stop()
