from pyspark.sql import SparkSession

# 1. Start SparkSession with Delta support
spark = SparkSession.builder \
    .appName("Verify HL7 Final Parsed Data") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Path to your structured Delta table
delta_path = "file:///C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7_streaming"

# 3. Load Delta table
df = spark.read.format("delta").load(delta_path)

# 4. Show structured HL7 fields — pick any you want to verify
df.select(
    "sending_facility",
    "message_datetime",
    "patient_id",
    "first_name",
    "last_name",
    "dob",
    "gender",
    "claim_id",
    "payer_name",
    "claim_start_date",
    "claim_end_date"
).show(truncate=False)

# 5. Optional: print schema to confirm everything is parsed correctly
df.printSchema()
