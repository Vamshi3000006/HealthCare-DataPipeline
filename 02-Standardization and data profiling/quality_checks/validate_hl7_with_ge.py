from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import great_expectations as ge
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

# Step 1: Build SparkSession with Delta support
builder = SparkSession.builder \
    .appName("ValidateHL7WithGE") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 2: Load HL7 Delta table from Bronze layer
hl7_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7"
df = spark.read.format("delta").load(hl7_path)

# Step 3: Wrap DataFrame with Great Expectations
gedf = SparkDFDataset(df)

# Step 4: Add expectations
gedf.expect_column_to_exist("patient_id")
gedf.expect_column_values_to_not_be_null("patient_id")
gedf.expect_column_to_exist("message_datetime")

# Step 5: Evaluate results
results = gedf.validate()
print(results)

# Optional: Stop Spark session
spark.stop()
