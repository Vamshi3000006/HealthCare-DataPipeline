from pyspark.sql import SparkSession

# 1. Start SparkSession with Delta support
spark = SparkSession.builder \
    .appName("Verify HL7 Parsed Data") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. Path to your Delta table
delta_path = "file:///C:/Users/sreej/Health Care Project/01-ingestion-pipeline/hl7_streaming/hl7_output"

# 3. Load the Delta table
df = spark.read.format("delta").load(delta_path)

# 4. Show actual HL7 segments (MSH, PID, PV1)
df.select("MSH", "PID", "PV1").show(truncate=False)

# Optional: Print schema
df.printSchema()
