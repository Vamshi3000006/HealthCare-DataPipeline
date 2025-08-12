from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verify Silver Unified Outputs") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

paths = {
    "csv": r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\silver_unified\csv",
    "hl7": r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\silver_unified\hl7",
    "api": r"C:\Users\sreej\Health Care Project\03-processing-delta-spark\silver_unified\api"
}

for source, path in paths.items():
    print(f"\n===== Verifying {source.upper()} data =====")
    df = spark.read.format("delta").load(path)

    print(f"\nSchema for {source}:")
    df.printSchema()

    print(f"\nSample rows from {source}:")
    df.select("patient_id").show(5)

    null_count = df.filter("patient_id IS NULL").count()
    print(f"\nNull patient_id count in {source}: {null_count}")

spark.stop()
