from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Check Cleaned Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

delta_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7_streaming"

df = spark.read.format("delta").load(delta_path)

df.printSchema()
df.show(10, truncate=False)

spark.stop()
