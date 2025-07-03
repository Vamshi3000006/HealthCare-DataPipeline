from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckHL7Schema").getOrCreate()

df = spark.read.format("delta").load("./01-ingestion-pipeline/hl7_streaming/hl7_output")
df.printSchema()
df.show(5, truncate=False)

spark.stop()
