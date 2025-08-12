from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckAPISchema").getOrCreate()

df = spark.read.option("multiline", "true").json("data")
df.printSchema()
df.show(5, truncate=False)

spark.stop()
