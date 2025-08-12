from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckCSVSchema").getOrCreate()

df = spark.read.option("header", "true").option("sep", "|").csv("downloads/extracted/outpatient.csv")
#df.printSchema()
print(df.head(1))

spark.stop()
