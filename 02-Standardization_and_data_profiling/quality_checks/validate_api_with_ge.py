from pyspark.sql import SparkSession
import great_expectations as ge

spark = (
    SparkSession.builder
        .appName("API Validation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
)

df = spark.read.format("delta").load(r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/silver/api")
batch_df = df.limit(1000).toPandas()

context = ge.get_context()
validator = context.run(
    batch_request={
        "datasource_name": "pandas",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "api_batch",
        "runtime_parameters": {"batch_data": batch_df},
        "batch_identifiers": {"default_identifier_name": "api_batch"},
    },
    expectation_suite_name="api_suite"
)

if not validator["success"]:
    df.filter("approval_time IS NULL") \
      .write.format("delta") \
      .mode("overwrite") \
      .save(r"C:/Users/sreej/Health Care Project/03-processing-delta-spark/dead_letter/api")
    exit(1)

spark.stop()
