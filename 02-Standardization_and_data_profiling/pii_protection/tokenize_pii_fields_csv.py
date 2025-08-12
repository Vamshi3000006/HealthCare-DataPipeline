from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import requests

# Get project ID from metadata
project_id = requests.get(
    "http://metadata.google.internal/computeMetadata/v1/project/project-id",
    headers={"Metadata-Flavor": "Google"}
).text

spark = SparkSession.builder.appName("TokenizePII").getOrCreate()

input_path = "gs://health-care-project/Health_Care_Project/03-processing-delta-spark/silver_unified/csv"
output_path = "gs://health-care-project/Health_Care_Project/03-processing-delta-spark/silver_unified_secure/csv"
df = spark.read.format("delta").load(input_path)

def deidentify_with_dlp(value, column_name):
    if not value:
        return ""

    from google.cloud import dlp_v2
    dlp = dlp_v2.DlpServiceClient()
    parent = f"projects/{project_id}"
    item = {"value": value}

    if column_name == "patient_id":
        inspect_config = {
            "custom_info_types": [
                {
                    "info_type": {"name": "PATIENT_ID"},
                    "regex": {"pattern": r"-?\d{8,14}"}
                }
            ]
        }
    elif column_name == "claim_id":
        inspect_config = {
            "custom_info_types": [
                {
                    "info_type": {"name": "CLAIM_ID"},
                    "regex": {"pattern": r"-?\d{8,14}"}
                }
            ]
        }
    else:
        inspect_config = {}

    deidentify_config = {
        "info_type_transformations": {
            "transformations": [
                {
                    "info_types": [{"name": column_name.upper()}],
                    "primitive_transformation": {
                        "crypto_deterministic_config": {
                            "crypto_key": {
                                "transient": {"name": "my-transient-key"}
                            },
                            "surrogate_info_type": {"name": "TOKEN"}
                        }
                    }
                }
            ]
        }
    }

    try:
        response = dlp.deidentify_content(
            request={
                "parent": parent,
                "deidentify_config": deidentify_config,
                "inspect_config": inspect_config,
                "item": item
            }
        )
        return response.item.value
    except Exception as e:
        return f"[DLP_ERROR]: {str(e)}"

# UDFs
patient_udf = udf(lambda x: deidentify_with_dlp(x, "patient_id"), StringType())
claim_udf   = udf(lambda x: deidentify_with_dlp(x, "claim_id"), StringType())

df = df.withColumn("patient_id", patient_udf(col("patient_id")))
df = df.withColumn("claim_id", claim_udf(col("claim_id")))

df.write.format("delta").mode("overwrite").save(output_path)
