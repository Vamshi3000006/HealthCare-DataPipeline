from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import os
from dotenv import load_dotenv

# -------------------------------
# Step 1: Load .env and set up credentials
# -------------------------------
load_dotenv(dotenv_path="C:/Users/sreej/Health_Care_Project/.env")
credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
project_id = os.getenv("PROJECT_ID")

# -------------------------------
# Step 2: Spark session
# -------------------------------
spark = SparkSession.builder.appName("TokenizePII").getOrCreate()

# -------------------------------
# Step 3: Read input Delta file
# -------------------------------
input_path = "C:/Users/sreej/Health_Care_Project/03-processing-delta-spark/silver_unified/hl7"
output_path = "C:/Users/sreej/Health_Care_Project/03-processing-delta-spark/silver_unified_secure/hl7"
df = spark.read.format("delta").load(input_path)

# -------------------------------
# Step 4: DLP logic (revised)
# -------------------------------
def deidentify_with_dlp(value, column_name):
    if not value:
        return ""
    from google.cloud import dlp_v2
    dlp = dlp_v2.DlpServiceClient()
    parent = f"projects/{project_id}"
    item = {"value": value}

    inspect_config = None

    if column_name == "patient_id":
        inspect_config = {
            "custom_info_types": [
                {
                    "info_type": {"name": "PATIENT_ID"},
                    "regex": {"pattern": r"\d{5,10}"}
                }
            ]
        }
        config = {
            "info_type_transformations": {
                "transformations": [{
                    "info_types": [{"name": "PATIENT_ID"}],
                    "primitive_transformation": {
                        "crypto_deterministic_config": {
                            "crypto_key": {
                                "transient": {"name": "my-transient-key"}
                            },
                            "surrogate_info_type": {"name": "TOKEN"}
                        }
                    }
                }]
            }
        }
    elif column_name == "claim_id":
        config = {
            "info_type_transformations": {
                "transformations": [{
                    "primitive_transformation": {
                        "crypto_deterministic_config": {
                            "crypto_key": {
                                "transient": {"name": "my-transient-key"}
                            },
                            "surrogate_info_type": {"name": "TOKEN"}
                        }
                    }
                }]
            }
        }
    else:
        config = {
            "info_type_transformations": {
                "transformations": [{
                    "primitive_transformation": {
                        "replace_with_info_type_config": {}
                    }
                }]
            }
        }

    try:
        response = dlp.deidentify_content(
            request={
                "parent": parent,
                "deidentify_config": config,
                "item": item,
                "inspect_config": inspect_config
            }
        )
        return response.item.value
    except Exception as e:
        return f"[DLP_ERROR]: {str(e)}"

# -------------------------------
# Step 5: Register UDFs
# -------------------------------
patient_udf = udf(lambda x: deidentify_with_dlp(x, "patient_id"), StringType())
claim_udf   = udf(lambda x: deidentify_with_dlp(x, "claim_id"), StringType())
fname_udf   = udf(lambda x: deidentify_with_dlp(x, "first_name"), StringType())
lname_udf   = udf(lambda x: deidentify_with_dlp(x, "last_name"), StringType())
dob_udf     = udf(lambda x: deidentify_with_dlp(x, "dob"), StringType())

# -------------------------------
# Step 6: Apply UDFs to DataFrame
# -------------------------------
df = df.withColumn("patient_id", patient_udf(col("patient_id")))
df = df.withColumn("claim_id", claim_udf(col("claim_id")))
df = df.withColumn("first_name", fname_udf(col("first_name")))
df = df.withColumn("last_name", lname_udf(col("last_name")))
df = df.withColumn("dob", dob_udf(col("dob")))

# -------------------------------
# Step 7: Save the secured output
# -------------------------------
df.write.format("delta").mode("overwrite").save(output_path)
