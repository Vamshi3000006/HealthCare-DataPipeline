from google.cloud import dlp_v2
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import json

# Create Spark session
spark = SparkSession.builder \
    .appName("TokenizePII") \
    .getOrCreate()

# Initialize Google DLP client
dlp = dlp_v2.DlpServiceClient()
project = "projects/YOUR_PROJECT_ID"  # Replace this with your GCP project

def call_dlp_deidentify(value: str, info_type: str) -> str:
    """Send a single value to DLP for masking/tokenization."""
    if not value:
        return None
    
    item = {"value": value}
    
    # Example: Tokenize patient_id with deterministic encryption
    config = {
        "info_type_transformations": {
            "transformations": [{
                "info_types": [{"name": info_type}],
                "primitive_transformation": {
                    "crypto_deterministic_config": {
                        "crypto_key": {
                            "transient": {
                                "name": "dlp-key"
                            }
                        },
                        "surrogate_info_type": {"name": f"TOKEN_{info_type}"}
                    }
                }
            }]
        }
    }

    try:
        response = dlp.deidentify_content(
            request={
                "parent": project,
                "deidentify_config": config,
                "item": item
            }
        )
        return response.item.value
    except Exception as e:
        print(f"Error in DLP API: {e}")
        return value
