from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

import json

# Define the sample JSON string (output of hl7_to_standard_json)
json_string = '''
{
  "sending_facility": "PREOP^101A1^1^^^CI",
  "message_datetime": "",
  "patient_id": "100065799^1^MRN^1",
  "last_name": "DUCK",
  "first_name": "DONALD",
  "dob": "19241010",
  "gender": "M",
  "provider_id": "37^DISNEY^WALT^^^^^AccMgr^^^^CI",
  "primary_diagnosis_code": "71596^OSTEOARTHROS NOS-L/LEG ^I9",
  "claim_id": "123121234",
  "payer_name": "MEDICAL MUTUAL CALIF.",
  "claim_start_date": "056269770",
  "claim_end_date": "20250101"
}
'''

# Create a Spark session
spark = SparkSession.builder.appName("Test JSON to Struct").getOrCreate()

# Define the schema
hl7_schema = StructType([
    StructField("sending_facility", StringType()),
    StructField("message_datetime", StringType()),
    StructField("patient_id", StringType()),
    StructField("last_name", StringType()),
    StructField("first_name", StringType()),
    StructField("dob", StringType()),
    StructField("gender", StringType()),
    StructField("provider_id", StringType()),
    StructField("primary_diagnosis_code", StringType()),
    StructField("claim_id", StringType()),
    StructField("payer_name", StringType()),
    StructField("claim_start_date", StringType()),
    StructField("claim_end_date", StringType())
])

# Create DataFrame from string
df = spark.createDataFrame([(json_string,)], ["parsed_json"])

# Apply from_json
parsed_struct_df = df.withColumn("data", from_json(col("parsed_json"), hl7_schema)).select("data.*")

# Show result
parsed_struct_df.show(truncate=False)
