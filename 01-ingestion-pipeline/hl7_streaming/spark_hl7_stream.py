from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StructType, StructField, StringType
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

# Step 1: Define full HL7 to standardized JSON parser

def hl7_to_standard_json(message):
    try:
        lines = message.replace('\r', '\n').split('\n')
        data = {}

        for line in lines:
            parts = line.strip().split('|')
            if not parts:
                continue

            seg_type = parts[0].strip()

            if seg_type == "PID":
                data["patient_id"] = parts[3] if len(parts) > 3 else ""
                name_parts = parts[5].split("^") if len(parts) > 5 else []
                data["last_name"] = name_parts[0] if len(name_parts) > 0 else ""
                data["first_name"] = name_parts[1] if len(name_parts) > 1 else ""
                data["dob"] = parts[7] if len(parts) > 7 else ""
                data["gender"] = parts[8] if len(parts) > 8 else ""

            elif seg_type == "PV1":
                data["provider_id"] = parts[7] if len(parts) > 7 else ""
                data["sending_facility"] = parts[3] if len(parts) > 3 else ""

            elif seg_type == "DG1":
                data["primary_diagnosis_code"] = parts[3] if len(parts) > 3 else ""

            elif seg_type == "IN1" and "claim_id" not in data:
                data["claim_id"] = parts[36] if len(parts) > 36 else ""
                data["payer_name"] = parts[4] if len(parts) > 4 else ""
                data["message_datetime"] = parts[12] if len(parts) > 12 else ""

            elif seg_type == "IN2" and "claim_start_date" not in data:
                data["claim_start_date"] = parts[2] if len(parts) > 2 else ""
                data["claim_end_date"] = parts[3] if len(parts) > 3 else ""

            elif seg_type == "MSH":
                data["sending_facility"] = parts[4] if len(parts) > 4 else ""
                data["message_datetime"] = parts[6] if len(parts) > 6 else ""

        required_fields = [
            "sending_facility", "message_datetime", "patient_id",
            "last_name", "first_name", "dob", "gender",
            "provider_id", "primary_diagnosis_code", "claim_id",
            "payer_name", "claim_start_date", "claim_end_date"
        ]
        for key in required_fields:
            data.setdefault(key, "")

        return json.dumps(data, ensure_ascii=True)

    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=True)


# Step 2: Register UDF
parse_udf = udf(hl7_to_standard_json, StringType())

# Step 3: Define Spark session
spark = SparkSession.builder \
    .appName("HL7 Kafka to Delta") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()


# Step 4: Read Kafka HL7 messages
data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hl7-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()



# Step 5: Cast Kafka value to string
raw_df = data.selectExpr("CAST(value AS STRING) AS raw_str")


# Step 6: Apply custom parser to convert raw HL7 into standardized JSON
parsed_df = raw_df.withColumn("parsed_json", parse_udf(col("raw_str")))


# Define schema that matches output of hl7_to_standard_json
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

from pyspark.sql.functions import from_json

# Step 7: Parse the JSON string into structured columns
data_df = parsed_df.withColumn("data", from_json(col("parsed_json"), hl7_schema)).select("data.*")

# Step 8: Write structured data to Delta in bronze layer

output_path = "C:/Users/sreej/Health Care Project/03-processing-delta-spark/bronze/hl7_streaming"
checkpoint_path = f"{output_path}/_checkpoints"

data_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(output_path) \
    .awaitTermination()









