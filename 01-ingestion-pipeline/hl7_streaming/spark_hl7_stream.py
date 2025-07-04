from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StructType, StructField, StringType
import json

# Step 1: Define full HL7 to standardized JSON parser

def hl7_to_standard_json(message):
    try:
        lines = message.strip().split('\n')
        data = {}

        for line in lines:
            parts = line.strip().split('|')
            if not parts:
                continue

            seg_type = parts[0]

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

            elif seg_type == "IN1":
                data["claim_id"] = parts[36] if len(parts) > 36 else ""
                data["payer_name"] = parts[4] if len(parts) > 4 else ""
                data["message_datetime"] = parts[12] if len(parts) > 12 else ""  # timestamp

            elif seg_type == "IN2":
                data["claim_start_date"] = parts[2] if len(parts) > 2 else ""
                data["claim_end_date"] = parts[3] if len(parts) > 3 else ""

            elif seg_type == "MSH":
                data["sending_facility"] = parts[4] if len(parts) > 4 else ""
                data["message_datetime"] = parts[6] if len(parts) > 6 else ""

        return json.dumps(data)

    except Exception as e:
        return json.dumps({"error": str(e)})

# Step 2: Register UDF
parse_udf = udf(hl7_to_standard_json, StringType())

# Step 3: Define Spark session
spark = SparkSession.builder \
    .appName("HL7 Kafka to Delta") \
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

# Step 7: Parse JSON string into structured format
data_df = parsed_df.withColumn("data", from_json(col("parsed_json"), hl7_schema)) \
    .select("data.*")

# Step 8 (Optional): Show to console for debugging
data_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

# Step 9: Write clean structured HL7 to Delta table
data_df.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("path", "hl7_output/") \
    .option("checkpointLocation", "checkpoint/") \
    .start() \
    .awaitTermination()
