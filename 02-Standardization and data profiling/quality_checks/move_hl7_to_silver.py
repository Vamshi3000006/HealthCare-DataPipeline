from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Initialize Spark
spark = SparkSession.builder \
    .appName("Clean HL7 for Silver") \
    .getOrCreate()

# UDF to reformat DOB from 'YYYYMMDD' → 'YYYY-MM-DD'
def reformat_dob(dob):
    if dob and len(dob) == 8:
        return f"{dob[:4]}-{dob[4:6]}-{dob[6:]}"
    return dob

# UDF to simplify patient_id → extract before ^^^
def simplify_patient_id(pid):
    if pid and "^^^" in pid:
        return pid.split("^^^")[0]
    return pid
# Clean sending_facility → first part before ^
def clean_sending_facility(val):
    return val.split("^")[0] if val else val

# Clean provider_id → keep only the first part before ^
def clean_provider_id(val):
    return val.split("^")[0] if val else val

# Clean primary_diagnosis_code → first part before ^
def clean_primary_diag(val):
    return val.split("^")[0] if val else val


# Register UDFs
reformat_dob_udf = udf(reformat_dob, StringType())
simplify_pid_udf = udf(simplify_patient_id, StringType())
clean_sending_facility_udf = udf(clean_sending_facility, StringType())
clean_provider_id_udf = udf(clean_provider_id, StringType())
clean_primary_diag_udf = udf(clean_primary_diag, StringType())

# Load from bronze HL7
df_bronze = spark.read.format("delta").load("file:///C:/Users/sreej/Health_Care_Project/03-processing-delta-spark/bronze/hl7")

# Apply cleaning transformations
df_cleaned = df_bronze \
    .withColumn("dob", reformat_dob_udf(col("dob"))) \
    .withColumn("patient_id", simplify_pid_udf(col("patient_id"))) \
    .withColumn("sending_facility", clean_sending_facility_udf(col("sending_facility"))) \
    .withColumn("provider_id", clean_provider_id_udf(col("provider_id"))) \
    .withColumn("primary_diagnosis_code", clean_primary_diag_udf(col("primary_diagnosis_code")))


# Write to silver_unified HL7
df_cleaned.write.format("delta") \
    .mode("overwrite") \
    .save("file:///C:/Users/sreej/Health_Care_Project/03-processing-delta-spark/silver/hl7")

print(" HL7 cleaned and written to silver_unified/hl7.")

spark.stop()
