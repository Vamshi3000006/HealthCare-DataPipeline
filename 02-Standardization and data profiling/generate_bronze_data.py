# Add this at the end of standardize_columns.py
if __name__ == "__main__":
    import sys
    import json
    from pyspark.sql import SparkSession

    source_type = sys.argv[1]

    with open("../02-Standardization and data profiling/unify-config.json", "r") as f:
        config = json.load(f)

    input_path = config["mappings"][source_type]["path"]
    column_mapping = config["mappings"][source_type]["standard_column_names"]

    spark = SparkSession.builder.appName("StandardizeColumns").getOrCreate()

    if source_type == "csv":
        df = spark.read.option("header", True).csv(input_path)
    elif source_type == "api":
        df = spark.read.option("multiline", True).json(input_path)
    elif source_type == "hl7":
        df = spark.read.format("delta").load(input_path)
    else:
        raise ValueError("Unsupported source type")

    for standard_col, source_col in column_mapping.items():
        if source_col in df.columns:
            df = df.withColumnRenamed(source_col, standard_col)

    df.write.format("delta").mode("overwrite").save(f"../03-processing-delta-spark/bronze/{source_type}")
    spark.stop()
