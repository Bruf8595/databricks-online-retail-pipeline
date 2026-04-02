# Databricks notebook source
import pyspark.sql.functions as F

#  PARAMETERS 
catalog = "retail_pipeline"
schema = "dev"
volume_path = f"/Volumes/{catalog}/{schema}/raw_data/"
bronze_table = f"{catalog}.{schema}.bronze_online_retail"
schema_location = f"/Volumes/{catalog}/{schema}/_schema/bronze"
checkpoint_path = f"/Volumes/{catalog}/{schema}/_checkpoint/bronze"

#  CREATE REQUIRED VOLUMES 
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}._schema")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}._checkpoint")

print(f"Auto Loader (cloudFiles) is starting on folder: {volume_path}")

#  AUTO LOADER 
df_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(volume_path)
)

#  SANITIZE COLUMN NAMES 
print("Original column names:", df_bronze.columns)

for old_col in df_bronze.columns:
    new_col = (old_col
               .replace(" ", "_")
               .replace(".", "_")
               .replace("(", "")
               .replace(")", "")
               .replace("-", "_")
               .replace("/", "_")
               .replace("'", "")
               .strip("_"))
    if old_col != new_col:
        df_bronze = df_bronze.withColumnRenamed(old_col, new_col)

print("Cleaned column names:", df_bronze.columns)

#  BRONZE METADATA 
df_bronze = df_bronze \
    .withColumn("load_timestamp", F.current_timestamp()) \
    .withColumn("source_file", F.col("_metadata.file_path")) \
    .withColumn("ingestion_date", F.current_date())

# W DELTA TABLE
query = (df_bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(once=True)
    .table(bronze_table)
)

print("Processing files from the folder (one-time batch)...")
query.awaitTermination()

print("\nBronze layer successfully created via Auto Loader!")
print(f"Table name: {bronze_table}")

#  CHECKS 
display(spark.sql(f"SELECT COUNT(*) as row_count FROM {bronze_table}"))
display(spark.table(bronze_table).limit(10))