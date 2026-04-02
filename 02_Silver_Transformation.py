# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# PARAMETERS 
catalog = "retail_pipeline"
schema = "dev"

bronze_table = f"{catalog}.{schema}.bronze_online_retail"
silver_table = f"{catalog}.{schema}.silver_online_retail"

print(f"Reading Bronze table: {bronze_table}")

# READ BRONZE 
df = spark.table(bronze_table)

print("Initial row count:", df.count())

# CLEANING
# Remove rows with null 
df = df.filter(
    F.col("Invoice").isNotNull() &
    F.col("StockCode").isNotNull() &
    F.col("Quantity").isNotNull() &
    F.col("Price").isNotNull()
)

# Cast types
df = df.withColumn("Quantity", F.col("Quantity").cast("int"))
df = df.withColumn("Price", F.col("Price").cast("double"))

# Convert InvoiceDate to timestamp
df = df.withColumn(
    "InvoiceDate",
    F.to_timestamp("InvoiceDate")
)

# Remove negative or zero values 
df = df.filter(
    (F.col("Quantity") > 0) &
    (F.col("Price") > 0)
)
df = df.drop("rescued_data")
#  DEDUPLICATION 

window_spec = Window.partitionBy(
    "Invoice", "StockCode", "Quantity", "InvoiceDate"
).orderBy(F.col("load_timestamp").desc())

df = df.withColumn("row_num", F.row_number().over(window_spec))
df = df.filter(F.col("row_num") == 1).drop("row_num")

#  ADD DERIVED COLUMNS 
df = df.withColumn(
    "TotalAmount",
    (F.col("Quantity") * F.col("Price")).cast("decimal(10,2)")
)

df = df.withColumn("InvoiceYear", F.year("InvoiceDate"))
df = df.withColumn("InvoiceMonth", F.month("InvoiceDate"))
df = df.withColumn("InvoiceDay", F.dayofmonth("InvoiceDate"))

#  DATA QUALITY CHECK 

print("After cleaning row count:", df.count())

#  WRITE TO SILVER

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(silver_table)

print(f"Silver table created: {silver_table}")

#  FINAL CHECK 

display(spark.sql(f"SELECT COUNT(*) as row_count FROM {silver_table}"))
display(spark.table(silver_table).limit(10))