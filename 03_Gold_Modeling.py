# Databricks notebook source
import pyspark.sql.functions as F

#  PARAMETERS 
catalog = "retail_pipeline"
schema = "dev"

silver_table = f"{catalog}.{schema}.silver_online_retail"

fact_table = f"{catalog}.{schema}.fact_sales"
dim_customer = f"{catalog}.{schema}.dim_customer"
dim_product = f"{catalog}.{schema}.dim_product"
dim_date = f"{catalog}.{schema}.dim_date"

print(f"Reading Silver table: {silver_table}")

df = spark.table(silver_table)

#  DIM CUSTOMER 
df_customer = df.select("Customer_ID", "Country") \
    .filter(F.col("Customer_ID").isNotNull()) \
    .dropDuplicates()

df_customer = df_customer.withColumn(
    "customer_key",
    F.monotonically_increasing_id()
)

df_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(dim_customer)

print("dim_customer created")

#  DIM PRODUCT 
df_product = df.select("StockCode", "Description") \
    .dropDuplicates()

df_product = df_product.withColumn(
    "product_key",
    F.monotonically_increasing_id()
)

df_product.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(dim_product)

print("dim_product created")

#  DIM DATE 
df_date = df.select("InvoiceDate").dropDuplicates()

df_date = df_date.withColumn("date_key", F.date_format("InvoiceDate", "yyyyMMdd").cast("int")) \
    .withColumn("year", F.year("InvoiceDate")) \
    .withColumn("month", F.month("InvoiceDate")) \
    .withColumn("day", F.dayofmonth("InvoiceDate"))

df_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(dim_date)

print("dim_date created")

#  FACT TABLE 

# Reload dimensions 
dim_cust_df = spark.table(dim_customer)
dim_prod_df = spark.table(dim_product)
dim_date_df = spark.table(dim_date)

fact_df = df \
    .join(dim_cust_df, ["Customer_ID", "Country"], "left") \
    .join(dim_prod_df, ["StockCode", "Description"], "left") \
    .join(dim_date_df, ["InvoiceDate"], "left")

fact_df = fact_df.select(
    "customer_key",
    "product_key",
    "date_key",
    "Invoice",
    "Quantity",
    "Price",
    "TotalAmount"
)

fact_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(fact_table)

print("fact_sales created")

#  FINAL CHECK 

display(spark.sql(f"SELECT COUNT(*) FROM {fact_table}"))
display(spark.table(fact_table).limit(10))