# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

customers_path = "/Volumes/workspace/default/raw_data/customers.csv"

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(customers_path)
)

print("Customer rows:", customers_df.count())

display(customers_df.limit(5))

# COMMAND ----------

transactions_path = "/Volumes/workspace/default/raw_data/transactions.csv"

transactions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(transactions_path)
)

print("Transaction rows:", transactions_df.count())

display(transactions_df.limit(5))

# COMMAND ----------

customers_bronze = customers_df.withColumn("ingestion_timestamp",F.current_timestamp())

transactions_bronze = transactions_df.withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
)

print("Bronze DataFrames prepared")

# COMMAND ----------

customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_customers")

transactions_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_transactions")

print("Both Bronze Delta tables created successfully!")

# COMMAND ----------

bronze_customers = spark.table(
    "workspace.default.bronze_customers"
)

bronze_transactions = spark.table(
    "workspace.default.bronze_transactions"
)

print("CUSTOMERS BRONZE")
print("Rows:", bronze_customers.count())
print("Columns:", bronze_customers.columns)

print("\nTRANSACTIONS BRONZE")
print("Rows:", bronze_transactions.count())
print("Columns:", bronze_transactions.columns)

display(bronze_customers.limit(5))
display(bronze_transactions.limit(5))

# COMMAND ----------

