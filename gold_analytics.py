# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

silver_customers = spark.table(
    "workspace.default.silver_customers"
)

silver_transactions = spark.table(
    "workspace.default.silver_transactions"
)

print("Silver datasets loaded successfully")

print("Customers:", silver_customers.count())
print("Transactions:", silver_transactions.count())

# COMMAND ----------

customer_transaction_totals = (
    silver_transactions
    .filter(F.col("payment_status") == "SUCCESS")
    .groupBy("customer_id")
    .agg(
        F.sum("amount").alias("total_transaction_amount"),
        F.count("transaction_id").alias("total_transactions")
    )
    .orderBy(F.desc("total_transaction_amount"))
)

display(customer_transaction_totals)

# COMMAND ----------

top_10_customers = (
    customer_transaction_totals
    .join(
        silver_customers.select(
            "customer_id",
            "customer_name",
            "city"
        ),
        on="customer_id",
        how="left"
    )
    .orderBy(F.desc("total_transaction_amount"))
    .limit(10)
)

display(top_10_customers)

# COMMAND ----------

monthly_revenue = (
    silver_transactions
    .filter(F.col("payment_status") == "SUCCESS")
    .withColumn(
        "transaction_month",
        F.date_format("transaction_date", "yyyy-MM")
    )
    .groupBy("transaction_month")
    .agg(
        F.sum("amount").alias("monthly_revenue"),
        F.count("*").alias("total_transactions")
    )
    .orderBy("transaction_month")
)

display(monthly_revenue)

# COMMAND ----------

transaction_status_summary = (
    silver_transactions
    .groupBy("payment_status")
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount")
    )
    .orderBy("payment_status")
)

display(transaction_status_summary)

# COMMAND ----------

city_transaction_analysis = (
    silver_transactions
    .filter(F.col("payment_status") == "SUCCESS")
    .join(
        silver_customers.select(
            "customer_id",
            "city"
        ),
        on="customer_id",
        how="inner"
    )
    .groupBy("city")
    .agg(
        F.avg("amount").alias("average_transaction_value"),
        F.count("*").alias("total_transactions")
    )
    .orderBy(F.desc("average_transaction_value"))
)

display(city_transaction_analysis)

# COMMAND ----------

customers_without_transactions = (
    silver_customers
    .join(
        silver_transactions.select(
            "customer_id"
        ).distinct(),
        on="customer_id",
        how="left_anti"
    )
)

display(customers_without_transactions)

# COMMAND ----------

gold_tables = {
    "gold_customer_totals": customer_transaction_totals,
    "gold_top_10_customers": top_10_customers,
    "gold_monthly_revenue": monthly_revenue,
    "gold_transaction_status": transaction_status_summary,
    "gold_city_analysis": city_transaction_analysis,
    "gold_customers_without_transactions": customers_without_transactions
}

for table_name, dataframe in gold_tables.items():
    dataframe.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"workspace.default.{table_name}")

    print(f"Created: {table_name}")

print("All Gold Delta tables created successfully!")

# COMMAND ----------

for table_name in gold_tables:
    result = spark.table(
        f"workspace.default.{table_name}"
    )

    print(
        table_name,
        "| Rows:",
        result.count()
    )

assert (
    spark.table("workspace.default.gold_top_10_customers").count()
    <= 10
)

assert (
    spark.table("workspace.default.gold_monthly_revenue")
    .filter(F.col("monthly_revenue") < 0)
    .count() == 0
)

print("Gold validation checks passed!")

# COMMAND ----------

silver_total = (
    silver_transactions
    .filter(F.col("payment_status") == "SUCCESS")
    .agg(F.sum("amount").alias("total"))
    .first()["total"]
)

gold_total = (
    spark.table("workspace.default.gold_customer_totals")
    .agg(F.sum("total_transaction_amount").alias("total"))
    .first()["total"]
)

print("Silver successful amount:", silver_total)
print("Gold customer total:", gold_total)

assert silver_total == gold_total

print("Revenue reconciliation passed!")

# COMMAND ----------

display(
    spark.table("workspace.default.rejected_transactions")
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

spark.sql("""
    SHOW TABLES IN workspace.default
""").show(100, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

tables = [
    "bronze_customers",
    "bronze_transactions",
    "silver_customers",
    "silver_transactions",
    "gold_customer_totals",
    "gold_monthly_revenue"
]

for table in tables:
    df = spark.table(f"workspace.default.{table}")
    print(f"{table}: {df.count()} rows")

print("\nSilver duplicate checks:")

for table, key in [
    ("silver_customers", "customer_id"),
    ("silver_transactions", "transaction_id")
]:
    df = spark.table(f"workspace.default.{table}")
    duplicates = (
        df.groupBy(key)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    print(f"{table} duplicate keys: {duplicates}")

# COMMAND ----------

