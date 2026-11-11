# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Silver transformation started")

# COMMAND ----------

customers_bronze = spark.table(
    "workspace.default.bronze_customers"
)

transactions_bronze = spark.table(
    "workspace.default.bronze_transactions"
)

print("Bronze tables loaded")

# COMMAND ----------

customers = (
    customers_bronze
    .withColumn(
        "customer_name",
        F.trim(F.col("customer_name"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )
    .withColumn(
        "registration_date",
        F.expr("try_cast(registration_date AS DATE)")
    )
)

# COMMAND ----------

customer_window = Window.partitionBy(
    "customer_id"
).orderBy(F.col("ingestion_timestamp").asc())

customers_ranked = customers.withColumn(
    "row_num",
    F.row_number().over(customer_window)
)

duplicate_customers = customers_ranked.filter(
    F.col("row_num") > 1
).drop("row_num")

unique_customers = customers_ranked.filter(
    F.col("row_num") == 1
).drop("row_num")

# COMMAND ----------

customers_checked = unique_customers.withColumn(
    "rejection_reason",
    F.when(
        F.col("customer_id").isNull(),
        "Missing customer ID"
    ).when(
        F.col("customer_name").isNull()
        | (F.col("customer_name") == ""),
        "Missing customer name"
    ).when(
        F.col("email").isNull()
        | (F.col("email") == ""),
        "Missing email"
    ).when(
        F.col("registration_date").isNull(),
        "Missing or invalid registration date"
    )
)

valid_customers = customers_checked.filter(
    F.col("rejection_reason").isNull()
).drop("rejection_reason")

rejected_customers = customers_checked.filter(
    F.col("rejection_reason").isNotNull()
)

# COMMAND ----------

duplicate_customers = duplicate_customers.withColumn(
    "rejection_reason",
    F.lit("Duplicate customer ID")
)

rejected_customers = rejected_customers.unionByName(
    duplicate_customers,
    allowMissingColumns=True
)


# COMMAND ----------

print("Valid customers:", valid_customers.count())
print("Rejected customers:", rejected_customers.count())

# COMMAND ----------

transactions = (
    transactions_bronze
    .withColumn(
        "amount",
        F.expr("try_cast(amount AS DECIMAL(12,2))")
    )
    .withColumn(
        "transaction_date",
        F.expr("try_cast(transaction_date AS DATE)")
    )
    .withColumn(
        "payment_status",
        F.upper(F.trim(F.col("payment_status")))
    )
)

# Identify duplicate transaction IDs
transaction_window = Window.partitionBy(
    "transaction_id"
).orderBy(F.col("ingestion_timestamp").asc())

transactions_ranked = transactions.withColumn(
    "row_num",
    F.row_number().over(transaction_window)
)

duplicate_transactions = transactions_ranked.filter(
    F.col("row_num") > 1
).drop("row_num")

unique_transactions = transactions_ranked.filter(
    F.col("row_num") == 1
).drop("row_num")

# Check customer references
known_customers = valid_customers.select(
    "customer_id"
).distinct()

transactions_checked = unique_transactions.join(
    known_customers.withColumn(
        "_customer_exists", F.lit(True)
    ),
    on="customer_id",
    how="left"
)

# Assign rejection reasons
transactions_checked = transactions_checked.withColumn(
    "rejection_reason",
    F.when(
        F.col("transaction_id").isNull(),
        "Missing transaction ID"
    ).when(
        F.col("amount").isNull(),
        "Missing or invalid amount"
    ).when(
        F.col("amount") <= 0,
        "Transaction amount must be positive"
    ).when(
        F.col("transaction_date").isNull(),
        "Invalid transaction date"
    ).when(
        F.col("_customer_exists").isNull(),
        "Customer ID does not exist"
    ).when(
        ~F.col("payment_status").isin(
            "SUCCESS", "FAILED", "PENDING"
        ),
        "Invalid payment status"
    )
)

# Step 4: Separate valid and rejected transactions
valid_transactions = transactions_checked.filter(
    F.col("rejection_reason").isNull()
).select(
    "transaction_id",
    "customer_id",
    "transaction_date",
    "amount",
    "payment_status",
    "ingestion_timestamp"
)

rejected_transactions = transactions_checked.filter(
    F.col("rejection_reason").isNotNull()
)

# Include duplicate records in rejection table
duplicate_transactions = duplicate_transactions.withColumn(
    "rejection_reason",
    F.lit("Duplicate transaction ID")
)

rejected_transactions = rejected_transactions.unionByName(
    duplicate_transactions,
    allowMissingColumns=True
)

print("Valid transactions:", valid_transactions.count())
print("Rejected transactions:", rejected_transactions.count())

# COMMAND ----------

valid_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_transactions")

rejected_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rejected_transactions")

# COMMAND ----------

silver_transactions = spark.table(
    "workspace.default.silver_transactions"
)

rejected_transactions = spark.table(
    "workspace.default.rejected_transactions"
)

# duplicate transaction IDs
duplicate_count = (
    silver_transactions
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# invalid amounts
invalid_amount_count = silver_transactions.filter(
    (F.col("amount").isNull()) |
    (F.col("amount") <= 0)
).count()

# invalid dates
invalid_date_count = silver_transactions.filter(
    F.col("transaction_date").isNull()
).count()

assert duplicate_count == 0
assert invalid_amount_count == 0
assert invalid_date_count == 0

print("All transaction validation checks passed!")
print("Valid transactions:", silver_transactions.count())
print("Rejected transactions:", rejected_transactions.count())

# COMMAND ----------

display(
    rejected_transactions
    .groupBy("rejection_reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Step 1: Read Bronze customers
customers = spark.table(
    "workspace.default.bronze_customers"
)

# Step 2: Standardize customer fields
customers = (
    customers
    .withColumn("customer_id", F.trim("customer_id"))
    .withColumn("customer_name", F.trim("customer_name"))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn(
        "registration_date",
        F.expr("try_cast(registration_date AS DATE)")
    )
)

# Step 3: Identify duplicate customer IDs
customer_window = Window.partitionBy(
    "customer_id"
).orderBy(F.col("ingestion_timestamp").asc())

customers_ranked = customers.withColumn(
    "row_num",
    F.row_number().over(customer_window)
)

duplicate_customers = customers_ranked.filter(
    F.col("row_num") > 1
).drop("row_num").withColumn(
    "rejection_reason",
    F.lit("Duplicate customer ID")
)

unique_customers = customers_ranked.filter(
    F.col("row_num") == 1
).drop("row_num")

# Step 4: Assign rejection reasons
customers_checked = unique_customers.withColumn(
    "rejection_reason",
    F.when(
        F.col("customer_id").isNull()
        | (F.col("customer_id") == ""),
        "Missing customer ID"
    ).when(
        F.col("customer_name").isNull()
        | (F.col("customer_name") == ""),
        "Missing customer name"
    ).when(
        F.col("email").isNull()
        | (F.col("email") == ""),
        "Missing email address"
    ).when(
        F.col("registration_date").isNull(),
        "Invalid registration date"
    )
)

# Step 5: Separate valid and rejected customers
silver_customers = customers_checked.filter(
    F.col("rejection_reason").isNull()
).drop("rejection_reason")

rejected_customers = customers_checked.filter(
    F.col("rejection_reason").isNotNull()
)

# Include duplicate records in rejected customers
rejected_customers = rejected_customers.unionByName(
    duplicate_customers,
    allowMissingColumns=True
)

print("Valid customers:", silver_customers.count())
print("Rejected customers:", rejected_customers.count())

# COMMAND ----------

silver_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_customers")

rejected_customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.rejected_customers")

print("Customer Silver tables created successfully!")

# COMMAND ----------

spark.sql("""
    SHOW TABLES IN workspace.default
""").show(truncate=False)

# COMMAND ----------

display(
    spark.table("workspace.default.silver_customers")
)

# COMMAND ----------

silver_customers = spark.table(
    "workspace.default.silver_customers"
)

silver_transactions = spark.table(
    "workspace.default.silver_transactions"
)

print("Both Silver tables loaded successfully!")

# COMMAND ----------

