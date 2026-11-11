# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-End Data Engineering Pipeline Using Databricks
# MAGIC
# MAGIC ## 1. Project Overview
# MAGIC
# MAGIC This project focuses on building an end-to-end data engineering pipeline using Databricks and PySpark. The main objective is to take raw customer and transaction data, process it through different stages, and prepare meaningful datasets that can be used for business analysis.
# MAGIC
# MAGIC The project follows the Medallion Architecture, which divides the data processing workflow into three layers: Bronze, Silver, and Gold.
# MAGIC
# MAGIC - **Bronze Layer:** Loads and stores the original CSV data.
# MAGIC - **Silver Layer:** Cleans the data, handles missing values, removes duplicates, and validates records.
# MAGIC - **Gold Layer:** Uses the cleaned data to generate business-level analytical results.
# MAGIC
# MAGIC The entire process is organized into three notebooks and automated using Databricks Jobs, allowing the pipeline to execute in the correct sequence.
# MAGIC
# MAGIC ## 2. Project Objectives
# MAGIC
# MAGIC The main objectives of this assignment are:
# MAGIC
# MAGIC - Understand and implement the Medallion Architecture in Databricks.
# MAGIC - Ingest customer and transaction data from CSV files.
# MAGIC - Store raw data as Delta tables in the Bronze layer.
# MAGIC - Perform data cleaning and validation using PySpark.
# MAGIC - Identify and separate invalid and duplicate records.
# MAGIC - Create analytical tables to answer business-related questions.
# MAGIC - Automate notebook execution using Databricks Jobs.
# MAGIC - Validate the output and check the consistency of the processed data.
# MAGIC
# MAGIC ## 3. Architecture
# MAGIC
# MAGIC The project follows a three-layer data processing approach.
# MAGIC
# MAGIC **Data Flow:**
# MAGIC
# MAGIC CSV Files → Bronze Layer → Silver Layer → Gold Layer → Business Analytics
# MAGIC
# MAGIC ### Architecture Components
# MAGIC
# MAGIC **Source Data**
# MAGIC - customers.csv
# MAGIC - transactions.csv
# MAGIC
# MAGIC **Bronze Layer**
# MAGIC - bronze_customers
# MAGIC - bronze_transactions
# MAGIC
# MAGIC **Silver Layer**
# MAGIC - silver_customers
# MAGIC - silver_transactions
# MAGIC - rejected_customers
# MAGIC - rejected_transactions
# MAGIC
# MAGIC **Gold Layer**
# MAGIC - gold_customer_totals
# MAGIC - gold_top_10_customers
# MAGIC - gold_monthly_revenue
# MAGIC - gold_transaction_status
# MAGIC - gold_city_analysis
# MAGIC - gold_customers_without_transactions
# MAGIC
# MAGIC Each layer has a separate responsibility. This makes the pipeline easier to understand, maintain, and validate.
# MAGIC
# MAGIC ## 4. Dataset Description
# MAGIC
# MAGIC Two CSV files were prepared for this project.
# MAGIC
# MAGIC ### Customers Dataset
# MAGIC
# MAGIC The customers dataset contains customer-related information.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | customer_id | Unique identifier for each customer |
# MAGIC | customer_name | Name of the customer |
# MAGIC | email | Customer email address |
# MAGIC | city | Customer's city |
# MAGIC | registration_date | Date when the customer registered |
# MAGIC
# MAGIC ### Transactions Dataset
# MAGIC
# MAGIC The transactions dataset contains information about customer transactions.
# MAGIC
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | transaction_id | Unique identifier for each transaction |
# MAGIC | customer_id | Customer associated with the transaction |
# MAGIC | transaction_date | Date of the transaction |
# MAGIC | amount | Transaction amount |
# MAGIC | payment_status | Status of the transaction |
# MAGIC
# MAGIC ### Data Quality Scenarios
# MAGIC
# MAGIC To test the data cleaning process, the datasets include different data quality issues, such as:
# MAGIC
# MAGIC - Duplicate customer and transaction records.
# MAGIC - Missing customer names and email addresses.
# MAGIC - Missing or invalid transaction amounts.
# MAGIC - Invalid date values.
# MAGIC - Negative and zero transaction amounts.
# MAGIC - Inconsistent city names and formatting.
# MAGIC - Transactions associated with customer IDs that do not exist.
# MAGIC - Invalid payment status values.
# MAGIC
# MAGIC These scenarios help verify whether the pipeline can identify and handle common data quality problems.
# MAGIC
# MAGIC ## 5. Bronze Layer – Data Ingestion
# MAGIC
# MAGIC The Bronze layer is the first stage of the pipeline. Its purpose is to load the source CSV files into Databricks and preserve the original data for further processing.
# MAGIC
# MAGIC ### Implementation
# MAGIC
# MAGIC The two CSV files were uploaded to a Unity Catalog Volume:
# MAGIC
# MAGIC `/Volumes/workspace/default/raw_data/`
# MAGIC
# MAGIC The files were then read using PySpark and stored as Delta tables.
# MAGIC
# MAGIC ### Tables Created
# MAGIC
# MAGIC - `workspace.default.bronze_customers`
# MAGIC - `workspace.default.bronze_transactions`
# MAGIC
# MAGIC An ingestion timestamp was also added to identify when records entered the Bronze layer.
# MAGIC
# MAGIC ### Ingestion Results
# MAGIC
# MAGIC | Table | Records |
# MAGIC |---|---:|
# MAGIC | bronze_customers | 151 |
# MAGIC | bronze_transactions | 301 |
# MAGIC
# MAGIC The Bronze layer retains the source records, including the intentional duplicates and invalid values. Data cleaning is performed in the next layer.
# MAGIC
# MAGIC ## 6. Silver Layer – Data Transformation
# MAGIC
# MAGIC The Silver layer is where the actual data cleaning and validation take place.
# MAGIC
# MAGIC The Bronze tables are used as input, and PySpark transformations are applied to prepare reliable datasets.
# MAGIC
# MAGIC ### Customer Data Processing
# MAGIC
# MAGIC The following operations were performed:
# MAGIC
# MAGIC - Removed duplicate customer records using `customer_id`.
# MAGIC - Handled missing customer names and email addresses according to the cleaning rules.
# MAGIC - Standardized email addresses using lowercase conversion and trimming.
# MAGIC - Cleaned city names by removing unnecessary spaces and applying consistent capitalization.
# MAGIC - Converted registration dates into the appropriate date format.
# MAGIC - Separated records that failed validation into a rejected customers table.
# MAGIC
# MAGIC ### Transaction Data Processing
# MAGIC
# MAGIC The following operations were performed:
# MAGIC
# MAGIC - Removed duplicate transactions using `transaction_id`.
# MAGIC - Converted transaction amounts into decimal format.
# MAGIC - Converted transaction dates into the correct date format.
# MAGIC - Standardized payment status values using trimming and uppercase conversion.
# MAGIC - Identified missing or invalid transaction amounts.
# MAGIC - Rejected zero and negative transaction amounts.
# MAGIC - Checked whether each transaction belonged to an existing customer.
# MAGIC - Identified invalid payment status values.
# MAGIC - Stored rejected records separately along with their rejection reasons.
# MAGIC
# MAGIC ### Tables Created
# MAGIC
# MAGIC **Valid records:**
# MAGIC - `workspace.default.silver_customers`
# MAGIC - `workspace.default.silver_transactions`
# MAGIC
# MAGIC **Rejected records:**
# MAGIC - `workspace.default.rejected_customers`
# MAGIC - `workspace.default.rejected_transactions`
# MAGIC
# MAGIC ### Silver Layer Results
# MAGIC
# MAGIC | Table | Records |
# MAGIC |---|---:|
# MAGIC | silver_customers | 147 |
# MAGIC | silver_transactions | 288 |
# MAGIC
# MAGIC The rejected transactions were also reviewed to understand why records failed validation.
# MAGIC
# MAGIC ### Transaction Rejection Summary
# MAGIC
# MAGIC | Rejection Reason | Count |
# MAGIC |---|---:|
# MAGIC | Customer ID does not exist | 7 |
# MAGIC | Transaction amount must be positive | 2 |
# MAGIC | Missing or invalid amount | 1 |
# MAGIC | Invalid transaction date | 1 |
# MAGIC | Invalid payment status | 1 |
# MAGIC | Duplicate transaction ID | 1 |
# MAGIC | **Total** | **13** |
# MAGIC
# MAGIC Keeping rejected records separately makes it easier to investigate data quality issues without mixing invalid records with the cleaned datasets.
# MAGIC
# MAGIC ## 7. Gold Layer – Analytics
# MAGIC
# MAGIC The Gold layer contains the final datasets prepared for business analysis and reporting.
# MAGIC
# MAGIC The cleaned Silver tables were used to answer six business questions using PySpark DataFrame operations.
# MAGIC
# MAGIC ### 1. Total Transaction Amount per Customer
# MAGIC
# MAGIC Calculated the total successful transaction amount for each customer.
# MAGIC
# MAGIC Output table: `gold_customer_totals`
# MAGIC
# MAGIC ### 2. Top 10 Customers by Spending
# MAGIC
# MAGIC Identified the ten customers with the highest successful transaction spending.
# MAGIC
# MAGIC Output table: `gold_top_10_customers`
# MAGIC
# MAGIC ### 3. Monthly Transaction Revenue
# MAGIC
# MAGIC Calculated transaction revenue month by month to understand revenue trends.
# MAGIC
# MAGIC Output table: `gold_monthly_revenue`
# MAGIC
# MAGIC ### 4. Transaction Status Summary
# MAGIC
# MAGIC Calculated the number of transactions for each payment status, including successful, failed, and pending transactions.
# MAGIC
# MAGIC Output table: `gold_transaction_status`
# MAGIC
# MAGIC ### 5. Average Transaction Value by City
# MAGIC
# MAGIC Calculated the average successful transaction value for each city using customer and transaction information.
# MAGIC
# MAGIC Output table: `gold_city_analysis`
# MAGIC
# MAGIC ### 6. Customers Without Transactions
# MAGIC
# MAGIC Identified customers who do not have any matching transaction records.
# MAGIC
# MAGIC Output table: `gold_customers_without_transactions`
# MAGIC
# MAGIC ### Gold Layer Results
# MAGIC
# MAGIC | Gold Table | Records |
# MAGIC |---|---:|
# MAGIC | gold_customer_totals | 64 |
# MAGIC | gold_top_10_customers | 10 |
# MAGIC | gold_monthly_revenue | 9 |
# MAGIC | gold_transaction_status | 3 |
# MAGIC | gold_city_analysis | 6 |
# MAGIC | gold_customers_without_transactions | 24 |
# MAGIC
# MAGIC All six analytical tables were successfully created and stored as Delta tables in the workspace.
# MAGIC
# MAGIC ## 8. Workflow Automation
# MAGIC
# MAGIC After completing the three individual notebooks, a Databricks Job was created to automate the complete pipeline.
# MAGIC
# MAGIC The job contains three notebook tasks with dependencies configured in the required execution order.
# MAGIC
# MAGIC ### Job Tasks
# MAGIC
# MAGIC | Task | Notebook | Dependency |
# MAGIC |---|---|---|
# MAGIC | bronze_ingestion | bronze_ingestion | None |
# MAGIC | silver_transformations | silver_transformations | bronze_ingestion |
# MAGIC | gold_analytics | gold_analytics | silver_transformations |
# MAGIC
# MAGIC ### Execution Flow
# MAGIC
# MAGIC Bronze Ingestion → Silver Transformations → Gold Analytics
# MAGIC
# MAGIC The dependency configuration ensures that:
# MAGIC
# MAGIC - Silver starts only after Bronze completes successfully.
# MAGIC - Gold starts only after Silver completes successfully.
# MAGIC - If an upstream task fails, its dependent task will not proceed.
# MAGIC
# MAGIC ### Execution Result
# MAGIC
# MAGIC The Databricks Job was executed successfully.
# MAGIC
# MAGIC All three notebook tasks completed with a **Succeeded** status.
# MAGIC
# MAGIC The tasks used Serverless compute in Databricks Free Edition.
# MAGIC
# MAGIC This automation makes it possible to execute the complete data processing workflow from a single job instead of running each notebook manually.
# MAGIC
# MAGIC ## 9. Data Quality and Validation
# MAGIC
# MAGIC Data validation was performed at different stages to make sure the processed data was consistent.
# MAGIC
# MAGIC The following checks were carried out:
# MAGIC
# MAGIC - Verified that the Bronze tables were created successfully.
# MAGIC - Checked duplicate customer IDs and transaction IDs in the Silver tables.
# MAGIC - Verified that invalid transaction amounts and dates were rejected.
# MAGIC - Checked transactions against the available customer records.
# MAGIC - Reviewed rejected records and their rejection reasons.
# MAGIC - Verified that all six Gold analytical tables were created.
# MAGIC - Compared the total successful transaction amount in Silver with the corresponding Gold customer totals.
# MAGIC - Executed the workflow again and checked the resulting table counts.
# MAGIC
# MAGIC ### Gold Data Reconciliation
# MAGIC
# MAGIC The total successful transaction amount in Silver was compared with the sum of customer transaction totals in Gold.
# MAGIC
# MAGIC | Metric | Amount |
# MAGIC |---|---:|
# MAGIC | Silver successful transaction amount | 443,867.77 |
# MAGIC | Gold customer total | 443,867.77 |
# MAGIC | Difference | 0.00 |
# MAGIC
# MAGIC **Result:** Revenue reconciliation passed.
# MAGIC
# MAGIC ### Duplicate Validation
# MAGIC
# MAGIC The following checks were performed after workflow execution:
# MAGIC
# MAGIC | Table | Duplicate Primary Keys |
# MAGIC |---|---:|
# MAGIC | silver_customers | 0 |
# MAGIC | silver_transactions | 0 |
# MAGIC
# MAGIC The Bronze table counts also remained at 151 customer records and 301 transaction records during the observed rerun validation.
# MAGIC
# MAGIC These results confirm that the Silver tables contain unique primary keys and that the Gold revenue totals match the successful Silver transaction amount for the tested dataset.
# MAGIC
# MAGIC ## 10. Execution Instructions
# MAGIC
# MAGIC The project can be executed using the following steps.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - Access to a Databricks workspace.
# MAGIC - Access to Unity Catalog.
# MAGIC - The two source CSV files.
# MAGIC - The three project notebooks.
# MAGIC
# MAGIC ### Execution Steps
# MAGIC
# MAGIC **Step 1: Upload Source Files**
# MAGIC
# MAGIC Upload `customers.csv` and `transactions.csv` to the following Unity Catalog Volume:
# MAGIC
# MAGIC `/Volumes/workspace/default/raw_data/`
# MAGIC
# MAGIC **Step 2: Run Bronze Notebook**
# MAGIC
# MAGIC Open `bronze_ingestion` and execute the notebook to read the CSV files and create the Bronze Delta tables.
# MAGIC
# MAGIC **Step 3: Run Silver Notebook**
# MAGIC
# MAGIC Open `silver_transformations` and execute the notebook to clean the data, validate records, and create the Silver and rejected-record tables.
# MAGIC
# MAGIC **Step 4: Run Gold Notebook**
# MAGIC
# MAGIC Open `gold_analytics` and execute the notebook to generate the six Gold analytical tables.
# MAGIC
# MAGIC **Step 5: Run the Databricks Job**
# MAGIC
# MAGIC Open Jobs & Pipelines, select the configured job, and click **Run now**.
# MAGIC
# MAGIC The three notebooks will execute automatically according to their configured dependencies.
# MAGIC
# MAGIC **Step 6: Verify Results**
# MAGIC
# MAGIC Check the table records, rejected data, reconciliation results, and task execution status.
# MAGIC
# MAGIC ## 11. Final Results
# MAGIC
# MAGIC The project successfully demonstrates an end-to-end data engineering workflow using Databricks and PySpark.
# MAGIC
# MAGIC The final implementation includes:
# MAGIC
# MAGIC - Two CSV source datasets.
# MAGIC - Two Bronze Delta tables.
# MAGIC - Two cleaned Silver Delta tables.
# MAGIC - Two rejected-record tables.
# MAGIC - Six Gold analytical tables.
# MAGIC - Three separate Databricks notebooks.
# MAGIC - One automated Databricks Job with sequential task dependencies.
# MAGIC - Data quality checks and revenue reconciliation.
# MAGIC
# MAGIC All three notebook tasks completed successfully during workflow execution.
# MAGIC
# MAGIC The revenue reconciliation returned matching totals of 443,867.77, and the Silver primary-key duplicate checks returned zero duplicates.
# MAGIC
# MAGIC ## 12. Conclusion
# MAGIC
# MAGIC Working on this project provided practical experience with the different stages involved in building a data engineering pipeline.
# MAGIC
# MAGIC I worked with CSV ingestion, PySpark transformations, Delta tables, data validation, analytical queries, and workflow automation in Databricks.
# MAGIC
# MAGIC One of the important parts of the implementation was handling data quality issues. Instead of simply removing invalid records, I separated rejected records and maintained rejection reasons so that they could be reviewed later.
# MAGIC
# MAGIC The project also helped me understand how the Bronze, Silver, and Gold layers work together to transform raw data into useful business information.
# MAGIC
# MAGIC Overall, this assignment gave me hands-on experience in building and validating a complete data processing workflow using Databricks.

# COMMAND ----------

