# Databricks notebook source
# MAGIC %md
# MAGIC ### Process the Customers Data
# MAGIC 1. Ingest the data into the data lakehouse - bronze_customers
# MAGIC 2. Perform data quality checks and transform the data as required - silver_customers_clean
# MAGIC 3. Apply changes to the Customers data - silver_customers

# COMMAND ----------

# MAGIC %md
# MAGIC Import Libraries

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ingest the data into the data lakehouse - bronze_customers

# COMMAND ----------

@dlt.table(
    name="bronze_customers",  # you can assign a schema name here as well: <schema_name>.bronze_customers
    comment="The customers data ingested from the customer's data lakehouse.",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
    # path = "location to store delta table"
)
def bronze_customers():
    """Ingest the customer's data into the bronze table."""
    df_customer = spark \
        .readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", "/Volumes/circuitbox/landing/operational_data/schema/customers/") \
        .load("/Volumes/circuitbox/landing/operational_data/customers/")

    df_customer = df_customer.select(
        "*",
        F.col("_metadata.file_path").alias("input_file_path"),
        F.current_timestamp().alias("ingest_timestamp")
    ) \
        .withColumn("load_date", F.current_date())

    return df_customer

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Perform data quality checks and transform the data as required - silver_customers_clean

# COMMAND ----------

@dlt.table(
    name="silver_customers_clean",
    comment="The customers data after data quality checks and transformation.",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL")
@dlt.expect("length_telephone", "LENGTH(telephone)>=10")
@dlt.expect("valid_email", "email IS NOT NULL")
def silver_customers_clean():
    """Perform data quality checks and transform the data as required."""

    df_silver_customers_clean = spark \
        .readStream \
        .table("LIVE.bronze_customers") \
        .select(
        "customer_id",
        "customer_name",
        F.col("date_of_birth").cast("date"),
        "telephone",
        "email",
        F.col("created_date").cast("date"),
        F.current_timestamp().alias("ingest_timestamp")
    )

    return df_silver_customers_clean

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Apply changes to the Customers data - silver_customers

# COMMAND ----------

dlt.create_streaming_table(
    name = "silver_customers",
    comment = "SCD Type 1 customers data",
    table_properties = {'quality' : 'silver'}
)

dlt.apply_changes(
    target="silver_customers",
    source="silver_customers_clean",
    keys=["customer_id"],
    sequence_by="created_date",
    stored_as_scd_type=1
)