# Databricks notebook source
# MAGIC %md
# MAGIC ## Process Addresses Data
# MAGIC 1. Ingest the data into the data lakehouse - bronze_addresses
# MAGIC 2. Perform data quality checks and transform the data as required - silver_addresses_clean
# MAGIC 3. Apply changes to the Addresses data (SCD Type 2) - silver_addresses

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ingest the data into the data lakehouse - bronze_addresses

# COMMAND ----------

@dlt.table(
    name="bronze_addresses",  # you can assign a schema name here as well: <schema_name>.bronze_address
    comment="The address data ingested from the address's data lakehouse.",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def bronze_address():
    df_address = spark \
        .readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", "/Volumes/circuitbox/landing/operational_data/schema/address/") \
        .load("/Volumes/circuitbox/landing/operational_data/address/")

    df_address = df_address.select(
        "*",
        F.col("_metadata.file_path").alias("input_file_path"),
        F.current_timestamp().alias("ingest_timestamp")
    ) \
        .withColumn("load_date", F.current_date())

    return df_address

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Perform data quality checks and transform the data as required - silver_addresses_clean

# COMMAND ----------

@dlt.table(
    name = "silver_addresses_clean",
    comment = "Cleaned addresses data",
    table_properties = {'quality' : 'silver'}
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_address", "address_line_1 IS NOT NULL")
@dlt.expect("valid_postcode", "LENGTH(postcode) = 5")
def create_silver_addresses_clean():
    return (
        spark.readStream.table("LIVE.bronze_addresses")
            .select(
                "customer_id",
                "address_line_1",
                "city",
                "state",
                "postcode",
                F.col("created_date").cast("date")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Apply changes to the Addresses data (SCD Type 2) - silver_addresses

# COMMAND ----------

dlt.create_streaming_table(
    name = "silver_addresses",
    comment = "SCD Type 2 addresses data",
    table_properties = {'quality' : 'silver'}
)

# COMMAND ----------

dlt.apply_changes(
    target = "silver_addresses",
    source = "silver_addresses_clean",
    keys = ["customer_id"],
    sequence_by = "created_date",
    stored_as_scd_type = 2
)