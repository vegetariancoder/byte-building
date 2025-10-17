# Databricks notebook source
# MAGIC %md
# MAGIC ## Process Orders Data
# MAGIC 1. Ingest the data into the data lakehouse - bronze_orders
# MAGIC 2. Perform data quality checks and transform the data as required - silver_orders_clean
# MAGIC 3. Apply changes to the Addresses data (SCD Type 2) - silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC Import Libraries

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Ingest the data into the data lakehouse - bronze_orders

# COMMAND ----------

@dlt.table(
    name="bronze_orders",  # you can assign a schema name here as well: <schema_name>.bronze_orders
    comment="The orders data ingested from the order's data lakehouse.",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def bronze_orders():
    df_orders = spark \
        .readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.inferSchema", "true") \
        .option("cloudFiles.inferColumnTypes", "true") \
        .option("cloudFiles.schemaLocation", "/Volumes/circuitbox/landing/operational_data/schema/orders/") \
        .load("/Volumes/circuitbox/landing/operational_data/orders/")

    df_orders = df_orders.select(
        "*",
        F.col("_metadata.file_path").alias("input_file_path"),
        F.current_timestamp().alias("ingest_timestamp")
    ) \
        .withColumn("load_date", F.current_date())

    return df_orders

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Perform data quality checks and transform the data as required - silver_orders_clean

# COMMAND ----------

@dlt.table(
    name="silver_orders_clean",
    comment="Cleaned orders data",
    table_properties={'quality': 'silver'}
)
@dlt.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_order_status", "order_status IN ('Pending', 'Shipped', 'Cancelled', 'Completed')")
def create_silver_addresses_clean():
    df_silver_orders_clean = spark.readStream.table("LIVE.bronze_orders") \
        .select(
        "order_id",
        "customer_id",
        F.col("order_timestamp").cast("date"),
        "payment_method",
        "items",
        "order_status"
    ) \
        .withColumn("load_date", F.current_date())
    return df_silver_orders_clean

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Apply changes to the Orders data (SCD Type 2) - silver_orders
# MAGIC Explode the items array from the order object - silver_orders

# COMMAND ----------

@dlt.table(
    name="silver_orders",
    comment="Flattened streaming orders with exploded items"
)
def silver_orders():

    orders_clean = dlt.read_stream("silver_orders_clean")

    exploded_orders = orders_clean.withColumn("item", F.explode(F.col("items")))

    return exploded_orders.select(
        "order_id",
        "customer_id",
        "order_timestamp",
        "payment_method",
        "order_status",
        F.col("item.item_id").alias("item_id"),
        F.col("item.name").alias("item_name"),
        F.col("item.price").alias("item_price"),
        F.col("item.quantity").alias("item_quantity"),
        F.col("item.category").alias("item_category")
    )