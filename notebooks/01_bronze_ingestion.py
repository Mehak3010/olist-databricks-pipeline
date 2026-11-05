# Databricks notebook source
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "olist"
BASE = f"/Volumes/{CATALOG}/{SCHEMA}/raw"

files = {
    "customers":    "olist_customers_dataset.csv",
    "geolocation":  "olist_geolocation_dataset.csv",
    "order_items":  "olist_order_items_dataset.csv",
    "payments":     "olist_order_payments_dataset.csv",
    "reviews":      "olist_order_reviews_dataset.csv",
    "orders":       "olist_orders_dataset.csv",
    "products":     "olist_products_dataset.csv",
    "sellers":      "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

for name, file in files.items():
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)   # reviews contain line breaks
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{BASE}/{file}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    (df.write.format("delta")
       .mode("overwrite")
       .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_{name}"))
    print(f"bronze_{name}: {df.count():,} rows")