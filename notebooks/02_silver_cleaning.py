# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG, SCHEMA = "workspace", "olist"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"
bronze = lambda n: spark.table(T(f"bronze_{n}"))

dq_results = []  # collects data quality stats

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))

def split_and_save(df, rule, name, reason):
    """Valid rows -> silver table, failing rows -> quarantine table, stats -> log."""
    rule = F.coalesce(rule, F.lit(False))
    good, bad = df.filter(rule), df.filter(~rule)
    save(good, name)
    save(bad.withColumn("_dq_reason", F.lit(reason)), f"quarantine_{name}")
    stats = (name, df.count(), good.count(), bad.count(), reason)
    dq_results.append(stats)
    print(f"{name}: {stats[2]:,} valid, {stats[3]:,} quarantined")

# COMMAND ----------

ts_cols = ["order_purchase_timestamp", "order_approved_at",
           "order_delivered_carrier_date", "order_delivered_customer_date",
           "order_estimated_delivery_date"]

orders = bronze("orders").dropDuplicates(["order_id"])
for c in ts_cols:
    orders = orders.withColumn(c, F.expr(f"try_to_timestamp({c})"))

rule = (
    F.col("order_id").isNotNull() & F.col("customer_id").isNotNull() &
    (F.col("order_delivered_customer_date").isNull() |
     (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp")))
)
split_and_save(orders.drop("_ingested_at", "_source_file"), rule, "silver_orders",
               "null id or delivered before purchase")

# COMMAND ----------

customers = (bronze("customers").dropDuplicates(["customer_id"])
    .withColumn("customer_city", F.initcap(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state")))
    .drop("_ingested_at", "_source_file"))
split_and_save(customers, F.col("customer_id").isNotNull(), "silver_customers", "null customer_id")

sellers = (bronze("sellers").dropDuplicates(["seller_id"])
    .withColumn("seller_city", F.initcap(F.trim("seller_city")))
    .withColumn("seller_state", F.upper(F.trim("seller_state")))
    .drop("_ingested_at", "_source_file"))
split_and_save(sellers, F.col("seller_id").isNotNull(), "silver_sellers", "null seller_id")

# Note: the source CSV misspells "lenght" - we fix it here
products = (bronze("products").dropDuplicates(["product_id"])
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length"))
for c in ["product_name_length", "product_description_length", "product_photos_qty",
          "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
    products = products.withColumn(c, F.expr(f"try_cast({c} as double)"))
products = (products
    .join(bronze("category_translation"), "product_category_name", "left")
    .withColumn("category_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")))
    .drop("product_category_name_english", "_ingested_at", "_source_file"))
split_and_save(products, F.col("product_id").isNotNull(), "silver_products", "null product_id")

# COMMAND ----------

items = (bronze("order_items")
    .withColumn("price", F.expr("try_cast(price as double)"))
    .withColumn("freight_value", F.expr("try_cast(freight_value as double)"))
    .withColumn("order_item_id", F.expr("try_cast(order_item_id as int)"))
    .withColumn("shipping_limit_date", F.expr("try_to_timestamp(shipping_limit_date)"))
    .dropDuplicates(["order_id", "order_item_id"])
    .drop("_ingested_at", "_source_file"))
split_and_save(items, (F.col("price") > 0) & F.col("order_id").isNotNull(),
               "silver_order_items", "price <= 0 or null order_id")

payments = (bronze("payments")
    .withColumn("payment_value", F.expr("try_cast(payment_value as double)"))
    .withColumn("payment_installments", F.expr("try_cast(payment_installments as int)"))
    .withColumn("payment_sequential", F.expr("try_cast(payment_sequential as int)"))
    .dropDuplicates(["order_id", "payment_sequential"])
    .drop("_ingested_at", "_source_file"))
split_and_save(payments, (F.col("payment_value") > 0) & F.col("order_id").isNotNull(),
               "silver_payments", "payment_value <= 0 or null order_id")

# Reviews: some review_ids repeat across orders; keep the latest answer per (review_id, order_id)
w = Window.partitionBy("review_id", "order_id").orderBy(F.col("review_answer_timestamp").desc())
reviews = (bronze("reviews")
    .withColumn("review_score", F.expr("try_cast(review_score as int)"))
    .withColumn("review_creation_date", F.expr("try_to_timestamp(review_creation_date)"))
    .withColumn("review_answer_timestamp", F.expr("try_to_timestamp(review_answer_timestamp)"))
    .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    .drop("_ingested_at", "_source_file"))
split_and_save(reviews, F.col("review_score").between(1, 5), "silver_reviews",
               "review_score outside 1-5")

# COMMAND ----------

orders_enriched = (spark.table(T("silver_orders"))
    .join(spark.table(T("silver_customers")), "customer_id", "left")
    .withColumn("delivery_days",
        F.datediff("order_delivered_customer_date", "order_purchase_timestamp"))
    .withColumn("delay_days",
        F.datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
    .withColumn("is_late", F.col("delay_days") > 0))
save(orders_enriched, "silver_orders_enriched")

dq_df = (spark.createDataFrame(dq_results,
        ["table", "rows_in", "rows_valid", "rows_quarantined", "rule"])
    .withColumn("run_at", F.current_timestamp()))
dq_df.write.format("delta").mode("append").saveAsTable(T("dq_log"))
display(dq_df)

# COMMAND ----------

