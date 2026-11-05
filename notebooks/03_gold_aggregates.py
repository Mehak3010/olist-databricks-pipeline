# Databricks notebook source
from pyspark.sql import functions as F

CATALOG, SCHEMA = "workspace", "olist"
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

def save(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    print(f"{name}: {df.count():,} rows")

orders  = spark.table(T("silver_orders_enriched"))
items   = spark.table(T("silver_order_items"))
products = spark.table(T("silver_products"))
reviews = spark.table(T("silver_reviews"))

# One review score per order (averaged if an order has several)
order_review = reviews.groupBy("order_id").agg(F.avg("review_score").alias("review_score"))

# Only delivered orders count for revenue and delivery KPIs
delivered = orders.filter(F.col("order_status") == "delivered")

# COMMAND ----------

order_value = items.groupBy("order_id").agg(
    F.sum("price").alias("item_revenue"),
    F.sum("freight_value").alias("freight_revenue"),
    F.count("*").alias("items_sold"))

monthly = (delivered.join(order_value, "order_id")
    .withColumn("month", F.date_trunc("month", "order_purchase_timestamp").cast("date"))
    .groupBy("month").agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("item_revenue"), 2).alias("revenue"),
        F.round(F.sum("freight_revenue"), 2).alias("freight"),
        F.round(F.avg("item_revenue"), 2).alias("avg_order_value"))
    .orderBy("month"))
save(monthly, "gold_monthly_revenue")

# COMMAND ----------

category = (items.join(products.select("product_id", "category_english"), "product_id", "left")
    .join(delivered.select("order_id"), "order_id")
    .groupBy("category_english").agg(
        F.round(F.sum("price"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.count("*").alias("items_sold"),
        F.round(F.avg("price"), 2).alias("avg_price"))
    .orderBy(F.desc("revenue")))
save(category, "gold_category_performance")

# COMMAND ----------

state = (delivered.filter(F.col("delivery_days").isNotNull())
    .join(order_review, "order_id", "left")
    .groupBy("customer_state").agg(
        F.count("*").alias("orders"),
        F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
        F.round(F.avg("delay_days"), 1).alias("avg_delay_days"),
        F.round(100 * F.avg(F.col("is_late").cast("int")), 1).alias("pct_late"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"))
    .orderBy(F.desc("avg_delivery_days")))
save(state, "gold_delivery_by_state")

# COMMAND ----------

seller_orders = (items.join(delivered.select("order_id", "is_late"), "order_id")
    .join(order_review, "order_id", "left"))

sellers = (seller_orders.groupBy("seller_id").agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("price"), 2).alias("revenue"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"),
        F.round(100 * F.avg(F.col("is_late").cast("int")), 1).alias("pct_late"))
    .join(spark.table(T("silver_sellers")).select("seller_id", "seller_state"), "seller_id", "left")
    .orderBy(F.desc("revenue")))
save(sellers, "gold_seller_performance")

# COMMAND ----------

# Repeat rate uses customer_unique_id (customer_id changes with every order)
per_customer = (orders.filter(F.col("order_status") != "canceled")
    .groupBy("customer_unique_id").agg(F.countDistinct("order_id").alias("n_orders")))

repeat = per_customer.agg(
    F.count("*").alias("customers"),
    F.sum((F.col("n_orders") > 1).cast("int")).alias("repeat_customers"),
).withColumn("repeat_rate_pct", F.round(100 * F.col("repeat_customers") / F.col("customers"), 2))
save(repeat, "gold_repeat_customers")

# Does lateness hurt review scores?
bucketed = (delivered.filter(F.col("delay_days").isNotNull())
    .join(order_review, "order_id")
    .withColumn("delay_bucket",
        F.when(F.col("delay_days") <= 0, "1. On time or early")
         .when(F.col("delay_days") <= 3, "2. 1-3 days late")
         .when(F.col("delay_days") <= 7, "3. 4-7 days late")
         .otherwise("4. 8+ days late")))
delay_review = (bucketed.groupBy("delay_bucket").agg(
        F.count("*").alias("orders"),
        F.round(F.avg("review_score"), 2).alias("avg_review_score"))
    .orderBy("delay_bucket"))
save(delay_review, "gold_delay_vs_review")

# COMMAND ----------

