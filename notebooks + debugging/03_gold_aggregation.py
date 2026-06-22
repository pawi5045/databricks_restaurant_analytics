# Databricks notebook source
# these tools help us count, sum, and average things
from pyspark.sql.functions import countDistinct, sum, avg, hour, col

# load the enriched silver data, this is our starting point for all gold tables
df_enriched = spark.read.table("ws_dbxproject.`02_silver`.orders_enriched")

# COMMAND ----------

# gold table 1: total revenue per restaurant
# this answers "which restaurant is making the most money"
df_gold_revenue_by_restaurant = (
    df_enriched
    .groupBy("restaurant_id", "restaurant_name", "restaurant_city", "country")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("subtotal").alias("total_revenue"),
        avg("subtotal").alias("avg_item_revenue")
    )
)

df_gold_revenue_by_restaurant.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.revenue_by_restaurant")

# COMMAND ----------

# gold table 2: how many orders happen each hour
# this answers "what are our busiest hours"
df_gold_orders_by_hour = (
    df_enriched
    .withColumn("order_hour", hour("order_timestamp"))
    .groupBy("order_hour")
    .agg(countDistinct("order_id").alias("total_orders"))
    .orderBy("order_hour")
)

df_gold_orders_by_hour.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.orders_by_hour")

# COMMAND ----------

# gold table 3: how much each customer orders and spends
# this answers "who are our most loyal customers"
df_gold_customer_patterns = (
    df_enriched
    .groupBy("customer_id", "customer_name", "customer_city")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("subtotal").alias("total_spend")
    )
    .orderBy(col("total_orders").desc())
)

df_gold_customer_patterns.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.customer_order_patterns")

# COMMAND ----------

# quick check on all three gold tables
spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.revenue_by_restaurant ORDER BY total_revenue DESC").show()
spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.orders_by_hour").show()
spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.customer_order_patterns ORDER BY total_orders DESC LIMIT 10").show()