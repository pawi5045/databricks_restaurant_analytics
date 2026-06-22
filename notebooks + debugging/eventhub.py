# Databricks notebook source
EH_NAMESPACE = "eh-ns-dbxproject-pari"
EH_NAME = "orders"
EH_CONN_STR = os.environ.get("EH_CONN_STR")  # load from environment variable, never hardcode secrets

# COMMAND ----------

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : '60000',
  "kafka.session.timeout.ms" : '30000',
  "maxOffsetsPerTrigger"     : '50000',
  "failOnDataLoss"           : 'false',
  "startingOffsets"          : 'earliest'
}

# COMMAND ----------

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)


# COMMAND ----------

checkpoint_path = "/Volumes/ws_dbxproject/01_bronze/checkpoints/orders_bronze"

query = (
    df_raw.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("ws_dbxproject.`01_bronze`.orders_raw")
)

query.awaitTermination()

# COMMAND ----------

spark.sql("SELECT * FROM ws_dbxproject.`01_bronze`.orders_raw").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_type", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("subtotal", DoubleType())
    ]))),
    StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType()),
    StructField("created_at", StringType())
])

df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) as json_str", "offset", "timestamp as kafka_timestamp")
    .withColumn("data", from_json(col("json_str"), order_schema))
    .select("data.*", "offset", "kafka_timestamp")
)

# COMMAND ----------

spark.sql("SELECT COUNT(*) as total_rows FROM ws_dbxproject.`01_bronze`.orders_raw").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS ws_dbxproject.`02_silver`.checkpoints;

# COMMAND ----------

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("restaurant_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_type", StringType()),
    StructField("items", ArrayType(StructType([
        StructField("item_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("subtotal", DoubleType())
    ]))),
    StructField("total_amount", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("order_status", StringType()),
    StructField("created_at", StringType())
])

df_parsed = (
    df_raw
    .selectExpr("CAST(value AS STRING) as json_str", "offset", "timestamp as kafka_timestamp")
    .withColumn("data", from_json(col("json_str"), order_schema))
    .select("data.*", "offset", "kafka_timestamp")
)

# COMMAND ----------

from pyspark.sql.functions import explode, col, to_timestamp

df_silver = (
    df_parsed
    .filter(col("order_id").isNotNull())
    .withColumn("item", explode(col("items")))
    .select(
        "order_id",
        to_timestamp("timestamp").alias("order_timestamp"),
        "restaurant_id",
        "customer_id",
        "order_type",
        col("item.item_id").alias("item_id"),
        col("item.name").alias("item_name"),
        col("item.category").alias("item_category"),
        col("item.quantity").alias("quantity"),
        col("item.unit_price").alias("unit_price"),
        col("item.subtotal").alias("subtotal"),
        "total_amount",
        "payment_method",
        "order_status",
        to_timestamp("created_at").alias("created_at"),
        "offset"
    )
)

# COMMAND ----------

silver_checkpoint = "/Volumes/ws_dbxproject/02_silver/checkpoints/orders_silver"

query = (
    df_silver.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)
    .trigger(availableNow=True)
    .toTable("ws_dbxproject.`02_silver`.orders_silver")
)
query.awaitTermination()

# COMMAND ----------

spark.sql("SELECT * FROM ws_dbxproject.`02_silver`.orders_silver LIMIT 10").show(truncate=False)

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM ws_dbxproject.`02_silver`.orders_silver").show()

# COMMAND ----------

spark.sql("SHOW TABLES IN ws_dbxproject.`02_silver`").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE ws_dbxproject.`02_silver`.dim_customers").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE ws_dbxproject.`02_silver`.dim_restaurants").show(truncate=False)

# COMMAND ----------

spark.sql("DESCRIBE ws_dbxproject.`02_silver`.dim_menu_items").show(truncate=False)

# COMMAND ----------

orders = spark.read.table("ws_dbxproject.`02_silver`.orders_silver")
customers = spark.read.table("ws_dbxproject.`02_silver`.dim_customers")
restaurants = spark.read.table("ws_dbxproject.`02_silver`.dim_restaurants")
menu_items = spark.read.table("ws_dbxproject.`02_silver`.dim_menu_items")

df_enriched = (
    orders
    .join(customers.select(
            "customer_id",
            col("name").alias("customer_name"),
            col("city").alias("customer_city")
          ), "customer_id", "left")
    .join(restaurants.select(
            "restaurant_id",
            col("name").alias("restaurant_name"),
            col("city").alias("restaurant_city"),
            "country"
          ), "restaurant_id", "left")
    .join(menu_items.select(
            "restaurant_id", "item_id",
            "is_vegetarian", "spice_level"
          ), ["restaurant_id", "item_id"], "left")
)

# COMMAND ----------

df_enriched.show(10, truncate=False)
df_enriched.count()

# COMMAND ----------

df_enriched.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`02_silver`.orders_enriched")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM ws_dbxproject.`02_silver`.orders_enriched").show()

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum, avg

# COMMAND ----------

df_gold_revenue_by_restaurant = (
    spark.read.table("ws_dbxproject.`02_silver`.orders_enriched")
    .groupBy("restaurant_id", "restaurant_name", "restaurant_city", "country")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("subtotal").alias("total_revenue"),
        avg("subtotal").alias("avg_item_revenue")
    )
)

# COMMAND ----------

df_gold_revenue_by_restaurant.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.revenue_by_restaurant")

# COMMAND ----------

spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.revenue_by_restaurant ORDER BY total_revenue DESC").show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import hour, countDistinct

df_gold_orders_by_hour = (
    spark.read.table("ws_dbxproject.`02_silver`.orders_enriched")
    .withColumn("order_hour", hour("order_timestamp"))
    .groupBy("order_hour")
    .agg(countDistinct("order_id").alias("total_orders"))
    .orderBy("order_hour")
)

# COMMAND ----------

df_gold_orders_by_hour.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.orders_by_hour")

# COMMAND ----------

spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.orders_by_hour").show(24)

# COMMAND ----------

from pyspark.sql.functions import countDistinct, sum

df_gold_customer_patterns = (
    spark.read.table("ws_dbxproject.`02_silver`.orders_enriched")
    .groupBy("customer_id", "customer_name", "customer_city")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("subtotal").alias("total_spend")
    )
    .orderBy(col("total_orders").desc())
)

# COMMAND ----------

df_gold_customer_patterns.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.customer_order_patterns")

# COMMAND ----------

spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.customer_order_patterns ORDER BY total_orders DESC LIMIT 15").show(truncate=False)