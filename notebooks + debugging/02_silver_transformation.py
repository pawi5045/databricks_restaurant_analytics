
from pyspark.sql.functions import col, from_json, explode, to_timestamp

#describes the schema
order_schema = "order_id STRING, timestamp STRING, restaurant_id STRING, customer_id STRING, order_type STRING, items ARRAY<STRUCT<item_id STRING, name STRING, category STRING, quantity INT, unit_price DOUBLE, subtotal DOUBLE>>, total_amount DOUBLE, payment_method STRING, order_status STRING, created_at STRING"

# COMMAND ----------

# read the bronze table as a stream so we can keep transforming new data as it comes in
df_bronze_stream = spark.readStream.table("ws_dbxproject.`01_bronze`.orders_raw")

# turn the raw bytes into readable json fields using the schema above
df_parsed = (
    df_bronze_stream
    .selectExpr("CAST(value AS STRING) as json_str", "offset")
    .withColumn("data", from_json(col("json_str"), order_schema))
    .select("data.*", "offset")
)

# COMMAND ----------

# each order can have many items, so we split (explode) the items list
# this means one row per item instead of one row per order
df_silver = (
    df_parsed
    .filter(col("order_id").isNotNull())  # skip any broken/empty rows
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

# save the cleaned, flattened data into the silver table
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

# now we add extra useful info by joining with our reference tables
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

# save this enriched version as its own table so gold can use it
df_enriched.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`02_silver`.orders_enriched")

# COMMAND ----------

# quick check
spark.sql("SELECT COUNT(*) FROM ws_dbxproject.`02_silver`.orders_enriched").show()
