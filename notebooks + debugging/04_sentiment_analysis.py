# Databricks notebook source
spark.sql("DESCRIBE ws_dbxproject.`01_bronze`.reviews").show(truncate=False)

# COMMAND ----------

df_sentiment = spark.sql("""
    SELECT 
        review_id,
        order_id,
        customer_id,
        restaurant_id,
        review_text,
        rating,
        review_timestamp,
        ai_analyze_sentiment(review_text) as sentiment
    FROM ws_dbxproject.`01_bronze`.reviews
""")

df_sentiment.show(10, truncate=False)

# COMMAND ----------

#saves the full sentiment-tagged reviews table
df_sentiment.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.reviews_with_sentiment")

# COMMAND ----------

#aggregates sentiment by restaurant
from pyspark.sql.functions import count, avg

df_gold_sentiment_summary = (
    spark.read.table("ws_dbxproject.`04_gold`.reviews_with_sentiment")
    .groupBy("restaurant_id", "sentiment")
    .agg(
        count("review_id").alias("review_count"),
        avg("rating").alias("avg_rating")
    )
    .orderBy("restaurant_id", "sentiment")
)

df_gold_sentiment_summary.write.format("delta").mode("overwrite").saveAsTable("ws_dbxproject.`04_gold`.sentiment_by_restaurant")

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM ws_dbxproject.`04_gold`.reviews_with_sentiment").show()
spark.sql("SELECT * FROM ws_dbxproject.`04_gold`.sentiment_by_restaurant ORDER BY restaurant_id, sentiment").show(20)