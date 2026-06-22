# Databricks notebook source
import os

# this cell sets up the connection details for our event hub
# event hub acts like kafka, so we connect to it using kafka style settings
# store your connection string as a databricks secret or environment variable, never hardcode it
EH_NAMESPACE = "eh-ns-dbxproject-pari"
EH_NAME = "orders"
EH_CONN_STR = os.environ.get("EH_CONN_STR")  # load from environment variable, never hardcode secrets

# COMMAND ----------

# these are the settings spark needs to read from event hub
# think of this as the "address and password" spark uses to listen to the stream
KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : '60000',
  "kafka.session.timeout.ms" : '30000',
  "maxOffsetsPerTrigger"     : '50000',
  "failOnDataLoss"           : 'false',  # if old messages expire, dont fail, just skip them
  "startingOffsets"          : 'earliest'  # start from the oldest available message
}

# COMMAND ----------

# this defines the live stream coming from event hub
# nothing happens yet, this just describes what we want to read
df_raw = (
    spark.readStream
    .format("kafka")
    .options(**KAFKA_OPTIONS)
    .load()
)

# COMMAND ----------

# this actually runs the stream and saves the raw data into our bronze table
checkpoint_path = "/Volumes/ws_dbxproject/01_bronze/checkpoints/orders_bronze"

query = (
    df_raw.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)  # spark uses this to remember what it already read
    .trigger(availableNow=True)  # process whatever is available right now, then stop
    .toTable("ws_dbxproject.`01_bronze`.orders_raw")
)
query.awaitTermination()

# COMMAND ----------

# quick check to confirm bronze table has data
spark.sql("SELECT COUNT(*) FROM ws_dbxproject.`01_bronze`.orders_raw").show()
