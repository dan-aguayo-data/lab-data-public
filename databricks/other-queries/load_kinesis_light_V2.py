# Databricks notebook source
# MAGIC %md
# MAGIC **LIGHT WEIGHT PYTHON CODE CONNECTING TO STREAM WITH KINESIS_DF USING INSTANCE _PROFILE_**

# COMMAND ----------

from pyspark.sql.functions import col, from_json, decode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# COMMAND ----------

# Example schema; modify according to your data
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])


# COMMAND ----------

#Read data from kinesis Stream
kinesisStreamName = "staging-embedded-dms-stream"
kinesisRegion = "ap-southeast-2"

kinesisDF = spark \
    .readStream \
    .format("kinesis") \
    .option("streamName", kinesisStreamName) \
    .option("region", kinesisRegion) \
    .option("initialPosition", "TRIM_HORIZON") \
    .load()

# COMMAND ----------

#Convert Binary Data to string
stringDF = kinesisDF.selectExpr(
    "stream",
    "partitionKey",
    "CAST(data AS STRING) AS data",
    "shardId",
    "sequenceNumber",
    "approximateArrivalTimestamp"
)


# COMMAND ----------

display(stringDF)
