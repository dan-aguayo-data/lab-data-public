# Databricks notebook source
# Import necessary libraries
import dlt
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------

# Bronze Layer: Read from Kinesis and cast binary data to string


kinesisStreamName = "staging-embedded-dms-stream"
kinesisRegion = "ap-southeast-2"
aws_poc_access_key_id = dbutils.secrets.get(scope = "aws_poc_credentials", key = "aws_poc_access_key_id")
aws_poc_secret_access_key = dbutils.secrets.get(scope = "aws_poc_credentials", key = "aws_poc_secret_access_key")

kinesisDF = spark \
    .readStream \
    .format("kinesis") \
    .option("streamName", kinesisStreamName) \
    .option("region", kinesisRegion) \
    .option("initialPosition", "TRIM_HORIZON") \
    .option("awsAccessKey", aws_poc_access_key_id) \
    .option("awsSecretKey", aws_poc_secret_access_key) \
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
