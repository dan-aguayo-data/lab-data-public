# Databricks notebook source
# Import necessary libraries
import dlt
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# COMMAND ----------



# Bronze Layer: Read from Kinesis and cast binary data to string
@dlt.table(
    name="bronze_kinesis_data",
    comment="Raw data ingested from Kinesis stream with data cast to string"
)
def bronze_kinesis_data():
    stream_name = "staging-embedded-dms-stream"
    region_name = "ap-southeast-2"
    aws_poc_access_key_id = dbutils.secrets.get(scope = "aws_poc_credentials", key = "aws_poc_access_key_id")
    aws_poc_secret_access_key = dbutils.secrets.get(scope = "aws_poc_credentials", key = "aws_poc_secret_access_key")

    # Read from Kinesis
    kinesisDF = (
        spark.readStream
        .format("kinesis")
        .option("streamName", stream_name)
        .option("region", region_name)
        .option("awsAccessKey", aws_poc_access_key_id) 
        .option("awsSecretKey", aws_poc_secret_access_key)
        .option("initialPosition", "TRIM_HORIZON")
        .load()
    )
    # Convert Binary Data to String and select additional fields
    stringDF = kinesisDF.selectExpr(
        "stream",
        "data",
        "partitionKey",
        "CAST(data AS STRING) AS data_str",  # Renamed column to 'data_str'
        "shardId",
        "sequenceNumber",
        "approximateArrivalTimestamp"
    )
    return stringDF
