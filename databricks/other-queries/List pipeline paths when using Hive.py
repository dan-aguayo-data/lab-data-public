# Databricks notebook source
# List contents of each pipeline directory
for path in ["xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
             "Kinesis_Stream_DLT_Pipeline", 
             "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
             "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"]:
    print(f"Contents of dbfs:/pipelines/{path}/:")
    display(dbutils.fs.ls(f"dbfs:/pipelines/{path}/"))



