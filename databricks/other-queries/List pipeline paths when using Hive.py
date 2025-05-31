# Databricks notebook source
# List contents of each pipeline directory
for path in ["89983833-ded8-4b45-a750-eff5f0133b3a", 
             "Kinesis_Stream_DLT_Pipeline", 
             "e1049114-87fe-461a-8b37-8022e0cb4809", 
             "fc5fdd8d-c0b4-4908-a0f2-1f7636101c43"]:
    print(f"Contents of dbfs:/pipelines/{path}/:")
    display(dbutils.fs.ls(f"dbfs:/pipelines/{path}/"))



