-- Databricks notebook source
-- MAGIC %md
-- MAGIC **SILVER LAYER**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SUPPLIER TABLE

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_SUPPLIER
COMMENT "Silver  table"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'SCHEMA-NAME'
    AND metadata_parsed.metadata.`table-name` = 'SUPPLIER'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_SUPPLIER
COMMENT "Merged Data SUPPLIER Table"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_SUPPLIER
FROM
  STREAM(LIVE.silver_SUPPLIER)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TABLE_NAME

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_geo_state
COMMENT "GEO STATE TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'SCHEMA_NAME'
    AND metadata_parsed.metadata.`table-name` = 'GEO_STATE'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.ID AS ID,
  parsed_data.data.NAME AS NAME,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_geo_state
COMMENT "Geo State Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_geo_state
FROM
  STREAM(LIVE.silver_geo_state)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TABLE_NAME

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_TABLE_NAME
COMMENT "TABLE_NAME TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'SCHEMA_NAME'
    AND metadata_parsed.metadata.`table-name` = 'TABLE_NAME'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_TABLE_NAME
COMMENT "TABLE_NAME  Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_TABLE_NAME
FROM
  STREAM(LIVE.silver_TABLE_NAME)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TABLE_NAME

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_TABLE_NAME
COMMENT "COMMENT HERE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'SCHEMA_NAME'
    AND metadata_parsed.metadata.`table-name` = 'TABLE_NAME'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_TABLE_NAME
COMMENT "Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_TABLE_NAME
FROM
  STREAM(LIVE.silver_TABLE_NAME)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TABLE_NAME_HERE

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
-- TABLE_NAME_HERE

CREATE OR REFRESH STREAMING LIVE TABLE silver_consumer_refund_txn_dtl
COMMENT "TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'SCHEMA_NAME'
    AND metadata_parsed.metadata.`table-name` = 'CONSUMER_REFUND_TXN_DTL'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: string,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: float,
          COLUMN_NAME_HERE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  TO_TIMESTAMP(parsed_data.data.COLUMN_NAME_HERE) AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE,
  parsed_data.data.COLUMN_NAME_HERE AS COLUMN_NAME_HERE
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_merge_consumer_refund_txn_dtl
COMMENT "Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_consumer_refund_txn_dtl
FROM
  STREAM(LIVE.silver_consumer_refund_txn_dtl)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;
