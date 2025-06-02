-- Modify table clustering
ALTER TABLE INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE CLUSTER BY (METADATA:"entity-type"::STRING, TO_DATE(METADATA:ingested_at::TIMESTAMP_NTZ));

-- Show table details
SHOW TABLES LIKE 'DATA_STREAM_STAGE';

-- Query automatic clustering history
SELECT * 
FROM snowflake.account_usage.automatic_clustering_history;

-- Aggregate row counts by entity type and date
SELECT 
    METADATA:"entity-type"::STRING AS entity_type,
    TO_DATE(METADATA:ingested_at::TIMESTAMP_NTZ) AS ingest_date,
    COUNT(*) AS row_count
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE
GROUP BY ALL
ORDER BY row_count;

-- Query specific query history
SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_ID IN ('01b5d3ee-3202-b43b-0001-b686002ad006', '01b5c322-3202-b5f0-0001-b68600258032');

-- Query table pruning history
SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_PRUNING_HISTORY
LIMIT 10;

-- Check clustering information for specific key
SELECT SYSTEM$CLUSTERING_INFORMATION('INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE', '(METADATA:"entity-type"::STRING)');

-- Query timestamp metadata
SELECT 
    TO_DATE(METADATA:ingested_at::TIMESTAMP_NTZ) AS ingest_date,
    METADATA:ingested_at
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE 
LIMIT 10;

-- Query clustering information with expression
SELECT SYSTEM$CLUSTERING_INFORMATION('INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE', '(METADATA:"entity-type"::STRING)');

-- Filter by specific entity type
SELECT * 
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE
WHERE METADATA:"entity-type" = 'CONFIG_FIELDS';

-- Query sample data
SELECT * 
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE
LIMIT 10;

-- Count total rows
SELECT COUNT(1) 
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE;

-- Query clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE');

-- Query warehouse credits usage
SELECT 
    WAREHOUSE_NAME, 
    SUM(CREDITS_USED) AS credits_used
FROM snowflake.account_usage.warehouse_metering_history
GROUP BY 1;

-- Drop clustering key
ALTER TABLE INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE_TEST DROP CLUSTERING KEY;

-- Manage materialized view clustering (example)
ALTER TABLE my_view CLUSTER BY (record_id);
ALTER TABLE my_view SUSPEND;
ALTER TABLE my_view RESUME;
ALTER TABLE my_view DROP CLUSTERING KEY;
ALTER TABLE my_view SUSPEND RECLUSTER;
ALTER TABLE my_view RESUME RECLUSTER;

-- Aggregate clustering credits by week
WITH credits_by_day AS (
    SELECT 
        TO_DATE(start_time) AS date,
        SUM(credits_used) AS credits_used
    FROM snowflake.account_usage.automatic_clustering_history
    GROUP BY 1
    ORDER BY 2 DESC
)
SELECT 
    DATE_TRUNC('week', date) AS week_start,
    AVG(credits_used) AS avg_daily_credits
FROM credits_by_day
GROUP BY 1
ORDER BY 1;

-- Query clustering credits by table
SELECT 
    TO_DATE(start_time) AS date,
    database_name,
    schema_name,
    table_name,
    SUM(credits_used) AS credits_used
FROM snowflake.account_usage.automatic_clustering_history
WHERE start_time >= DATEADD(MONTH, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3, 4
ORDER BY 5 DESC;

-- Test single-column clustering
ALTER TABLE INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE CLUSTER BY (METADATA:"entity-type"::STRING);

/** Check Current Cluster Information **/

-- Show table details
SHOW TABLES LIKE 'DATA_STREAM_STAGE';

-- Query clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE');

-- Query clustering depth
SELECT SYSTEM$CLUSTERING_DEPTH('INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE', '(METADATA:"entity-type"::STRING)');

-- Check for null entity types
SELECT * 
FROM INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE 
WHERE METADATA:"entity-type"::STRING IS NULL;

-- Query metering history
SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY 
ORDER BY usage_date;

SELECT * 
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY 
ORDER BY start_time;

-- Restore original clustering
ALTER TABLE INGESTION_RDS.TEST_ENV.DATA_STREAM_STAGE CLUSTER BY (METADATA:"entity-type"::STRING, TO_DATE(METADATA:ingested_at::TIMESTAMP_NTZ));