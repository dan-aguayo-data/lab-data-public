-- 4. Missing privileges or USED_CACHED_RESULTS is set to false
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

--retention period

  show tables like '%CUSTOMERS%';

  // Drop and undrop table
  drop table our_first_db.public.customers;
  undrop table our_first_db.public.customers;

  // Alter retention period
  alter table UR_FIRST_DB.public.customers_example 
  set data_retention_period_in_days = 0;

  alter account set data_retention_time_in_days = 1;

 Show current_account;


 //// TIME TRAVEL COST ////

// Storage usage on account level
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;


// Storage usage on table level
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

// Storage usage on table level - formatted
SELECT 	ID, 
		TABLE_NAME, 
		TABLE_SCHEMA,
        TABLE_CATALOG,
		ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
		TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB,
        FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;


//CLONING USING TIME TRAVEL //

CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.CUSTOMERS_CLONE
CLONE OUR_FIRST_DB.public.customers AT (OFFSET => -60*1.0);  -- USE QUERY ID TO BE MORE SURE