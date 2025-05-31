USE DATABASE SNOWFLAKE ;
USE SCHEMA ACCOUNT_USAGE ;

SELECT * FROM ACCOUNT_USAGE.PIPE_USAGE_HISTORY LIMIT 100 ;
SELECT * FROM ACCOUNT_USAGE.SERVICES LIMIT 100 ;


select * from SNOWFLAKE.INFORMATION_SCHEMA.TABLES LIMIT 100;
select * from SNOWFLAKE.INFORMATION_SCHEMA.VIEWS LIMIT 100;
select * from SNOWFLAKE.INFORMATION_SCHEMA.STAGES LIMIT 100;
select * from SNOWFLAKE.INFORMATION_SCHEMA.PIPES LIMIT 100;
SELECT * FROM ACCOUNT_USAGE.STAGE_STORAGE_USAGE_HISTORY LIMIT 100 ;


/*TO ADD TO POWER BI*/


select * from snowflake.account_usage.stages where deleted is null limit 100  ;  -- stages listing to single our the internal stages that we currently have

SELECT * FROM TABLE_STORAGE_METRICS WHERE ACTIVE_BYTES >0 AND TABLE_DROPPED IS NULL LIMIT 10; --tableS and their size



SELECT USAGE_DATE,STORAGE_BYTES,STAGE_BYTES,FAILSAFE_BYTES FROM ACCOUNT_USAGE.STORAGE_USAGE LIMIT 100 ; --storage by date with stage split

select * from COMMON_PROD.DWH.REF_DATE ;

WITH date_difference AS (
    SELECT
        DATE(START_TIME) AS START_DATE,
        DATE(END_TIME) AS END_DATE,
        WAREHOUSE_ID, WAREHOUSE_NAME,
        CREDITS_USED,
        CREDITS_USED_CLOUD_SERVICES, 
        CREDITS_USED_COMPUTE,
        DATEDIFF(day, DATE(START_TIME), DATE(END_TIME)) + 1 AS DIFFERENCE
    FROM
        SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY 
    WHERE 
        YEAR(START_TIME) >= YEAR(DATEADD(year, -1, CURRENT_DATE))
),
recursive_dates AS (
    SELECT
        START_DATE,
        END_DATE,
        WAREHOUSE_ID, WAREHOUSE_NAME,
        CREDITS_USED,
        CREDITS_USED_CLOUD_SERVICES, 
        CREDITS_USED_COMPUTE,
        DIFFERENCE,
        START_DATE AS DATE,
        1 AS LEVEL
    FROM
        date_difference
    UNION ALL
    SELECT
        rd.START_DATE,
        rd.END_DATE,
        rd.WAREHOUSE_ID, rd.WAREHOUSE_NAME,
        rd.CREDITS_USED,
        rd.CREDITS_USED_CLOUD_SERVICES, 
        rd.CREDITS_USED_COMPUTE,
        rd.DIFFERENCE,
        DATEADD(day, 1, rd.DATE) AS DATE,
        rd.LEVEL + 1
    FROM
        recursive_dates rd
    WHERE
        rd.LEVEL < rd.DIFFERENCE
),
date_result AS ( SELECT
    DATE,
    WAREHOUSE_ID, WAREHOUSE_NAME,
    DIFFERENCE,
    LEVEL,
    START_DATE,
    END_DATE,
    CREDITS_USED / DIFFERENCE AS CREDITS_USED,
    CREDITS_USED_CLOUD_SERVICES / DIFFERENCE AS CREDITS_USED_CLOUD_SERVICES,
    CREDITS_USED_COMPUTE / DIFFERENCE AS CREDITS_USED_COMPUTE
FROM
    recursive_dates 
    )
    select
    DATE,
    WAREHOUSE_ID, 
    WAREHOUSE_NAME,
     SUM(CREDITS_USED)  AS CREDITS_USED,
    SUM(CREDITS_USED_CLOUD_SERVICES)  AS CREDITS_USED_CLOUD_SERVICES,
    SUM(CREDITS_USED_COMPUTE) AS CREDITS_USED_COMPUTE
FROM date_result 
GROUP BY
    DATE,
    WAREHOUSE_ID, 
    WAREHOUSE_NAME ;



/*LAST ACCESED - YOU WOULD HAVE TO CREATE A QUERY THAT SEARCHES THROUGH ACCOUNT_USAGE QUERY HISTORY AND SSEARCHES FOR THE OBJECT NAME IN THE JSON*/

-- select *
-- from table(information_schema.query_history())
-- order by start_time;

-- select end_time, * from table(information_schema.query_history()) where query_text like '%<table_name>%' and execution_status='SUCCESS' order by 1 desc;

/*LIST OF VIEW - YOU WOULD HVE TO MAKE S SCRRIPT THAT IDENTIFIES TEH LIST OF DATABASES TO LOOP THROUGH TO SEARCG FOR THE INFORMATION SCHEMA*/ 


--SNOWFLAKE.INFORMATION_SCHEMA.DATABASES

-- select table_schema,
--     table_name as view_name,
--     view_definition,
--     created as create_date,
--     last_altered as modify_date,
--     comment as description
-- from information_schema.views
-- where table_schema != 'INFORMATION_SCHEMA'
-- order by table_schema,
--         table_name;  

