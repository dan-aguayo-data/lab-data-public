
ALTER TABLE LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM CLUSTER BY (METADATA:"table-name"::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ));

SHOW TABLES LIKE 'AWS_RDS_STAGE_CES_MSCM';

SELECT * FROM snowflake.account_usage.automatic_clustering_history;

select 
METADATA:"table-name"::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ), count(*) rowcnt
from LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM
group by all
order by 3;


select * from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
where QUERY_ID in ('01b5d3ee-3202-b43b-0001-b686002ad006','01b5c322-3202-b5f0-0001-b68600258032')

select * from SNOWFLAKE.ACCOUNT_USAGE.TABLE_PRUNING_HISTORY
limit 10

the alter table identifer statement error is - 01b5c322-3202-b5f0-0001-b68600258032


https://cd63105.ap-southeast-2.snowflakecomputing.com
limit 10


SELECT SYSTEM$CLUSTERING_INFORMATION('LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM', 'METADATA:"table-name"::string');

select TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ), METADATA:timestamp
from LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM limit 10


2024-07-18T10:19:33.078366Z


select SYSTEM$CLUSTERING_INFORMATION( 'LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM', '(METADATA:"table-name"::string)' );

SELECT * FROM LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM
WHERE METADATA:"table-name"='ADM_FLEXFIELDS';

SELECT * FROM LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM
limit 10;

SELECT count(1) FROM LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM;

select SYSTEM$CLUSTERING_INFORMATION( 'LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM' );

{
  "cluster_by_keys" : "LINEAR(METADATA:\"table-name\"::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ))",
  "total_partition_count" : 514332,
  "total_constant_partition_count" : 0,
  "average_overlaps" : 514331.0,
  "average_depth" : 514332.0,
  "partition_depth_histogram" : {
    "00000" : 0,
    "00001" : 0,
    "00002" : 0,
    "00003" : 0,
    "00004" : 0,
    "00005" : 0,
    "00006" : 0,
    "00007" : 0,
    "00008" : 0,
    "00009" : 0,
    "00010" : 0,
    "00011" : 0,
    "00012" : 0,
    "00013" : 0,
    "00014" : 0,
    "00015" : 0,
    "00016" : 0,
    "524288" : 514332
  },
  "clustering_errors" : [ ]
}




{
  "cluster_by_keys" : "LINEAR(METADATA:\"table-name\"::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ))",
  "total_partition_count" : 514332,
  "total_constant_partition_count" : 0,
  "average_overlaps" : 514331.0,
  "average_depth" : 514332.0,
  "partition_depth_histogram" : {
    "00000" : 0,
    "00001" : 0,
    "00002" : 0,
    "00003" : 0,
    "00004" : 0,
    "00005" : 0,
    "00006" : 0,
    "00007" : 0,
    "00008" : 0,
    "00009" : 0,
    "00010" : 0,
    "00011" : 0,
    "00012" : 0,
    "00013" : 0,
    "00014" : 0,
    "00015" : 0,
    "00016" : 0,
    "524288" : 514332
  },
  "clustering_errors" : [ ]
}




SELECT WAREHOUSE_NAME, SUM(CREDITS_USED) CREDITS_USED
FROM snowflake.account_usage.warehouse_metering_history
group by 1;





alter table LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM_TEST drop clustering key;


ALTER TABLE my_mv CLUSTER BY(i);
ALTER TABLE my_mv SUSPEND;
ALTER TABLE my_mv RESUME;
ALTER TABLE my_mv DROP CLUSTERING KEY;
ALTER TABLE my_mv SUSPEND RECLUSTER;
ALTER TABLE my_mv RESUME RECLUSTER;



 



--manual recluster unsupported
ALTER TABLE LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM RECLUSTER;




WITH credits_by_day AS (
  SELECT TO_DATE(start_time) AS date,
    SUM(credits_used) AS credits_used
  FROM snowflake.account_usage.automatic_clustering_history
  --WHERE start_time >= DATEADD(year,-1,CURRENT_TIMESTAMP())
  GROUP BY 1
  ORDER BY 2 DESC
)

SELECT DATE_TRUNC('week',date),
      AVG(credits_used) AS avg_daily_credits
FROM credits_by_day
GROUP BY 1
ORDER BY 1;



SELECT TO_DATE(start_time) AS date,
  database_name,
  schema_name,
  table_name,
  SUM(credits_used) AS credits_used
FROM snowflake.account_usage.automatic_clustering_history
WHERE start_time >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2,3,4
ORDER BY 5 DESC;





ALTER TABLE LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM CLUSTER BY (METADATA:"table-name"::string);


-----------------------------DANIEL MODS -------------------------------------------------------------
/*CHECK CURRENT CLUSTER INFORMATION */

SHOW TABLES  LIKE 'AWS_RDS_STAGE_CES_MSCM' ;

--"LINEAR(METADATA:""table-name""::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ))"


select SYSTEM$CLUSTERING_INFORMATION('LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM') ;


-- "{
--   ""cluster_by_keys"" : ""LINEAR(METADATA:\""table-name\""::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ))"",
--   ""total_partition_count"" : 514332,
--   ""total_constant_partition_count"" : 0,
--   ""average_overlaps"" : 514331.0,
--   ""average_depth"" : 514332.0,
--   ""partition_depth_histogram"" : {
--     ""00000"" : 0,
--     ""00001"" : 0,
--     ""00002"" : 0,
--     ""00003"" : 0,
--     ""00004"" : 0,
--     ""00005"" : 0,
--     ""00006"" : 0,
--     ""00007"" : 0,
--     ""00008"" : 0,
--     ""00009"" : 0,
--     ""00010"" : 0,
--     ""00011"" : 0,
--     ""00012"" : 0,
--     ""00013"" : 0,
--     ""00014"" : 0,
--     ""00015"" : 0,
--     ""00016"" : 0,
--     ""524288"" : 514332
--   },
--   ""clustering_errors"" : [ ]
-- }"


/* TEST WITH CLUSTER ON TABLE NAME ONLY */ 

ALTER TABLE LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM CLUSTER BY (METADATA:"table-name"::string);

select * from SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY order by usage_date;
select * from SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY order by start_time ;


SHOW TABLES  LIKE 'AWS_RDS_STAGE_CES_MSCM' ;

--"LINEAR(METADATA:""table-name""::string)"


select SYSTEM$CLUSTERING_INFORMATION('LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM') ;

-- "{
--   ""cluster_by_keys"" : ""LINEAR(METADATA:\""table-name\""::string)"",
--   ""total_partition_count"" : 514332,
--   ""total_constant_partition_count"" : 0,
--   ""average_overlaps"" : 514331.0,
--   ""average_depth"" : 514332.0,
--   ""partition_depth_histogram"" : {
--     ""00000"" : 0,
--     ""00001"" : 0,
--     ""00002"" : 0,
--     ""00003"" : 0,
--     ""00004"" : 0,
--     ""00005"" : 0,
--     ""00006"" : 0,
--     ""00007"" : 0,
--     ""00008"" : 0,
--     ""00009"" : 0,
--     ""00010"" : 0,
--     ""00011"" : 0,
--     ""00012"" : 0,
--     ""00013"" : 0,
--     ""00014"" : 0,
--     ""00015"" : 0,
--     ""00016"" : 0,
--     ""524288"" : 514332
--   },
--   ""clustering_errors"" : [ ]
-- }"

 select SYSTEM$CLUSTERING_DEPTH( 'LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM', '(METADATA:"table-name"::string)') ;


--514332


SELECT * FROM LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM WHERE METADATA:"table-name"::string IS NULL ; 



/* RETURNING CLUSTER BACK TO ORIGINAL SETTING */

 ALTER TABLE LANDING_ORACLE.ORACLE_UAT.AWS_RDS_STAGE_CES_MSCM CLUSTER BY (METADATA:"table-name"::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ));