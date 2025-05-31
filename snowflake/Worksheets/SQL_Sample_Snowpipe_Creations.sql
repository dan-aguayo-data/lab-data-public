
WITH SOURCE AS (
  SELECT split_part(FILE_NAME,'/',1) FILE_NAME, LAST_LOAD_TIME, PIPE_RECEIVED_TIME, ERROR_COUNT
  FROM TABLE(EXT_DEV.information_schema.copy_history(TABLE_NAME=>'LANDING.SNOWPIPE_PROD.CES_MSCM_PROD', start_time=> dateadd(minute, -60, current_timestamp())))
)
SELECT
    COUNT(B."table_name") AS NUM_TABLES_TO_CHECK,
    SUM(CASE WHEN S.FILE_NAME IS NOT NULL THEN 1 ELSE 0 END) AS TABLES_REFRESHED
FROM CES_REF.ADF.CONTROL_DBT_TABLES_CHECK B 
LEFT OUTER JOIN SOURCE S
    ON LOWER(B."table_name") = S.FILE_NAME


;

--COEX AD snowpipe
 
--use sysadmin
 
create TABLE "LANDING"."SNOWPIPE_PROD"."COEX_AD" (
	FILE_NAME VARCHAR(16777216),
	FILE_ROW_NUMBER NUMBER(38,0),
	LANDED_DATETIME TIMESTAMP_NTZ(9),
	MESSAGE VARIANT
) COMMENT='Used for loading Coex AD data via copy into';
 
 
GRANT SELECT ON TABLE "LANDING"."SNOWPIPE_PROD"."COEX_AD" TO ROLE DBT_CES_SA;
GRANT SELECT ON TABLE "LANDING"."SNOWPIPE_PROD"."COEX_AD" TO ROLE LANDING_SA;
 
 
--STAGE
 
show stages;
desc stage COEX_COLLECT_STAGE ; 
SHOW FILES IN STAGE COEX_COLLECT_STAGE ;
ls @LANDING.SNOWPIPE_PROD.COEX_COLLECT_STAGE ;
--accountadmin
 
create or replace stage COEX_AD_STAGE
url='azure://cexauecsrsa.blob.core.windows.net/coex-data-extracts/dev/azure-data-extracts'
credentials=(azure_sas_token='sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2030-04-19T13:27:08Z&st=2024-04-19T05:27:08Z&spr=https&sig=FrkK3F%2BX84OiH6kp2eLhYvRT4X9QSpxIgWKamSdJ6Ps%3D')
file_format = (TYPE=JSON);

USE DATABASE LANDING; 

USE SCHEMA SNOWPIPE_PROD;


--REVIEW STAGE properties

desc stage COEX_AD_STAGE ;

--TEST filtering of the patter of files to grab


ls @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE PATTERN= '.*.json'; 
ls @"LANDING"."SNOWPIPE_PROD"."COEX_AD_STAGE"   ;
ls @"LANDING"."SNOWPIPE_PROD"."COEX_AD_STAGE" PATTERN='.*users/.*.json' ;
ls @"LANDING"."SNOWPIPE_PROD"."COEX_AD_STAGE" PATTERN='.*groupmembers/.*.json' ;
ls @"LANDING"."SNOWPIPE_PROD"."COEX_AD_STAGE" PATTERN='.*.txt';


-- FIX APPLIED TO BYPASS THE TXT FILE, $1 refers to the first JSON Column

SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT 
FROM @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE  (pattern=>'.*users/.*.json') t;
 
SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT 
FROM @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE  (pattern=>'.*groupmembers/.*.json') t;
 
SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT 
FROM @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE  (pattern=>'.*groups/.*.json') t;
 
 
--test load into future table
copy into LANDING.SNOWPIPE_PROD.COEX_AD(FILE_NAME, FILE_ROW_NUMBER, LANDED_DATETIME, MESSAGE)
FROM (SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT
FROM @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE (pattern=>'.*.json') t)
file_format = (type = JSON, compression = Auto) on_error = 'skip_file';


select * from LANDING.SNOWPIPE_PROD.COEX_AD;

select
split_part(FILE_NAME,'/',3) folder_name  -- get third object of path string
,*
from LANDING.SNOWPIPE_PROD.COEX_AD
order by folder_name  

;


 
--Notification
 
create or replace notification integration COEX_AD_NOTIFICATION
enabled = true
type = queue
notification_provider = azure_storage_queue
azure_storage_queue_primary_uri = 'https://cexauecsrsa.queue.core.windows.net/cexauecsraqueue'
azure_tenant_id = '146bdb13-3311-4772-8254-08cc5909f701';
 
show notification integrations;
DESC NOTIFICATION INTEGRATION COEX_AD_NOTIFICATION;
 
 
--PIPE
 
show pipes;
desc pipe COEX_COLLECT_PIPE;
desc pipe COEX_AD_PIPE; 
create or replace pipe COEX_AD_PIPE 
auto_ingest = true
integration = 'COEX_AD_NOTIFICATION'
as
copy into LANDING.SNOWPIPE_PROD.COEX_AD(FILE_NAME, FILE_ROW_NUMBER, LANDED_DATETIME, MESSAGE)
FROM (SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT
FROM @LANDING.SNOWPIPE_PROD.COEX_AD_STAGE (pattern=>'.*.json') t)
file_format = (type = JSON, compression = Auto) on_error = 'skip_file';
 
select system$pipe_status('LANDING.SNOWPIPE_PROD.COEX_AD_PIPE');

select system$pipe_status('LANDING.SNOWPIPE_PROD.COEX_COLLECT_PIPE');





