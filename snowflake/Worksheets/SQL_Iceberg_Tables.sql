-- DROP SCHEMA RAW_PROD.COEX_COLLECT_UAT;
-- DROP SCHEMA RAW_DEV.COEX_COLLECT_UAT;
-- DROP SCHEMA RAW_DEV.TEST_POC ;
-- DROP SCHEMA RAW_DEV.TEST_COLLECT ;
-- DROP SCHEMA RAW_DEV.TEST_COLLECT_UAT ;




USE DATABASE RAW_DEV;

CREATE SCHEMA RAW_DEV.TEST ; 






/* TEST TABLE*/
SELECT * FROM RAW_DEV.CES_ERP.AP_BALANCES ;



-- /*STORAGE INTEGRATION WITH S3 */

-- CREATE  OR REPLACE   FILE FORMAT  RAW_DEV.TEST.PARQUET_GENERIC
--    TYPE = PARQUET 
-- ;


-- CREATE OR REPLACE STORAGE INTEGRATION INT_ICEBERG_TEST    -- DONT ADD DATABASE AND SCHEMA IN THE NAME 
--   TYPE = EXTERNAL_STAGE
--   STORAGE_PROVIDER = 'S3'
--   ENABLED = TRUE
--   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::330977107317:role/iceberg_test_demo_folder_access' 
--  STORAGE_ALLOWED_LOCATIONS = ('s3://iceberg-data-test/demo/')  ;


-- DESC INTEGRATION INT_ICEBERG_TEST;

-- ---aws user arn == arn:aws:iam::211125613752:user/externalstages/ci38x60000
-- --- external id == UR45154_SFCRole=49_nKgoYmmeZAB1UU5jXoQhITk1tvQ=

drop storage integration INT_ICEBERG_TEST ; 

/*AFTER YOUR INTEGRATION IS DONE   CREATE EXTERNAL VOLUME */


use role sysadmin ; 

use schema test ;


use role accountadmin ;

CREATE OR REPLACE EXTERNAL VOLUME exvol_iceberg_test
  STORAGE_LOCATIONS =
      (
        (
            NAME = 'my-s3-iceberg-demo'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://iceberg-data-test/demo/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::330977107317:role/iceberg_test_demo_folder_access'
            --ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab') 
            )
        ) ;

DESC external volume  exvol_iceberg_test;
-- STORAGE_AWS_IAM_USER_ARN: arn:aws:iam::211125613752:user/externalstages/ci38x60000
 -- STORAGE_AWS_EXTERNAL_ID: UR45154_SFCRole=49_y+UXdQW1lMuNeooK6Q+PqZ5Jox4=


CREATE OR REPLACE EXTERNAL VOLUME exvol_iceberg_test_us_west
  STORAGE_LOCATIONS =
      (
        (
            NAME = 'my-s3-iceberg-demo-us-west'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://iceberg-data-test-us-west/demo/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::330977107317:role/iceberg_test_demo_folder_access'
            --ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab') 
            )
        ) ;

DESC external volume  exvol_iceberg_test_us_west;
-- STORAGE_AWS_IAM_USER_ARN: aarn:aws:iam::211125613752:user/externalstages/ci38x60000
 -- STORAGE_AWS_EXTERNAL_ID: UR45154_SFCRole=2_eF6B4CAEVyd3ScYbo/q2Y95EYWc=



/* CREATE STAGE */


/* GRANT PRIVILEGES TO A LOWER ROLE */


grant all privileges on external volume exvol_iceberg_test to role sysadmin ;
grant all privileges on external volume exvol_iceberg_test_us_west to role sysadmin ;


/* CREATE ICEBERG TABLE USING SNOWFLAKE AS CATALOG */  -- RAW_DEV.GG_CES_MSCM.SCHEME_REF_CODES

--DESC TABLE RAW_DEV.CES_MSCM.SCHEME_REF_CODES ;


use role sysadmin;

use schema test;  --make sure you are working in the same location you created the external volume

CREATE OR REPLACE ICEBERG TABLE ref_codes_iceberg (
    SCHEME_REF_CODES_ID NUMBER(38,0),
    MULTI_SCHEME_ID NUMBER(38,0),
    CATEGORY VARCHAR(16777216),
    CODE VARCHAR(16777216),
    VALUE VARCHAR(16777216),
    DESCRIPTION VARCHAR(16777216),
    EFFECTIVE_FROM TIMESTAMP_NTZ(6), -- max timestamp is 6
    EFFECTIVE_TO TIMESTAMP_NTZ(6),
    CREATED_BY VARCHAR(16777216),
    CREATED_ON TIMESTAMP_NTZ(6),
    LAST_MODIFIED_BY VARCHAR(16777216),
    LAST_MODIFIED_ON TIMESTAMP_NTZ(6),
    ROW_EDITING_ENABLED VARCHAR(16777216),
    ROW_DISPLAY_ENABLED VARCHAR(16777216)
)  
    CATALOG='SNOWFLAKE'
    EXTERNAL_VOLUME='exvol_iceberg_test'
    BASE_LOCATION='';


 INSERT INTO ref_codes_iceberg
  SELECT * FROM RAW_DEV.GG_CES_MSCM.SCHEME_REF_CODES;


  ;


  select * from RAW_UAT.CES_MSCM_ORACLE.ADM_FLEXFIELDS

