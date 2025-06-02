


USE ROLE SYSADMIN; 

  CREATE DATABASE LANDING_S3; 
  CREATE SCHEMA SNOWPIPE;

DROP DATABASE LANDING_S3;
DROP SCHEMA SNOWPIPE;
  
show users ;

/*S3 STORAGE INTEGRATION  REFERENCE: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration

SETP 1. AWS -  CREATE POLICY
Note
Make sure to replace bucket and prefix with your actual bucket name and folder path prefix.
The Amazon Resource Names (ARN) for buckets in government regions have a arn:aws-us-gov:s3::: prefix.

JSON TO FILL: 
 

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
              "s3:PutObject",
              "s3:GetObject",
              "s3:GetObjectVersion",
              "s3:DeleteObject",
              "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::<bucket>/<prefix>/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::<bucket>",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "<prefix>/*"
                    ]
                }
            }
        }
    ]
}



SETP 2. AWS - CREATE ROLE AS AWS ACCOUNT AND ATTACH POLICY. 
Notes: 
In the Account ID field, enter your own AWS account ID temporarily. 
Later, you modify the trust relationship and grant access to Snowflake.
Select the Require external ID option. An external ID is used to grant access to your AWS resources (such as S3 buckets) to a third party like Snowflake.
Enter a placeholder ID such as 0000. In a later step, you will modify the trust relationship for your IAM role and specify the external ID for your storage integration




STEP 3. SF-  CREATE STORAGE INTEGRATION IN SNOWFLAKE 

--arn:aws:iam::XXXXXXXXXXXX:role/Snowpipe_Test_AWS_SF_Role_dan

*/



CREATE STORAGE INTEGRATION JSON_TEST
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxxxxxxx:role/Snowpipe_Test_AWS_SF_Role_dan'
  STORAGE_ALLOWED_LOCATIONS = ('*')
  --[ STORAGE_BLOCKED_LOCATIONS = ('s3://mybucket1/mypath1/sensitivedata/', 's3://mybucket2/mypath2/sensitivedata/') ]
;


--DROP STORAGE INTEGRATION JSON_TEST ;


/*
STEP 4. SF - Retrieve the AWS IAM User for Snowflake Account */


DESC INTEGRATION JSON_TEST;

--RECORD THE PROPERTIES BELOW:
--STORAGE_AWS_IAM_USER_ARN
arn:aws:iam::xxxxxxxxxxxxxxxx:user/2mtj0000-s
--External ID
 CD63105_SFCRole=2_OntUCjIAI9RsItesp3G203CvY88=

/*
--STEP 5. AWS - GRANT THE IAM USER PERMISSIONS TO ACCESS THE BUCKET OBJECTS. 

--MODIFY ROLE CREATED IN STEP 2, PRESS ON THE ROLE AND UNDER THE TAB "EDIT TRUST POLICY MODIFY AS THE BELOW

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::891377375664:user/2mtj0000-s"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "CD63105_SFCRole=2_OntUCjIAI9RsItesp3G203CvY88="
                }
            }
        }
    ]
}


STEP 6. SF - CREATE EXTERNAL STAGE. 

Remember to assign permissions to roles: 
GRANT CREATE STAGE ON SCHEMA public TO ROLE myrole;
GRANT USAGE ON INTEGRATION s3_int TO ROLE myrole;

*/
--https://docs.snowflake.com/en/sql-reference/sql/create-file-format
create or replace file format generic_csv
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 2;
    
create or replace file format generic_json
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    --ALLOW_DUPLICATE =  FALSE 
    ;

    
CREATE STAGE my_S3_test
  STORAGE_INTEGRATION = JSON_TEST
  URL =  's3://datateamtest-dan/SnowPipe/'
  FILE_FORMAT = generic_json;





--STEP 7. SF  TEST STAGE


LIST @my_S3_test;

 --(pattern=>'*.json') to filter;

SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT FROM @my_S3_test (file_format => generic_json,pattern=>'.*_data.json') t;


;


--STEP 8. SF CREATE TABLE TO COPY PIPE INTO AND PIPE IN SF

CREATE SCHEMA SNOWPIPE_TABLES ;

create table LANDING_S3.SNOWPIPE_TABLES.MY_S3_TEST_BRONZE (
	FILE_NAME VARCHAR(16777216),
	FILE_ROW_NUMBER NUMBER(38,0),
	LANDED_DATETIME TIMESTAMP_NTZ(9),
	MESSAGE VARIANT
) COMMENT='Used my first s3 test';


create pipe my_s3_test_pipe 
auto_ingest=true 
as
copy into LANDING_S3.SNOWPIPE_TABLES.MY_S3_TEST_BRONZE
from (SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, sysdate()::timestamp, t.$1::VARIANT FROM @my_S3_test (file_format => generic_json,pattern=>'.*_data.json') t) ;



--STEP 9. sf get the notification integration arn from pipe.   -- https://snowflakewiki.medium.com/snowpipe-setup-for-aws-s3-xxxxxxxxxxxx

DESC PIPE my_S3_test_pipe;

--notification_channael : arn:aws:sqs:us-west-2:xxxxxxxxxxxxxx:sf-snowpipe-AIDA47CR3LGYMDEU7HDCJ-_61I1rp87Xm1TB4fUDfrXg
--samplex_data.json

--STEP 10. AWS - CREATE NOTIFICATIONS IN AWS BUCKET


--GO TO THE BUCKET PROPERTIES AND CREATE EVENT NOTIFICATIONS
----SET UP NAME
----EVENT TYPES SELECT ALL OBJECTS
----DESTINATION SELECT SQS QUEUE AND SELECT "ENTER SQS QUEUE ARN"

--STEP 11. TEST PIPE.  

SELECT *  FROM LANDING_S3.SNOWPIPE_TABLES.MY_S3_TEST_BRONZE




CREATE STORAGE INTEGRATION LANDING_ORABLE.ORACLE_NONPROD.INT_ICEBERG_TEST
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxx:role/iceberg_test_demo_folder_access' 
 STORAGE_ALLOWED_LOCATIONS = ('s3://iceberg-data-test/demo/*')  ;
