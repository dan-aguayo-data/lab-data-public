// Flattening data


--QUERY TABLE AS IS FROM 

SELECT FILE_NAME, FILE_ROW_NUMBER, LANDED_DATETIME, message
    FROM LANDING.SNOWPIPE_PROD.COEX_AD 

;

SELECT FILE_NAME, FILE_ROW_NUMBER, LANDED_DATETIME, f.VALUE
    FROM LANDING.SNOWPIPE_PROD.COEX_AD s,
    lateral flatten(input => s.MESSAGE:value) f

;


SELECT FILE_NAME, FILE_ROW_NUMBER, LANDED_DATETIME, f.VALUE
    FROM LANDING.SNOWPIPE_PROD.COEX_AD ,flatten(input => s.MESSAGE:value) ;
    

SELECT * FROM TABLE(FLATTEN(input => [2,4,6])) f;


SELECT 
    RAW_FILE:id::int as id,  
    RAW_FILE:first_name::STRING as first_name,
    VALUE::STRING as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
,TABLE(flatten ( input => RAW_FILE:prev_company ));


// Step 1: Extract Raw JSON

-- Set up stage, file format and table
CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';

CREATE OR REPLACE file format MANAGE_DB.FILE_FORMATS.JSONFORMAT
    TYPE = JSON;
        
CREATE OR REPLACE table OUR_FIRST_DB.PUBLIC.JSON_RAW (
    raw_file variant);

    
-- Copy data directly into table    
COPY INTO OUR_FIRST_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMATS.JSONFORMAT
    files = ('HR_data.json');
    
   
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


select RAW_FILE:city from OUR_FIRST_DB.PUBLIC.JSON_RAW  

;




