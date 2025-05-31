/// TIME TRAVEL ////

// Setting up table

CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id int,
    first_name string,
    last_name string,
    email string,
    gender string,
    Job string,
    Phone string);
    

CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    type = csv
    field_delimiter = ','
    skip_header = 1;
    
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    file_format = MANAGE_DB.file_formats.csv_file;
    

// Load data into table    
LIST @MANAGE_DB.external_stages.time_travel_stage;



COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');


SELECT * FROM OUR_FIRST_DB.public.test;


// Use-case: Update data (by mistake)

UPDATE OUR_FIRST_DB.public.test
SET FIRST_NAME = 'Joyen' ;

2023-03-27 06:01:00.830


// // // Using time travel: Method 1 - 2 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*1);



// // // Using time travel: Method 2 - before timestamp

SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => '2023-03-27 14:40:30.830'::timestamp);

SELECT current_timestamp;



-- Setting up UTC time for convenience

ALTER SESSION SET TIMEZONE ='UTC';
SELECT CURRENT_TIMESTAMP;


// // // Using time travel: Method 3 - before Query ID

// Preparing table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
  last_name string,
  email string,
  gender string,
  Phone string,
  Job string);

COPY INTO OUR_FIRST_DB.public.test
from @MANAGE_DB.external_stages.time_travel_stage
files = ('customers.csv');


SELECT * FROM OUR_FIRST_DB.public.test;

DESC TABLE OUR_FIRST_DB.public.test;

// Altering table (by mistake)

UPDATE OUR_FIRST_DB.public.test
SET EMAIL = null;


SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test before (statement => '01ab3a54-0001-0861-0003-a4c60003108e');

