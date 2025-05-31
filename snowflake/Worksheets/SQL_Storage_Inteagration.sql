USE DATABASE MANAGE_DB;


//create with no credentials as we are granting permission from Azure
create or replace stage manage_db.public.stage_azure
    URL = 'azure://dandevtrainingaus.blob.core.windows.net/dev-snow-test'
  credentials=( azure_sas_token='sv=2023-01-03&st=2023-12-30T23%3A13%3A54Z&se=2023-12-31T23%3A13%3A54Z&sr=c&sp=racwdxltf&sig=JAwBSEdXPAS57Kzjm33qnvyxPDdkbsBIJu5epoh%2BO4o%3D') 
    ;
   
-- list files
LIST @manage_db.public.stage_azure;

-- create integration object that contains the access information
CREATE OR REPLACE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID =  'e2287b86-cde7-446c-b2c0-0687de8d5afe'   --can be found in account active directory in the initial overview page
  STORAGE_ALLOWED_LOCATIONS = ( 'azure://dandevtrainingaus.blob.core.windows.net/dev-snow-test')  -- possible to add multiple locations
  
  ;

  
  
-- Describe integration object to provide access
// get consent URL and copy paste in the browser and next steps will follow

DESC STORAGE integration azure_integration

// name of snowflake (storage integration) can be found under enterprise application resource, this is the name that has to be added to the role above


// after t he URL part is done the next step  is to go int oazure and go to blobl storage and into AIM to see the roles assignments for Blob Storage





;

---- Create file format & stage objects ----

-- create file format
create or replace file format manage_db.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 2;

-- create stage object
create or replace stage manage_db.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://dandevtrainingaus.blob.core.windows.net/dev-snow-test'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @manage_db.public.stage_azure   ;



create or replace stage manage_db.public.stage_azure
    URL = 'https://<your-container-url>';
   

-- list files
LIST @manage_db.public.stage_azure;
