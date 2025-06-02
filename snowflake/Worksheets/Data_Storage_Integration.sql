-- Set the working database
USE DATABASE MANAGE_DB;

-- STEP 1: Create a file format (CSV example)
CREATE OR REPLACE FILE FORMAT MANAGE_DB.PUBLIC.FILEFORMAT_AZURE
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 2;

-- STEP 2: Create a storage integration (requires Azure setup)
CREATE OR REPLACE STORAGE INTEGRATION AZURE_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = AZURE
    ENABLED = TRUE
    AZURE_TENANT_ID = '<your-azure-tenant-id>'
    STORAGE_ALLOWED_LOCATIONS = ('azure://<your-storage-account>.blob.core.windows.net/<your-container-path>');

-- View integration details (copy consent URL and approve it in Azure portal)
DESC STORAGE INTEGRATION AZURE_INTEGRATION;

-- STEP 3: Create a stage using the integration and file format
CREATE OR REPLACE STAGE MANAGE_DB.PUBLIC.STAGE_AZURE
    STORAGE_INTEGRATION = AZURE_INTEGRATION
    URL = 'azure://<your-storage-account>.blob.core.windows.net/<your-container-path>'
    FILE_FORMAT = FILEFORMAT_AZURE;

-- STEP 4: List files from Azure blob container
LIST @MANAGE_DB.PUBLIC.STAGE_AZURE;

-- OPTIONAL: SAS-based stage (not recommended for long-term use)
-- CREATE OR REPLACE STAGE MANAGE_DB.PUBLIC.STAGE_AZURE_SAS
--     URL = 'azure://<your-storage-account>.blob.core.windows.net/<your-container-path>'
--     CREDENTIALS = (AZURE_SAS_TOKEN = '<your-temporary-sas-token>')
--     FILE_FORMAT = FILEFORMAT_AZURE;

-- LIST FILES
-- LIST @MANAGE_DB.PUBLIC.STAGE_AZURE_SAS;
