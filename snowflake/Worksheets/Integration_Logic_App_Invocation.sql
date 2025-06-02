-- SNOWFLAKE + AZURE EXTERNAL FUNCTION SETUP

USE ROLE ACCOUNTADMIN;

-- STEP 1: View existing API integrations
SHOW API INTEGRATIONS;
DESCRIBE API INTEGRATION AZURE_EXTERNAL_FUNCTION;

-- STEP 2: CREATE NEW API INTEGRATION FOR AZURE FUNCTION
-- Reference: https://docs.snowflake.com/en/sql-reference/external-functions-creating-azure-common-ext-function

CREATE OR REPLACE API INTEGRATION AZURE_EXTERNAL_FUNCTION
  API_PROVIDER = azure_api_management
  AZURE_TENANT_ID = '<your_tenant_id>'  -- Example: abcd1234-5678-90ab-cdef-1234567890ab
  AZURE_AD_APPLICATION_ID = '<your_application_id>'  -- The Azure AD app registered in the Function App
  API_ALLOWED_PREFIXES = ('https://my-api-mgmt-name.azure-api.net/my-function/')
  ENABLED = TRUE;

-- STEP 3: GET THE AZURE CONSENT URL AND GRANT PERMISSIONS
DESCRIBE API INTEGRATION AZURE_EXTERNAL_FUNCTION;

-- Copy the AZURE_CONSENT_URL value from the result and open it in your browser to grant Snowflake access.
-- Example format:
-- https://login.microsoftonline.com/<tenant_id>/oauth2/authorize?client_id=<app_id>&response_type=code

-- STEP 4: CREATE AN EXTERNAL FUNCTION IN SNOWFLAKE
-- Adjust parameter names/types and function URL as required.

CREATE OR REPLACE EXTERNAL FUNCTION invoke_external_logic(
    input_string VARCHAR
)
RETURNS VARIANT
API_INTEGRATION = AZURE_EXTERNAL_FUNCTION
AS 'https://my-api-mgmt-name.azure-api.net/my-function/trigger';

-- STEP 5: GRANT ACCESS TO THE FUNCTION
GRANT USAGE ON FUNCTION invoke_external_logic(VARCHAR) TO ROLE PUBLIC;

-- STEP 6: TEST THE FUNCTION (WITH A SAFE ENCODED URL OR INPUT VALUE)
SELECT invoke_external_logic('some_encoded_or_input_value');

-- STEP 7: (OPTIONAL) VIEW EXISTING EXTERNAL FUNCTIONS
SHOW EXTERNAL FUNCTIONS;
