-- Ensure you're using a privileged role
USE ROLE ACCOUNTADMIN;

-- Optional: Create security integration (you left it empty, fill in if needed)
-- CREATE SECURITY INTEGRATION ... ;

------------------------------------------------
-- Create ADF_COMPANY_NAME_DEV User for ADF Testing
------------------------------------------------
CREATE USER IF NOT EXISTS ADF_COMPANY_NAME_DEV
  PASSWORD = 'adfPassword123'
  LOGIN_NAME = 'adf_COMPANY_NAME_dev'
  MUST_CHANGE_PASSWORD = FALSE
  DEFAULT_WAREHOUSE = 'BRONZE_DEV'
  DEFAULT_ROLE = 'ADF_COMPANY_NAME'
  COMMENT = 'User for ADF DEV tests';

-- Assign Role to the ADF user
GRANT ROLE ADF_COMPANY_NAME TO USER ADF_COMPANY_NAME_DEV;

-- Update default warehouse if needed
ALTER USER ADF_COMPANY_NAME_DEV SET DEFAULT_WAREHOUSE = 'ANALYTICS_COMPANY_NAME';

-- Remove login password if using key-based auth
ALTER USER ADF_COMPANY_NAME_DEV UNSET PASSWORD;

------------------------------------------------
-- Create CATALOG_USER_TEST for catalog reading
------------------------------------------------
CREATE USER IF NOT EXISTS CATALOG_USER_TEST
  PASSWORD = 'sada123'
  LOGIN_NAME = 'catalog_user_test'
  MUST_CHANGE_PASSWORD = FALSE
  DEFAULT_WAREHOUSE = 'SNOWFLAKE'
  DEFAULT_ROLE = 'CATALOG_READ_ONLY'
  COMMENT = 'User for collector tests';

-- Grant read-only catalog access
GRANT ROLE CATALOG_READ_ONLY TO USER KADA_CATALOG_USER_TEST;

-- Grant same role to a corporate user
GRANT ROLE CATALOG_READ_ONLY TO USER "DANIEL.AGUAYO@COMPANY_NAMESERVICOMPANY_NAME.COM.AU";

------------------------------------------------
-- RSA Key Setup (for secure programmatic access)
------------------------------------------------

-- Remove any existing RSA key for a user if required
ALTER USER ADF_COMPANY_NAME_DEV UNSET RSA_PUBLIC_KEY;

-- Set RSA key for a secure user (must be one line, remove line breaks)
ALTER USER ADF_COMPANY_NAME_SEC SET RSA_PUBLIC_KEY = 'MI---sample ---QAB';

-- Confirm public key fingerprint (useful for verification)
SELECT TRIM((SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  WHERE "property" = 'RSA_PUBLIC_KEY_FP'), 'SHA256:');

------------------------------------------------
-- Grant compute access to ADF role
------------------------------------------------
GRANT USAGE ON WAREHOUSE ETL_COMPANY_NAME_ADF TO ROLE ADF_COMPANY_NAME_SEC;
GRANT OPERATE ON WAREHOUSE ETL_COMPANY_NAME_ADF TO ROLE ADF_COMPANY_NAME_SEC;

------------------------------------------------
-- Miscellaneous Checks
------------------------------------------------
-- List all users
SHOW USERS;

-- Get account identifier (useful for Snowflake OAuth/Azure setups)
SELECT LOWER(CONCAT_WS('-', CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME())) AS account_identifier;
