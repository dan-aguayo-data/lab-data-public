SHOW ROLES;

-- Set up the marketing database
USE ROLE SYSADMIN;

CREATE DATABASE MARKETING;
CREATE SCHEMA SALES;
CREATE TABLE MARKETING.SALES.CAMPAIGN (ID INT, COST NUMERIC,SALES_AMOUNT NUMERIC);

-- Use USERADMIN or SECURITYADMIN to set up role
USE ROLE SECURITYADMIN;

-- Create role and user
CREATE ROLE MARKETING_ADMIN;

CREATE USER INITIAL_USER
  PASSWORD = 'AbC201§#';   --- Use AD Grroup or Azure ID instead

-- Assign user  
GRANT ROLE MARKETING_ADMIN TO USER INITIAL_USER;

-- Assign role to SYSADMIN
GRANT ROLE MARKETING_ADMIN TO ROLE SYSADMIN;

-- Grant privileges
GRANT USAGE ON DATABASE MARKETING TO ROLE MARKETING_ADMIN;
GRANT USAGE ON SCHEMA MARKETING.SALES TO ROLE MARKETING_ADMIN;
GRANT SELECT ON TABLE  MARKETING.SALES.CAMPAIGN TO ROLE MARKETING_ADMIN;
GRANT INSERT ON TABLE  MARKETING.SALES.CAMPAIGN TO ROLE MARKETING_ADMIN;


-- Assign warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MARKETING_ADMIN;

-- Create tables
GRANT CREATE TABLES ON SCHEMA MARKETING.SALES TO ROLE MARKETING_ADMIN;

-- SELECT on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA  MARKETING.SALES TO ROLE MARKETING_ADMIN;

-- SELECT on all future tables
GRANT SELECT ON FUTURE TABLES IN SCHEMA MARKETING.SALES TO ROLE MARKETING_ADMIN;

-- Revoke USAGE on database
REVOKE USAGE ON SCHEMA MARKETING.SALES FROM ROLE MARKETING_ADMIN;

GRANT OWNERSHIP ON SCHEMA MARKETING.SALES TO ROLE MARKETING_ADMIN;

-- What are the privileges of the role?
SHOW GRANTS TO ROLE DBT_CES_SA;

-- To whom was the role assigned?
SHOW GRANTS OF ROLE MARKETING_ADMIN;


-- Drop USER and Database
USE ROLE SYSADMIN;
DROP DATABASE MARKETING;

USE ROLE SECURITYADMIN;
DROP ROLE MARKETING_ADMIN;
DROP USER INITIAL_USER;




WITH All_Roles_And_Privileges AS (
    SELECT
        Created_On,
        Modified_On,
        Privilege,
        Granted_On,
        Table_Catalog,
        Table_Schema,
        Name AS Name_Granted,
        Granted_To,
        Grantee_Name AS Grantee_Role_Name,
        Deleted_On
    FROM
        Snowflake.Account_Usage.Grants_To_Roles
),
Roles_Assigned_To_Users AS (
    SELECT
        Created_On,
        Deleted_On,
        Role AS Role_Granted,
        Granted_To,
        Grantee_Name AS User_Name,
        Granted_By
    FROM
        Snowflake.Account_Usage.Grants_To_Users
)
SELECT
    t.*,
    t2.User_Name
FROM
    All_Roles_And_Privileges t
LEFT JOIN
    Roles_Assigned_To_Users t2
    ON t.Grantee_Role_Name = t2.Role_Granted
WHERE
    t.Deleted_On IS NULL
    AND t2.Deleted_On IS NULL

;
