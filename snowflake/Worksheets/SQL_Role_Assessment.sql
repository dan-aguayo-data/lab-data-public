

GRANT ROLE GOVERNANCE_CES TO ROLE

;

select * from snowflake.account_usage.roles where deleted_on is null order by created_on
;
USE DATABASE SNOWFLAKE ;

;
select * from information_schema.enabled_roles er

;

--GRANTEE NAME -- 
WITH role_hierarchy AS (
    SELECT 
        grantee_name AS role,
        granted_to AS parent_role
    FROM 
        snowflake.account_usage.grants_to_roles
    WHERE 
        privilege = 'USAGE'
)
SELECT 
    role,
    parent_role
FROM 
    role_hierarchy
ORDER BY 
    role;


USE ROLE SF_DEV_CES;
    
;

WITH RECURSIVE role_hierarchy AS (
    SELECT
        role_name,
        role_owner AS parent_role
    FROM
        information_schema.enabled_roles
    WHERE
        role_owner is not NULL

    UNION ALL

    SELECT
        er.role_name,
        rh.role_name AS parent_role
    FROM
        information_schema.enabled_roles er
        JOIN role_hierarchy rh ON er.role_owner = rh.role_name
)
SELECT
    role_name,
    parent_role
FROM
    role_hierarchy
ORDER BY
    parent_role,
    role_name
    
    ;
    

    SELECT
        role_name,
        role_owner AS parent_role
    FROM
        information_schema.enabled_roles
    WHERE
        role_name = 'ADMIN'

;
        
;

/*GRANTS TO ROLES BY TABLE AND TABLE SCHEMA AND PRIVILIEGE */

SELECT TOP 10 * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES

;

SELECT  * FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES WHERE DELETED_ON IS NULL AND ROLE_TYPE <> 'INSTANCE_ROLE' ORDER BY ROLE_ID

;

SELECT TOP 10 * FROM SNOWFLAKE.ACCOUNT_USAGE.USERS

-- ;
-- SELECT TOP 10 * FROM SNOWFLAKE.ACCOUNT_USAGE.CLASSES
-- ;
-- SELECT TOP 10 * FROM SNOWFLAKE.ACCOUNT_USAGE.CLASS_INSTANCES

--BASIC GRANTS FOR PRIVILEGES EXAMPLE--

// Create a role named Marketing
Create Role "Marketing";
// Grant this new role to SYSADMIN so that SYSADMIN can manage it.
Grant Role "Marketing" to Role "SYSADMIN";
// Now grant to role to a user
Grant Role "Marketing" to User LaurenMinder;
// Users need to have access to a warehouse, which will give them computing power
// to perform queries in Snowflake.
Grant Operate on Warehouse Compute_XSmall to role "Marketing";
// Grant Select on all tables in a target database and schema and assign to the
// marketing role.
Grant Select on All Tables in Schema <database name>.<schema_name> to role Marketing;
// Grant Select on all future tables.
Grant Select on Future Tables in Schema <database name>.<schema_name> to role Marketing;


USE ROLE SF_ADMIN;

/*ROLE PRIVILEGES  ---------------------------------------------------------------------------------------------------------------------------------------*/
/* ---------------------------------------------------------------------------------------------------------------------------------------*/
/* ---------------------------------------------------------------------------------------------------------------------------------------*/


With All_Roles_And_Privileges AS
(
  Select 
    Created_On,                
    Modified_On,                
    Privilege,                 
    Granted_On,              
    Table_Catalog,             
    Table_Schema,              
    Name as Name_Granted,
    Granted_To,                
    Grantee_Name Grantee_Role_Name,  
    Deleted_On                
  From Snowflake.Account_Usage.Grants_To_Roles 
),
Roles_Assigned_To_Users AS
(
    Select
        Created_On,                 
        Deleted_On,                
        Role as Role_Granted,         
        Granted_To,                 
        Grantee_Name as User_Name, 
        Granted_By                  
    From Snowflake.Account_Usage.Grants_To_Users 
)
Select t.*, t2.User_Name
From All_Roles_And_Privileges t
Left Join Roles_Assigned_To_Users t2 
On t.Grantee_Role_Name = t2.Role_Granted
Where t.Deleted_On IS NULL And t2.Deleted_On IS NULL 
and t.Grantee_Role_Name = 'DBT_CES_SA' 
--and t.Granted_On = 'USER'



;

;

(Select  name as Role_Granted, Grantee_Name as Grantee_Role_Name From Snowflake.Account_Usage.Grants_To_Roles where grantee_name ='SYSADMIN'and granted_on = 'ROLE' and deleted_on is null ) sadmin


;


;

-- GRANT USAGE ON SCHEMA SNOWFLAKE.INFORMATION_SCHEMA TO ROLE SF_ADMIN;
-- GRANT SELECT ON ALL TABLES IN SCHEMA  SNOWFLAKE.INFORMATION_SCHEMA TO ROLE SF_ADMIN;




    ;


/*ROLE HIERARCHY LEVELS ---------------------------------------------------------------------------------------------------------------------------------------*/
/* ---------------------------------------------------------------------------------------------------------------------------------------*/
/* ---------------------------------------------------------------------------------------------------------------------------------------*/
;
select * from snowflake.account_usage.roles r where role_type <> 'INSTANCE_ROLE'
;

select  
r.created_on, 
r.name as role_name, 
r.comment,
r.owner,
r.role_type,
ru.user_count as user_grants_count,
rr.role_count as role_grants_count,
case when sadmin.sadmin_flag is null then 0 when r.name ='SYSADMIN' then 1 else sadmin.sadmin_flag end as sadmin_flag
from snowflake.account_usage.roles r 
left join (SELECT  role as role_name, count(*) as user_count  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS where deleted_on is null and granted_to = 'USER' group by role) ru on r.name = ru.role_name
left join (select name as role_name, count(*) as role_count from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES where deleted_on is null  and granted_on = 'ROLE' and privilege = 'USAGE' group by name) rr on r.name = rr.role_name
left join (Select distinct name as Role_Granted, Grantee_Name as Grantee_Role_Name, 1 as sadmin_flag From Snowflake.Account_Usage.Grants_To_Roles where grantee_name ='SYSADMIN'and granted_on = 'ROLE' and deleted_on is null ) sadmin 
on r.name = sadmin.Role_Granted
WHERE r.deleted_on is null and  role_type <> 'INSTANCE_ROLE'

;

select    r.created_on,   r.name as role_name,   r.comment,  r.owner,  r.role_type,  ru.user_count as user_grants_count,  rr.role_count as role_grants_count,  case when sadmin.sadmin_flag is null then 0 when r.name ='SYSADMIN' then 1 else sadmin.sadmin_flag end as sadmin_flag  from snowflake.account_usage.roles r -- and  --and role_type <> 'INSTANCE_ROLE'  left join (SELECT  role as role_name, count(*) as user_count  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS where deleted_on is null and granted_to = 'USER' group by role) ru on r.name = ru.role_name  left join (select name as role_name, count(*) as role_count from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES where deleted_on is null  and granted_on = 'ROLE' and privilege = 'USAGE' group by name) rr on r.name = rr.role_name  left join (Select distinct name as Role_Granted, Grantee_Name as Grantee_Role_Name, 1 as sadmin_flag From Snowflake.Account_Usage.Grants_To_Roles where grantee_name ='SYSADMIN'and granted_on = 'ROLE' and deleted_on is null ) sadmin   on r.name = sadmin.Role_Granted  WHERE r.deleted_on is null and  role_type <> 'INSTANCE_ROLE'
;

SELECT created_on, privilege, name as role_name, grantee_name  FROM  SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
WHERE
--privilege = 'USER' AND
granted_on = 'ROLE' AND
granted_to = 'ROLE' AND
AND prvilege = 'usage'
deleted_on is null



;
show roles
;


-- create test role
CREATE ROLE CES_TEST
COMMENT = 'This is a test role' -- Add a comment to describe the role
;

GRANT ROLE CES_TEST TO ROLE SYSADMIN ; 
;

DROP ROLE   CES_TEST
;









select * from account_usage.USERS  where name = 'GOVERNANCE_CES';












;

select top 100 * from information_schema.enabled_roles --

;

SELECT
*
FROM
SNOWFLAKE.ACCOUNT_USAGE.USERS

;

SELECT  role as role_name, count(*)  FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS where deleted_on is null and granted_to = 'USER' group by role --and --and role_name = 'FIVETRAN_COEX' 

;

select * from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES where deleted_on is null  and granted_on = 'ROLE' --and privilege = 'USAGE' group by name

-- Grant usage on the schemas
GRANT USAGE ON SCHEMA SNOWFLAKE.ACCOUNT_USAGE TO ROLE GOVERNANCE_CES;
GRANT USAGE ON SCHEMA SNOWFLAKE.INFORMATION_SCHEMA TO ROLE GOVERNANCE_CES;

-- Grant select privileges on all tables in ACCOUNT_USAGE
GRANT SELECT ON ALL VIEWS IN SCHEMA SNOWFLAKE.ACCOUNT_USAGE TO ROLE GOVERNANCE_CES;

-- Grant select privileges on all tables in INFORMATION_SCHEMA
GRANT SELECT ON ALL TABLES IN SCHEMA SNOWFLAKE.INFORMATION_SCHEMA TO ROLE GOVERNANCE_CES;

-- Optionally, you can grant select privileges on future tables in these schemas
GRANT SELECT ON FUTURE TABLES IN SCHEMA SNOWFLAKE.ACCOUNT_USAGE TO ROLE GOVERNANCE_CES;
GRANT SELECT ON FUTURE TABLES IN SCHEMA SNOWFLAKE.INFORMATION_SCHEMA TO ROLE GOVERNANCE_CES;
