-- Query schema permissions for a specific schema and user
SELECT 
    state_desc,
    permission_name,
    'ON' AS on_keyword,
    class_desc,
    SCHEMA_NAME(major_id) AS schema_name,
    'TO' AS to_keyword,
    USER_NAME(grantee_principal_id) AS grantee
FROM sys.database_permissions AS PERM
JOIN sys.database_principals AS Prin
    ON PERM.major_id = Prin.principal_id
    AND class_desc = 'SCHEMA'
WHERE major_id = SCHEMA_ID(1001) -- Generic schema ID
    AND grantee_principal_id = USER_ID(2001); -- Generic user ID
GO

-- Get schema ID for a specific schema
SELECT SCHEMA_ID('DATA_OPERATIONS');
GO

-- Get current database principal ID
SELECT DATABASE_PRINCIPAL_ID();
GO

-- Get principal ID for a specific role
SELECT DATABASE_PRINCIPAL_ID('db_admin');
GO

-- Query all database users (sysusers)
SELECT * FROM sysusers;
GO

-- Query SQL users with default schema 'dbo'
SELECT name 
FROM sys.database_principals 
WHERE type_desc = 'SQL_USER' 
    AND default_schema_name = 'dbo';
GO

-- Query database principals excluding specific types
SELECT 
    name AS username,
    principal_id AS ID,
    USER_NAME(principal_id) AS ID_Name,
    create_date,
    modify_date,
    type_desc AS type,
    authentication_type_desc AS authentication_type
FROM sys.database_principals
WHERE type NOT IN ('A', 'G', 'R', 'X')
    AND sid IS NOT NULL
    AND name != 'guest'
ORDER BY username;
GO

-- Query database principals with linked server logins
SELECT 
    DP.name,
    DP.principal_id,
    DP.type_desc,
    DP.default_schema_name,
    DP.create_date,
    DP.modify_date,
    SP.name AS login_name,
    SP.type_desc AS login_type_desc
FROM sys.database_principals AS DP
LEFT JOIN sys.server_principals AS SP
    ON DP.sid = SP.sid;
GO

-- Query detailed database principal information
SELECT 
    name,
    principal_id,
    type,
    type_desc,
    default_schema_name,
    create_date,
    modify_date,
    owning_principal_id,
    sid,
    is_fixed_role,
    authentication_type,
    authentication_type_desc,
    default_language_name,
    default_language_lcid
FROM sys.database_principals;
GO