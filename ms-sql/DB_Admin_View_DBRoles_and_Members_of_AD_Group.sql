--View DB roles and its members

SELECT DP1.name AS DatabaseRoleName,   
   isnull (DP2.name, 'No members') AS DatabaseUserName   
 FROM sys.database_role_members AS DRM  
 RIGHT OUTER JOIN sys.database_principals AS DP1  
   ON DRM.role_principal_id = DP1.principal_id  
 LEFT OUTER JOIN sys.database_principals AS DP2  
   ON DRM.member_principal_id = DP2.principal_id  
WHERE DP1.type = 'R'
ORDER BY DP1.name; 





--View Role access details
--Note: the script excludes public role to limit the number of rows for easier read


SELECT pr.name, pr.type_desc,  ss.name as 'SCHEMA',  
    pr.authentication_type_desc, pe.state_desc,   
    pe.permission_name, s.name + '.' + o.name AS ObjectName   
FROM sys.database_principals AS pr  
JOIN sys.database_permissions AS pe  
    ON pe.grantee_principal_id = pr.principal_id  
LEFT JOIN sys.objects AS o  
    ON pe.major_id = o.object_id  
LEFT JOIN sys.schemas AS s  
    ON o.schema_id = s.schema_id 
LEFT JOIN sys.schemas ss 
              ON pe.major_id = ss.schema_id 
where pr.name<>'public'


 
-- 1. Create a user in the DB from an AD group
CREATE USER [DATAHUB_ANALYTICS_READER_PROD] FROM EXTERNAL PROVIDER;

-- 2. Create a role in the DB
CREATE ROLE [DATAHUB_ANALYTICS_READER_TEST_TEMP];

-- 3. Grant the role permissions to access a specific schema
GRANT SELECT ON SCHEMA::[DATA_REPORTS] TO [DATAHUB_ANALYTICS_READER_TEST_TEMP];

-- 4. Assign the user to the role
ALTER ROLE [DATAHUB_ANALYTICS_READER_TEST_TEMP]
    ADD MEMBER [DATAHUB_ANALYTICS_READER_PROD];
GO

-- Instruct user to test access

-- Cleanup: Remove user and role
DROP USER [DATAHUB_ANALYTICS_READER_PROD];
DROP ROLE [DATAHUB_ANALYTICS_READER_TEST