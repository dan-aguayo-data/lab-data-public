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


 
-- 1.  Create a user in the DB and give them access to a certain schema... User is Always a AD group that has been set up.
CREATE USER [PRW_NEBULA_DWH_FINANCE_READER_PROD] FROM EXTERNAL PROVIDER  -- PRW_NEBULA_ZZINT_ANAPLAN_READER_PROD    --PRW_NEBULA_REVUP_READER_PROD


-- 2.  Create Role in the DB
CREATE ROLE [PRW-NEBULA_DWH_FINANCE_READER_UAT_TEMP]
 
-- 3.  Give the role permissiosn to access a certain schema
 
GRANT SELECT ON SCHEMA::[DWH_SALES] TO [PRW-NEBULA_DWH_FINANCE_READER_UAT_TEMP]

 
-- 4.  Assign the User to the relevant roles
 
ALTER ROLE [PRW-NEBULA_DWH_FINANCE_READER_UAT_TEMP]
       ADD MEMBER [PRW_NEBULA_DWH_FINANCE_READER_PROD]
GO
 
-- Done get user to test the access


DROP USER  [PRW_NEBULA_DWH_FINANCE_READER_PROD]

DROP ROLE [PRW-NEBULA_DWH_FINANCE_READER_UAT_TEMP]

GO