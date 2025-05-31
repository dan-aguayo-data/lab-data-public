SELECT DP1.name AS DatabaseRoleName,   
   isnull (DP2.name, 'No members') AS DatabaseUserName   
 FROM sys.database_role_members AS DRM  
 RIGHT OUTER JOIN sys.database_principals AS DP1  
   ON DRM.role_principal_id = DP1.principal_id  
 LEFT OUTER JOIN sys.database_principals AS DP2  
   ON DRM.member_principal_id = DP2.principal_id  
WHERE DP1.type = 'R'
ORDER BY DP1.name; 


go


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


go




--REVOKE AND DELETE USER 


REVOKE CONNECT FROM GRP_PRW_NEBULA_FMES_READER_UAT
GO

DROP USER GRP_PRW_NEBULA_FMES_READER_UAT

GO

--ADD NEW USER


CREATE USER [PRW_NEBULA_FMES_READER_UAT] FROM EXTERNAL PROVIDER  

GO

ALTER ROLE [PRW-NEBULA_ZZINT_FMES_READER_ROLE]
       ADD MEMBER [PRW_NEBULA_FMES_READER_UAT]
GO