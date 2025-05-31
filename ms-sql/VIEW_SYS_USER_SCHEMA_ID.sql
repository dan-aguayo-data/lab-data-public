SELECT state_desc
    ,permission_name
    ,'ON'
    ,class_desc
    ,SCHEMA_NAME(major_id)
    ,'TO'
    ,USER_NAME(grantee_principal_id)
FROM sys.database_permissions AS PERM
JOIN sys.database_principals AS Prin
    ON PERM.major_ID = Prin.principal_id
        AND class_desc = 'SCHEMA'
WHERE major_id = SCHEMA_ID(56)
  AND grantee_principal_id = user_id(1684)
    --AND    permission_name = 'SELECT'
GO


SELECT SCHEMA_ID('DWH_SUPPLYCHAIN');  

SELECT DATABASE_PRINCIPAL_ID();  
GO  

SELECT DATABASE_PRINCIPAL_ID('db_owner');  
GO  



SELECT * FROM sysusers


SELECT name FROM sys.database_principals WHERE
type_desc = 'SQL_USER' AND default_schema_name = 'dbo'


select name as username,
USER_ID as ID,
USER_NAME as ID_Name,
       create_date,
       modify_date,
       type_desc as type,
       authentication_type_desc as authentication_type
from sys.database_principals
where type not in ('A', 'G', 'R', 'X')
      and sid is not null
      and name != 'guest'
order by username;


SELECT DP.name,
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


SELECT DP.name,
DP.principal_id,
DP.type,
DP.type_desc,
DP.default_schema_name,
DP.create_date,
DP.modify_date,
DP.owning_principal_id,
DP.sid,
DP.is_fixed_role,
DP.authentication_type,
DP.authentication_type_desc,
DP.default_language_name,
DP.default_language_lcid
FROM sys.database_principals AS DP;