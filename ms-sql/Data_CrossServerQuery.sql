CREATE MASTER KEY; -- create master key
GO
 
-- credential maps to a login or contained user used to connect to remote database 
CREATE DATABASE SCOPED CREDENTIAL CrossDbCred1 -- credential name
WITH IDENTITY = 'pradmin',                    -- login or contained user name
SECRET = 'S7uk4@PD3467!';                    -- login or contained user password
GO




-- data source to remote Azure SQL Database server and database
CREATE EXTERNAL DATA SOURCE NebulaProd
WITH
(
    TYPE=RDBMS,                           -- data source type
    LOCATION='sqlserver-prw-nbl-prod.database.windows.net', -- Azure SQL Database server name
    DATABASE_NAME='SQLDB-PRW-NBL-PROD',         -- database name
    CREDENTIAL=CrossDbCred1                -- credential used to connect to server / database  
);
GO
 
-- external table points to table in an external database with the identical structure
CREATE EXTERNAL TABLE [dbo].[ext_src]
(
    [Id] [int]
)
WITH (DATA_SOURCE = [source],  -- data source 
      SCHEMA_NAME = 'dbo',           -- external table schema
      OBJECT_NAME = 'source'       -- name of table in external database
    );
GO




SELECT *
FROM sys.symmetric_keys
WHERE name LIKE '##MS_DatabaseMasterKey##';


-- external table points to table in an external database with the identical structure
CREATE EXTERNAL TABLE  [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG_PROD](
	[REGION] [nvarchar](50) NOT NULL,
	[EBH] [int] NOT NULL,
	[EBH_DESCRIPTION] [nvarchar](50) NOT NULL,
	[CUSTOMER_GROUP] [nvarchar](50) NOT NULL,
	)WITH (DATA_SOURCE = [NebulaProd],  -- data source 
      SCHEMA_NAME = 'ZZINT_POLARIS',           -- external table schema
      OBJECT_NAME = 'STL_SalesReductionEBHtoCG'       -- name of table in external database
     );
GO



SELECT * FROM  [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG] 

GO


TRUNCATE TABLE [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG] 

GO


INSERT INTO [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG]

SELECT * FROM [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG_PROD] ;

GO


SELECT * FROM  [ZZINT_POLARIS].[STL_SalesReductionEBHtoCG] 

GO