select * from [SYS_NEBULA].[File_ReplicationMasterList] WHERE PROJECT_NAME = 'ANAPLAN_EXTRACTS'


declare @P_Environment as nvarchar(4)

set @P_Environment = 
(
  select case 
    when DB_NAME() = 'SQLDB-PRW-NBL-PROD' then ''
    when DB_NAME() = 'SQLDB-PRW-NBL-UAT' then 'uat'
    else 'dev' end
)

INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Warehouse 0'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/Australia/'
           ,'Export Warehouse 0'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/Monthly Forecast Export Warehouse/Australia/'
           ,'Export Warehouse 0')

		   
INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Warehouse 0'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/NewZealand/'
           ,'Export Warehouse 0'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/Monthly Forecast Export Warehouse/NewZealand/'
           ,'Export Warehouse 0')



		   
INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Warehouse 0'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/PacificTR/'
           ,'Export Warehouse 0'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/Monthly Forecast Export Warehouse/PacificTR/'
           ,'Export Warehouse 0')


	   
INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Fcst Neo Output'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/Australia/'
           ,'Export Fcst Neo Output'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/FcstW txt Export - Neo Output/Australia/'
           ,'Export Fcst Neo Output')


		   
	   
INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Fcst Neo Output'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/NewZealand/'
           ,'Export Fcst Neo Output'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/FcstW txt Export - Neo Output/NewZealand/'
           ,'Export Fcst Neo Output')



		   
	   
INSERT INTO [SYS_NEBULA].[File_ReplicationMasterList]
           ([PROJECT_NAME]
           ,[REPLICATION_DESCRIPTION]
           ,[ACTIVE]
           ,[FREQUENCY]
           ,[FILE_TO_SQLDB]
           ,[SQLDB_TO_FILE]
           ,[STL_TO_STL]
           ,[SOURCE_CONTAINER_NAME]
           ,[SOURCE_FOLDER_NAME]
           ,[SOURCE_FILE_NAME]
           ,[SOURCE_SQL_SCHEMA]
           ,[SOURCE_SQL_OBJECT]
           ,[DESTINATION_SQL_SCHEMA]
           ,[DESTINATION_SQL_OBJECT]
           ,[DESTINATION_CONTAINER_NAME]
           ,[DESTINATION_FOLDER_NAME]
           ,[DESTINATION_FILE_NAME])
     VALUES
           ('ANAPLAN_EXTRACTS'
           ,'Export Fcst Neo Output'
           ,'Y'
           ,'Daily'
           ,'N'
           ,'N'
           ,'Y'

		   ,concat('prw-', @P_Environment, '-datalake-landing')
           ,'prw/anaplan/PacificTR/'
           ,'Export Fcst Neo Output'
           ,null
           ,null
           ,null
           ,null
           ,concat('prw-', @P_Environment, '-datalake-project-production')
           ,'prw/Anaplan/Anaplan_Exports/FcstW txt Export - Neo Output/PacificTR/'
           ,'Export Fcst Neo Output')


		   select * from [SYS_NEBULA].[File_ReplicationMasterList] WHERE PROJECT_NAME = 'ANAPLAN_EXTRACTS'


		

GO

DROP TABLE [SYS_NEBULA].[ADFPipeline_Exports_ANAPLAN]


GO