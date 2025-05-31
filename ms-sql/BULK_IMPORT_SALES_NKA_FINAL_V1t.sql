
/**STEP 1. CREATE SHARE ACCESS SIGNATURE**// --Remove Initial charater -> Question Mark from Token


CREATE DATABASE SCOPED CREDENTIAL AzureTestCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = 'sv=2020-10-02&st=2022-02-03T21%3A55%3A07Z&se=2022-02-04T21%3A55%3A07Z&sr=c&sp=rl&sig=RLKP8A%2FKQSUxgXd7lGwK9GhnRIzep256%2FquDcsZppkw%3D';


/** STEP 2. CREATE AN EXTERNAL DATA SOURCE TO THE CONTAINER**//  --- Location doesn't have a trailing slash
CREATE EXTERNAL DATA SOURCE MyAzureTest
    WITH (
        TYPE = BLOB_STORAGE,
        LOCATION = 'https://stlprwnbldev.blob.core.windows.net/datalake-dev-datalake-project-production',
        CREDENTIAL = AzureTestCredential
    );

	
	/**SETEP 3. BULK INSERT**/

   SET NOCOUNT ON ---Reduce Network Traffic
BULK INSERT dbo.IRISpirits_NKA_Sales  --Table you created
FROM 'prw/Spirits_Sales.txt'   --Specify what file we want to insert
   WITH 
  (
  DATA_SOURCE = 'MyAzureTest' --Using external Data Source from previous step
   ,FIELDTERMINATOR ='|'
   ,ROWTERMINATOR ='\n'
	,FIRSTROW = 2
	,BATCHSIZE =100000   --PERFORMANCE IMPROVEMENT
	);

	   SET NOCOUNT ON ---REDUCE NETWORK TRAFFIC
BULK INSERT dbo.IRIWine_NKA_Sales
FROM 'prw/Wine_Sales.txt'
   WITH 
  (
  DATA_SOURCE = 'MyAzureTest' 
   ,FIELDTERMINATOR ='|'
   ,ROWTERMINATOR ='\n'
	,FIRSTROW = 2
	,BATCHSIZE =100000   --PERFORMANCE IMPROVEMENT
	);

	/**OTHER TESTING DONE**/

	SELECT * FROM OPENROWSET(
   BULK 'prw/F4101test.csv',
   SINGLE_CLOB,
   DATA_SOURCE = 'MyAzureTest',
  -- FORMAT = 'CSV',
 --  FORMATFILE='invoices.fmt',
   FORMATFILE_DATA_SOURCE = 'MyAzureTest'
   ) AS DataFile;   


   SELECT * FROM OPENROWSET(
   BULK 'prw/Spirits_Sales.txt',
   SINGLE_CLOB,
   DATA_SOURCE = 'MyAzureTest',
  -- FORMAT = 'CSV',
 --  FORMATFILE='invoices.fmt',
   FORMATFILE_DATA_SOURCE = 'MyAzureTest'
   ) AS DataFile1;   
