/** STEP 1. CREATE SHARED ACCESS CREDENTIAL **/
CREATE DATABASE SCOPED CREDENTIAL CloudStorageCredential
WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
SECRET = '---SECRET HERE --';

/** STEP 2. CREATE EXTERNAL DATA SOURCE TO STORAGE CONTAINER **/
CREATE EXTERNAL DATA SOURCE MyCloudStorage
WITH (
    TYPE = BLOB_STORAGE,
    LOCATION = ' --BLOB URL HERE -----',
    CREDENTIAL = CloudStorageCredential
);

/** STEP 3. BULK INSERT **/
SET NOCOUNT ON; -- Reduce network traffic
BULK INSERT dbo.ProductSales
FROM 'data/sales_records.txt'
WITH (
    DATA_SOURCE = 'MyCloudStorage',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    BATCHSIZE = 100000 -- Performance optimization
);

SET NOCOUNT ON; -- Reduce network traffic
BULK INSERT dbo.InventoryItems
FROM 'data/inventory_records.txt'
WITH (
    DATA_SOURCE = 'MyCloudStorage',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
    BATCHSIZE = 100000 -- Performance optimization
);

/** OTHER TESTING QUERIES **/
SELECT * 
FROM OPENROWSET(
    BULK 'data/item_list.csv',
    SINGLE_CLOB,
    DATA_SOURCE = 'MyCloudStorage',
    FORMATFILE_DATA_SOURCE = 'MyCloudStorage'
) AS DataFile;

SELECT * 
FROM OPENROWSET(
    BULK 'data/sales_records.txt',
    SINGLE_CLOB,
    DATA_SOURCE = 'MyCloudStorage',
    FORMATFILE_DATA_SOURCE = 'MyCloudStorage'
) AS DataFile1;