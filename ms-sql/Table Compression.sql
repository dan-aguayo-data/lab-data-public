

/*Compression on Table */
EXEC sp_estimate_data_compression_savings 'QLIK_EXT_WIPSAU_PROD', 'UUBACPP', NULL, NULL, 'ROW' ;  

ALTER TABLE Production.TransactionHistory REBUILD PARTITION = ALL  
WITH (DATA_COMPRESSION = ROW);   
GO

/* Compression on Index*/
SELECT name, index_id  
FROM sys.indexes  
WHERE OBJECT_NAME (object_id) = N'TransactionHistory';  

EXEC sp_estimate_data_compression_savings   
    @schema_name = 'Production',   
    @object_name = 'TransactionHistory',  
    @index_id = 2,   
    @partition_number = NULL,   
    @data_compression = 'PAGE' ;   

ALTER INDEX IX_TransactionHistory_ProductID ON Production.TransactionHistory REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);  
GO  