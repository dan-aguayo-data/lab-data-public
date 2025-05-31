/* Compression on Table */
EXEC sp_estimate_data_compression_savings 'DATA_HUB_PROCESSING', 'RAW_EVENTS', NULL, NULL, 'ROW';

ALTER TABLE Operations.EventLog
REBUILD PARTITION = ALL
WITH (DATA_COMPRESSION = ROW);
GO

/* Compression on Index */
SELECT 
    name, 
    index_id
FROM sys.indexes
WHERE OBJECT_NAME(object_id) = N'EventLog';

EXEC sp_estimate_data_compression_savings
    @schema_name = 'Storage',
    @object_name = 'EventLog',
    @index_id = 1001,
    @partition_number = NULL,
    @data_compression = 'PAGE';

ALTER INDEX idx_eventlog_recordid
ON Storage.EventLog
REBUILD PARTITION = ALL
WITH (DATA_COMPRESSION = PAGE);
GO