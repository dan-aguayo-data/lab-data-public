SELECT  SC.name as schema_name
          ,AO.name as table_name
          ,IXUS.user_seeks AS NumOfSeeks
          ,IXUS.user_scans AS NumOfScans
          ,IXUS.user_lookups AS NumOfLookups
          ,IXUS.user_updates AS NumOfUpdates
          ,IXUS.last_user_seek AS LastSeek
          ,IXUS.last_user_scan AS LastScan
          ,IXUS.last_user_lookup AS LastLookup
          ,IXUS.last_user_update AS LastUpdate
FROM sys.indexes IX
  LEFT JOIN sys.dm_db_index_usage_stats IXUS 
    ON IXUS.index_id = IX.index_id AND IXUS.object_id = IX.object_id
  LEFT JOIN sys.all_objects AO
    ON AO.object_id = IX.object_id
  LEFT JOIN sys.schemas SC
    ON AO.schema_id = SC.schema_id
WHERE SC.name IN ('TABLE_NAME')
