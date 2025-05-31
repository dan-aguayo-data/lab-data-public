--######How to calculate SQL Azure Storage per client in your DB ######
-- Calculates the size of individual database objects.   

SELECT sys.objects.name, SUM(reserved_page_count) * 8.0 / 1024 MB_size
FROM sys.dm_db_partition_stats, sys.objects
WHERE sys.dm_db_partition_stats.object_id = sys.objects.object_id
GROUP BY sys.objects.name
ORDER BY SUM(reserved_page_count) * 8.0 / 1024 desc
;
GO