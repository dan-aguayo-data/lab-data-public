SELECT 
*
FROM sys.views;

SELECT 
name
FROM sys.columns;



SELECT
t.Name,
p.rows,
st.column_count--,
--SUM(a.total_pages) * 8 AS TotalSpaceKB,
--CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS TotalSpaceMB,
--st.Average_row_size * 8 average_row_sizeKB

FROM 

sys.views t


INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN  sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
--INNER JOIN  sys.allocation_units a ON p.partition_id = a.container_id
--LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT OUTER JOIN (SELECT name tablename, 
									COUNT(1) column_count,
									AVG(length) Average_row_size
									FROM   syscolumns
									GROUP BY name) st ON t.name = st.tablename


WHERE t.NAME NOT LIKE 'dt%'AND 
t.is_ms_shipped = 0 AND 
i.OBJECT_ID > 255
GROUP BY t.Name, p.Rows, st.column_count--, st.Average_row_size
ORDER BY  t.Name
