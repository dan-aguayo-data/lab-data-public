

select  * from  poc_raw.bronze_kinesis_data limit 1;

WITH records_per_minute AS (
    SELECT 
        DATE_TRUNC('minute', approximateArrivalTimestamp) AS arrival_minute,
        COUNT(*) AS records_per_minute
    FROM 
        poc_raw.bronze_kinesis_data
    GROUP BY 
        DATE_TRUNC('minute', approximateArrivalTimestamp)
)

SELECT 
    arrival_minute,
    records_per_minute,
    records_per_minute * 1144 AS bytes_per_minute  -- Assuming average record size is 1144 bytes
FROM 
    records_per_minute
WHERE 
    arrival_minute >= '2024-11-15T06:24:00.000' --and arrival_minute <= '2024-11-15T06:35:00.000'
ORDER BY 
    arrival_minute DESC
LIMIT 100;


