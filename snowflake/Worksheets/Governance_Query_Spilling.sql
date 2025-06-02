/*SPILL ANALYSIS */

use role sysadmin;

use database snowflake;

select  
--query_id,
--query_text, 
database_name, 
warehouse_name,
--start_time,
sum(total_elapsed_time)/1000 elapsed_time_secs, 
sum(bytes_scanned)/1000000 mb_scanned,
sum(bytes_written)/1000000 mb_written, 
-- rows_inserted,
-- rows_deleted,
-- rows_updated,
-- rows_unloaded, 
sum(bytes_deleted)/1000000 mb_deleted, 
sum(bytes_spilled_to_local_storage)/1000000 mb_spilled_local_storage,  -- Number of bytes spilled to local storage during query execution (often due to insufficient memory).
sum(bytes_spilled_to_remote_storage)/1000000 mb_spilled_remote_storage,
sum(bytes_sent_over_the_network)/1000000 mb_sent_over_network,
sum(credits_used_cloud_services)/1000000 cloud_credits,

from  SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY where user_name = 'USER'
and start_time >= dateadd(day,-10,current_date)
group by
database_name, 
warehouse_name
order by 7 ;

/*USED IN POWER BI */

select warehouse_name
,      warehouse_size
--,      database_name
,      round(sum(total_elapsed_time)/1000/60/60) as last7d_elapsed_hrs
,      round(sum(execution_time)/1000/60/60) as last7d_execution_hrs
,       count(*) as last7d_count_queries
,      count(iff(bytes_spilled_to_local_storage/1024/1024/1024 > 1,1,null)) as last7d_count_spilled_queries
,      round(sum(bytes_spilled_to_local_storage/1024/1024/1024))  as last7d_spilled_to_local_gb
,      round(sum(bytes_spilled_to_remote_storage/1024/1024/1024)) as last7d_spilled_to_remote_gb
,round(sum(queued_provisioning_time)/1000/60/60) as last7d_queued_provisioning_hrs
, round(sum(queued_repair_time)/1000/60/60) as last7d_queued_repair_time_hrs
from Snowflake.account_usage.query_history
where warehouse_size is not null and CONVERT_TIMEZONE('UTC', 'Australia/Sydney',start_time) >=  CONVERT_TIMEZONE('UTC', 'Australia/Sydney',dateadd(day,-7,current_date))
group by 1, 2 -- ,3
having last7d_spilled_to_local_gb > 1
order by 7 desc


;
/*LAST DAY*/
select warehouse_name
,      warehouse_size
--,      database_name
,      round(sum(total_elapsed_time)/1000/60/60) as last7d_elapsed_hrs
,      round(sum(execution_time)/1000/60/60) as last7d_execution_hrs
,       count(*) as last7d_count_queries
,      count(iff(bytes_spilled_to_local_storage/1024/1024/1024 > 1,1,null)) as last7d_count_spilled_queries
,      round(sum(bytes_spilled_to_local_storage/1024/1024/1024))  as last7d_spilled_to_local_gb
,      round(sum(bytes_spilled_to_remote_storage/1024/1024/1024)) as last7d_spilled_to_remote_gb
,round(sum(queued_provisioning_time)/1000/60/60) as last7d_queued_provisioning_hrs
, round(sum(queued_repair_time)/1000/60/60) as last7d_queued_repair_time_hrs
from Snowflake.account_usage.query_history
where warehouse_size is not null and CONVERT_TIMEZONE('UTC', 'Australia/Sydney',start_time) >=  CONVERT_TIMEZONE('UTC', 'Australia/Sydney',dateadd(day,-7,current_date))
group by 1, 2 -- ,3
having last7d_spilled_to_local_gb > 1
order by 7 desc
WHERE CONVERT_TIMEZONE('UTC', 'Australia/Sydney',USAGE_DATE) = '2024-05-27'


---any query that spills more than a gigabyte of storage is a candidate to run on a larger warehouse.  Typically, if a query includes significant spilling to storage is will execute twice as fast for the same cost on the next size virtual warehouse

select top 10 * from from Snowflake.account_usage.w; 

