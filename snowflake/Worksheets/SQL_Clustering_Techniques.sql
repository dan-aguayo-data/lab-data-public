

--- OVERLAPPING MICROPARTITIONS (NUMBER OF MICROPARTITION THAT HAVE OVERLAPPING VALUES)

--  CLUSTERING DEPTH (Average depth of overalapping micropartitions in specific columns, in how many micro paritions the value occurs )






--- DEFINE CLUSTERING KEYS  -- 


/* RECLUSTERING HAPPENS AUTOMATICALLY */
--RECLUSTERING.. THIS IS NOT HAPPENING ALWAYS AFTER DEFINING CLUSTERING KEY, THIS HAPPENS AFTER THE PERIODIC RECLUSTING.. 
-- THIS IS DONE AUTOMATICALLY AND WE DON'T NEED TO MAINTAIN THIS... AUTOMATIC RECLUSTERING.. WE ARE USING NO ACTIVE WAREHOUSE.. BUT SERVERLESS FEATUTE.. IT IS ADJUSTING PARTITIONS THAT ARE BENEFITING FROM THIS. NEW PARTITIONS ARE CREATED AND OLD PARTITIONS ARE DELETED. THIS IS CAUSING THE CONSUMPTION OF CREDITS AND WE HAVE ADDITIONAL STORAGE COSTS BECAUSE OLD PARTITIONS ARE MARKED AS DELETED BUT ALSO STAY IN T TRAVEL AND FAIL SAFE. 

-- CLUSTERING IS NOT A GOOD CHOICE FOR EVERY TABLE ..... RECLUSTERING CAN INCREASE THE COST. TRADE OFF OF QUERY PERFOMANCE AND ARE WE WILLING TO TRADE FOR THE COST. 
--https://docs.snowflake.com/en/user-guide/tables-auto-reclustering
;


SHOW TABLES  LIKE 'AWS_RDS_STAGE_CES_MSCM' ;

ALTER TABLE t1 SUSPEND RECLUSTER;

ALTER TABLE t1 RESUME RECLUSTER;

SELECT SYSTEM$ESTIMATE_AUTOMATIC_CLUSTERING_COSTS('AWS_RDS_STAGE_CES_MSCM') ;


/*OTHER NOTES */
--- QUERIES WITH WHERE AND JOINS BENEFIT THE MOST FROM CLUSTER KEYS 

---CARDINALITY TOO LOW OR TOO HIGH IS NOT GOOD (MORE MICROPARTITIONS) .. TO REDUCE CARDINALITY WE CAN USE EXPRESSION DATA ON TIMESTAMP



/* ADD CLUSTER KEYS */


ALTER TABLE t1 CLUSTER BY (c1,c5) ; --- we cluster from left to right from high cardinality to low cardinality, we can also use an expression DATE(timestamp ),

ALTER TABLE t1 DROP CLUSTER KEY ;



/* IMPORTANT SYSTEM FUNCTIONS FOR CLUSTERING */


--SYSTEM$CLUSTERING_INFORMATION ( 'table_name', '(col1,col3)')

select SYSTEM$CLUSTERING_INFORMATION('AWS_RDS_STAGE_CES_MSCM') ;


-- total partition count -- total partitions used on the table
-- constant partition -- ideal parition that can't benefit any further from any other reclustering .. this is good if it is high 
-- average overlaps ---
-- avererage depth --

--partition depth histogram -- we want to have most of the partitions in the lower buckets


--- partitions constant are better. 

"{
  ""cluster_by_keys"" : ""LINEAR(METADATA:\""table-name\""::string, TO_DATE(METADATA:timestamp::TIMESTAMP_NTZ))"",
  ""total_partition_count"" : 514332,
  ""total_constant_partition_count"" : 0,
  ""average_overlaps"" : 514331.0,
  ""average_depth"" : 514332.0,
  ""partition_depth_histogram"" : {
    ""00000"" : 0,
    ""00001"" : 0,
    ""00002"" : 0,
    ""00003"" : 0,
    ""00004"" : 0,
    ""00005"" : 0,
    ""00006"" : 0,
    ""00007"" : 0,
    ""00008"" : 0,
    ""00009"" : 0,
    ""00010"" : 0,
    ""00011"" : 0,
    ""00012"" : 0,
    ""00013"" : 0,
    ""00014"" : 0,
    ""00015"" : 0,
    ""00016"" : 0,
    ""524288"" : 514332
  },
  ""clustering_errors"" : [ ]
}"