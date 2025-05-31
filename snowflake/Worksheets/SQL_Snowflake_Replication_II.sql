CREATE TABLE LANDING_ORACLE.ORACLE_UAT.DimDate (
    DateKey INT PRIMARY KEY,
    Date DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    DayOfWeek VARCHAR(10) NOT NULL
);

INSERT INTO LANDING_ORACLE.ORACLE_UAT.DimDate (DateKey, Date, Year, Quarter, Month, Day, DayOfWeek) VALUES
(20230101, '2023-01-01', 2023, 1, 1, 1, 'Sunday'),
(20230102, '2023-01-02', 2023, 1, 1, 2, 'Monday'),
(20230103, '2023-01-03', 2023, 1, 1, 3, 'Tuesday');

INSERT INTO LANDING_ORACLE.ORACLE_UAT.DimDate (DateKey, Date, Year, Quarter, Month, Day, DayOfWeek, MULTI_SCHEME_ID) VALUES
(20230104, '2023-01-04', 2023, 1, 1, 4, 'Sunday', 10000)


select * from LANDING_ORACLE.ORACLE_UAT.DimDate

select * from LANDING_ORACLE.ORACLE_UAT.DimDate
show replication accounts;

show replication groups;

show replication databases;

show accounts



use role accountadmin;

create replication group landing_oracle_replication_group
    object_types = databases
    allowed_databases = LANDING_ORACLE
    allowed_accounts = BP43112.HR73515
    REPLICATION_SCHEDULE = '{ USING CRON <expr> <time_zone> }'
    --REPLICATION_SCHEDULE = '10 minutes'
    IGNORE EDITION CHECK
    ;

    ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = '5 MINUTE'

    ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = '10 MINUTE'
    
    ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = 'USING CRON 20 06 */4 * * UTC'

     [ REPLICATION_SCHEDULE = 'USING CRON <expr> <time_zone> }'
;

    
    DATEKEY, DATE, YEAR, QUARTER, MONTH, DAY, DAYOFWEEK

alter table LANDING_ORACLE.ORACLE_UAT.DIMDATE add (MULTI_SCHEME_ID NUMBER(38,0))

alter database LANDING_ORACLE refresh

    show replication groups;

BP43112.HR73515, BP43112.HU99273



alter replication group landing_oracle_replication_group suspend;

ALTER DATABASE LANDING_ORACLE ENABLE REPLICATION TO ACCOUNTS BP43112.HR73515 IGNORE EDITION CHECK


ALTER DATABASE LANDING_ORACLE DISABLE REPLICATION TO ACCOUNTS BP43112.HR73515


select SYSTEM$DISABLE_DATABASE_REPLICATION('LANDING_ORACLE')


--azure

create database LANDING_ORACLE as replica of BP43112.HU99273.LANDING_ORACLE;

CREATE DATABASE <name>AS REPLICA OF <account_identifier>.<primary_db_name>[ DATA_RETENTION_TIME_IN_DAYS = <integer> ]


CREATE REPLICATION GROUP landing_oracle_replication_group AS REPLICA OF BP43112.HU99273.landing_oracle_replication_group


;
show replication groups;

select * from LANDING_ORACLE.ORACLE_UAT.DIMDATE


ALTER REPLICATION GROUP IF EXISTS landing_oracle_replication_group SUSPEND IMMEDIATE;

ALTER REPLICATION GROUP IF EXISTS landing_oracle_replication_group RESUME;


    
    
show replication groups;
    
ALTER REPLICATION GROUP IF EXISTS landing_oracle_replication_group SUSPEND IMMEDIATE;

    ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = '18 HOUR' ; 

ALTER REPLICATION GROUP IF EXISTS landing_oracle_replication_group RESUME;


        ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = '5 MINUTE';



select
        current_timestamp()  ; 

/* LAST USED CRON */

 ALTER REPLICATION GROUP landing_oracle_replication_group SET REPLICATION_SCHEDULE = 'USING CRON 54 6 */4 * * UTC';


/* MANUALLY REFRESH REPLICATION */

 ALTER REPLICATION GROUP IF EXISTS landing_oracle_replication_group REFRESH;
