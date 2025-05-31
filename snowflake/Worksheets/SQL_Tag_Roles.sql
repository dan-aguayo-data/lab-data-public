/*CREATE ROLE AND COMMENT ON IT*/
USE ROLE SECURITYADMIN ;
CREATE ROLE TEST_TAGS 
COMMENT = 'This role is to test tags on roles and should be deleted soon';

/*GRANT ROLE PRIVILEGES*/
GRANT USAGE ON DATABASE RAW_DEV TO ROLE TEST_TAGS; 
GRANT ROLE TEST_TAGS TO ROLE SYSADMIN ; 
GRANT ROLE TEST_TAGS TO USER "DANIEL.AGUAYO@COEXSERVICES.COM.AU" ;

/*CHECK THE ROLE WAS CREATED AS EXPECTED, NOTE THAT ACCOUNT USAGA MIGHT TAKE A FEW MIN TO UPDATE*/
USE ROLE SYSADMIN; 
select * from snowflake.account_usage.roles where deleted_on is null order by created_on desc;

/* IF NEEDED, CREATE A TAG */
USE DATABASE CES_GOVERNANCE ;
USE SCHEMA TAGS;
CREATE TAG TagsforRolesTest
COMMENT = 'This is a tag test for Role' --Specify you are using the tag for roles;

/* ASSIGN TAG TO ROLE */
USE ROLE ACCOUNTADMIN; 
ALTER ROLE TEST_TAGS SET TAG TagsforRolesTest = 'TagsforRolesTest';
USE ROLE SYSADMIN;

/* REVIEW TAG WAS ASSIGNED AS EXPECTED */
SHOW TAGS;
select get_ddl('tag', 'TagsforRolesTest') ;
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES ;
SELECT * FROM  SNOWFLAKE.ACCOUNT_USAGE.TAGS where deleted is null ;

/*FIND YOUR ASSIGNED TAGS */
SELECT * FROM  SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES where object_deleted is null and domain = 'ROLE';
;

/* DROP ROLE OR TAGS AS NEEDED */

drop tag TagsforRolesTest ;
drop role TEST_TAGS; 


-- Query the TAG_REFERENCES view (in Account Usage) to determine the tag assignments.

-- Unset the tag from the object or column. For example:

-- For objects, use the corresponding ALTER <object> ... UNSET TAG command.

-- For a table or view column, use the corresponding ALTER { TABLE | VIEW } ... { ALTER | MODIFY } COLUMN ... UNSET TAG command.

-- Drop the tag using a DROP TAG statement.
--select get_ddl('tag', 'cost_center')

-- alter tag cost_center
--     add allowed_values 'marketing';