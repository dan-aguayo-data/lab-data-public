
select * from LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING where ISSUE_NUMBER = 'SDAT-1853'
;

/** Create each JIRA ISSUE as a single field vs multiple fields **/

CREATE SCHEMA LAB_CES.AI_APP_TEST;

GRANT USAGE ON SCHEMA LAB_CES.CES_AI TO ROLE SF_DEV_CES;
GRANT USAGE ON SCHEMA LAB_CES.CES_AI TO ROLE DBT_CES_SA;
GRANT USAGE ON WAREHOUSE AI_CES TO ROLE DBT_CES_SA;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA LAB_CES.CES_AI TO ROLE SF_DEV_CES;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA LAB_CES.CES_AI TO ROLE DBT_CES_SA;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA LAB_CES.CES_AI TO ROLE SYSADMIN;

CREATE OR REPLACE TABLE LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING AS
SELECT 
A.ID AS ISSUE_ID,
CONCAT(' The issue Summary is ', IFNULL(A.SUMMARY,'Issue Summary Unknown')
,' The number reference of the issue is ', IFNULL(A.KEY,'unknown')
, 'The due date of the respective issue is ', IFNULL(TO_CHAR(A.DUE_DATE),'unknown')
, 'The priority number of the respective issue is (5 being the highest 0 being the lowest) ', IFNULL(TO_CHAR(A.PRIORITY),'unknown')
, 'The time spent that has been logged on the respective issue is ', IFNULL(TO_CHAR(A.TIME_SPENT),'unknown')
, 'The resolved date is ', IFNULL(TO_CHAR(A.RESOLVED), 'unknown')
, 'The project name attached to the issue is ',  IFNULL(C.NAME,'unknown')
, 'The person assigned to the issue is ',  IFNULL(D.NAME,'unknown')
,' The person that is the reporter to on the issue is ',  IFNULL(E.NAME,'unknown')) AS issue_information,
 A.KEY AS ISSUE_NUMBER,
    A.SUMMARY AS ISSUE_SUMMARY,
    A.DUE_DATE,
    A.PRIORITY,
    A.TIME_SPENT,
    A.RESOLVED AS RESOLVED_DATE,
    C.NAME AS PROJECT_NAME,
    IFNULL(D.NAME, 'Unassigned') AS ASSIGNEE_NAME, 
    IFNULL(E.NAME, 'Unknown Reporter') AS REPORTER_NAME,
    A.STATUS  
FROM RAW_PROD.CES_JIRA.ISSUE A
LEFT JOIN RAW_PROD.CES_JIRA.PROJECT C ON A.PROJECT = C.ID
LEFT JOIN RAW_PROD.CES_JIRA.USER D ON A.ASSIGNEE = D.ID
LEFT JOIN RAW_PROD.CES_JIRA.USER E ON A.REPORTER = E.ID
WHERE A.SUMMARY IS NOT NULL
;

SELECT * FROM LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING WHERE ISSUE_NUMBER = 'SDAT-1853'


 /** Create the vector table from the wine review single field table **/

 ;
      CREATE or REPLACE TABLE LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING_VECTORS AS 
            SELECT issue_id, issue_information,
             ISSUE_NUMBER,ISSUE_SUMMARY,DUE_DATE,PRIORITY,TIME_SPENT,RESOLVED_DATE,PROJECT_NAME,ASSIGNEE_NAME,REPORTER_NAME,
            snowflake.cortex.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', issue_information) as JIRA_EMBEDDING 
            FROM LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING;

            ;

            SELECT * FROM LAB_CES.CES_AI.JIRA_DATA_SINGLE_STRING_VECTORS WHERE ISSUE_NUMBER = 'SDAT-1853'
;


            WITH context_cte AS (
                SELECT A.ID AS ISSUE_ID, A.KEY AS ISSUE_KEY, A.PARENT_ID, A.UPDATED, A.SUMMARY, 
                       A.DUE_DATE, A.PRIORITY, A.TIME_SPENT, 
                       A.RESOLVED AS RESOLVED_DATE, C.NAME AS PROJECT_NAME, 
                       C.PROJECT_TYPE_KEY, D.NAME AS ASSIGNEE_NAME, E.NAME AS REPORTER_NAME,
                       vector_cosine_similarity(
                           snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', LEFT(A.SUMMARY,500)),  --  Use SUMMARY instead
                           snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', 'sample text to embed')
                       ) AS v_sim
                FROM RAW_PROD.CES_JIRA.ISSUE A
                LEFT JOIN RAW_PROD.CES_JIRA.PROJECT C ON A.PROJECT = C.ID
                LEFT JOIN RAW_PROD.CES_JIRA.USER D ON A.ASSIGNEE = D.ID
                LEFT JOIN RAW_PROD.CES_JIRA.USER E ON A.REPORTER = E.ID
                WHERE A.SUMMARY IS NOT NULL
                AND A.UPDATED >= DATEADD(DAY, -180, CURRENT_DATE) --  Only search last 180 days
                HAVING v_sim > 0.5 
                ORDER BY v_sim DESC
            )
            SELECT V_SIM,ISSUE_ID, ISSUE_KEY, PARENT_ID, UPDATED, SUMMARY,  
                   DUE_DATE, PRIORITY, TIME_SPENT, RESOLVED_DATE, 
                   PROJECT_NAME, PROJECT_TYPE_KEY, ASSIGNEE_NAME, REPORTER_NAME
            FROM context_cte;


;

SELECT A.ID, A.KEY, A.PARENT_ID, A.UPDATED,A.DESCRIPTION, A.SUMMARY, A.DUE_DATE, A.PRIORITY, A.TIME_SPENT,A.RESOLVED AS RESOLVED_DATE,C.NAME AS PROJECT_NAME, C.PROJECT_TYPE_KEY, D.NAME AS ASSIGNEE_NAME, E.NAME AS REPORTER_NAME  FROM RAW_PROD.CES_JIRA.ISSUE A
LEFT JOIN RAW_PROD.CES_JIRA.PROJECT C
ON A.PROJECT = C.ID
LEFT JOIN RAW_PROD.CES_JIRA.USER D
ON A.ASSIGNEE = D.ID
LEFT JOIN RAW_PROD.CES_JIRA.USER E
ON A.REPORTER = E.ID
WHERE SUMMARY IS NOT NULL
;



                WITH context_cte AS (
                    SELECT ID, SUMMARY, DESCRIPTION, SUMMARY,TIME_SPENT, CREATOR
                    FROM RAW_PROD.CES_JIRA.ISSUE
                    HAVING v_sim > 0.5 -- Only return highly relevant matches
                    ORDER BY v_sim DESC
                    LIMIT 100
                )
                SELECT ID, SUMMARY, DESCRIPTION FROM context_cte;


    SELECT * FROM RAW_PROD.CES_JIRA.ISSUE;
    SELECT * FROM RAW_PROD.CES_JIRA.ISSUE_PROPERTY;

    SELECT  * FROM RAW_PROD.CES_JIRA.PROJECT ;

    SELECT * FROM RAW_PROD.CES_JIRA.USER
    