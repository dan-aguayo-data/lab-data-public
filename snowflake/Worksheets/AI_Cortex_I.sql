-- Query specific issue data
SELECT * 
FROM DATA_LAB.ANALYTICS.TASK_DATA_CONSOLIDATED 
WHERE TASK_CODE = 'PROJ-1001';

/** Create schema and set permissions for task data processing **/
CREATE SCHEMA DATA_LAB.ANALYTICS_TEST;

GRANT USAGE ON SCHEMA DATA_LAB.ANALYTICS TO ROLE DEV_ANALYTICS;
GRANT USAGE ON SCHEMA DATA_LAB.ANALYTICS TO ROLE DBT_ANALYTICS;
GRANT USAGE ON WAREHOUSE ANALYTICS_WAREHOUSE TO ROLE DBT_ANALYTICS;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DATA_LAB.ANALYTICS TO ROLE DEV_ANALYTICS;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DATA_LAB.ANALYTICS TO ROLE DBT_ANALYTICS;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DATA_LAB.ANALYTICS TO ROLE SYSADMIN;

/** Create consolidated task data table **/
CREATE OR REPLACE TABLE DATA_LAB.ANALYTICS.TASK_DATA_CONSOLIDATED AS
SELECT 
    A.ID AS TASK_ID,
    CONCAT(
        'Task Title: ', IFNULL(A.TITLE, 'Unknown Title'),
        ' Task Reference: ', IFNULL(A.CODE, 'Unknown'),
        ' Due Date: ', IFNULL(TO_CHAR(A.DEADLINE), 'Unknown'),
        ' Priority Level (5 highest, 0 lowest): ', IFNULL(TO_CHAR(A.PRIORITY_LEVEL), 'Unknown'),
        ' Time Logged: ', IFNULL(TO_CHAR(A.TIME_LOGGED), 'Unknown'),
        ' Completion Date: ', IFNULL(TO_CHAR(A.COMPLETED_DATE), 'Unknown'),
        ' Project: ', IFNULL(C.PROJECT_NAME, 'Unknown'),
        ' Assigned To: ', IFNULL(D.USER_NAME, 'Unknown'),
        ' Reported By: ', IFNULL(E.USER_NAME, 'Unknown')
    ) AS task_details,
    A.CODE AS TASK_CODE,
    A.TITLE AS TASK_TITLE,
    A.DEADLINE,
    A.PRIORITY_LEVEL,
    A.TIME_LOGGED,
    A.COMPLETED_DATE,
    C.PROJECT_NAME,
    IFNULL(D.USER_NAME, 'Unassigned') AS ASSIGNEE,
    IFNULL(E.USER_NAME, 'Unknown Reporter') AS REPORTER,
    A.TASK_STATUS
FROM {{ source('DB_OPERATIONS', 'TASKS') }} A
LEFT JOIN {{ source('DB_OPERATIONS', 'PROJECTS') }} C ON A.PROJECT_ID = C.ID
LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} D ON A.ASSIGNEE_ID = D.ID
LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} E ON A.REPORTER_ID = E.ID
WHERE A.TITLE IS NOT NULL;

-- Query specific consolidated task
SELECT * 
FROM DATA_LAB.ANALYTICS.TASK_DATA_CONSOLIDATED 
WHERE TASK_CODE = 'PROJ-1001';

/** Create vector table from consolidated task data **/
CREATE OR REPLACE TABLE DATA_LAB.ANALYTICS.TASK_DATA_VECTORS AS
SELECT 
    task_id,
    task_details,
    TASK_CODE,
    TASK_TITLE,
    DEADLINE,
    PRIORITY_LEVEL,
    TIME_LOGGED,
    COMPLETED_DATE,
    PROJECT_NAME,
    ASSIGNEE,
    REPORTER,
    snowflake.cortex.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', task_details) AS TASK_EMBEDDING
FROM DATA_LAB.ANALYTICS.TASK_DATA_CONSOLIDATED;

-- Query specific vectorized task
SELECT * 
FROM DATA_LAB.ANALYTICS.TASK_DATA_VECTORS 
WHERE TASK_CODE = 'PROJ-1001';

/** Query tasks with vector similarity search **/
WITH context_cte AS (
    SELECT 
        A.ID AS TASK_ID,
        A.CODE AS TASK_CODE,
        A.PARENT_TASK_ID,
        A.LAST_UPDATED,
        A.TITLE,
        A.DEADLINE,
        A.PRIORITY_LEVEL,
        A.TIME_LOGGED,
        A.COMPLETED_DATE,
        C.PROJECT_NAME,
        C.PROJECT_CATEGORY,
        D.USER_NAME AS ASSIGNEE,
        E.USER_NAME AS REPORTER,
        vector_cosine_similarity(
            snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', LEFT(A.TITLE, 500)),
            snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', 'example task description')
        ) AS similarity_score
    FROM {{ source('DB_OPERATIONS', 'TASKS') }} A
    LEFT JOIN {{ source('DB_OPERATIONS', 'PROJECTS') }} C ON A.PROJECT_ID = C.ID
    LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} D ON A.ASSIGNEE_ID = D.ID
    LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} E ON A.REPORTER_ID = E.ID
    WHERE A.TITLE IS NOT NULL
        AND A.LAST_UPDATED >= DATEADD(DAY, -90, CURRENT_DATE) -- Last 90 days
    HAVING similarity_score > 0.5
    ORDER BY similarity_score DESC
)
SELECT 
    similarity_score,
    TASK_ID,
    TASK_CODE,
    PARENT_TASK_ID,
    LAST_UPDATED,
    TITLE,
    DEADLINE,
    PRIORITY_LEVEL,
    TIME_LOGGED,
    COMPLETED_DATE,
    PROJECT_NAME,
    PROJECT_CATEGORY,
    ASSIGNEE,
    REPORTER
FROM context_cte;

/** Query all task details **/
SELECT 
    A.ID,
    A.CODE,
    A.PARENT_TASK_ID,
    A.LAST_UPDATED,
    A.NOTES,
    A.TITLE,
    A.DEADLINE,
    A.PRIORITY_LEVEL,
    A.TIME_LOGGED,
    A.COMPLETED_DATE,
    C.PROJECT_NAME,
    C.PROJECT_CATEGORY,
    D.USER_NAME AS ASSIGNEE,
    E.USER_NAME AS REPORTER
FROM {{ source('DB_OPERATIONS', 'TASKS') }} A
LEFT JOIN {{ source('DB_OPERATIONS', 'PROJECTS') }} C ON A.PROJECT_ID = C.ID
LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} D ON A.ASSIGNEE_ID = D.ID
LEFT JOIN {{ source('DB_OPERATIONS', 'USERS') }} E ON A.REPORTER_ID = E.ID
WHERE TITLE IS NOT NULL;

/** Query tasks with specific fields using CTE **/
WITH context_cte AS (
    SELECT 
        ID,
        TITLE,
        NOTES,
        TIME_LOGGED,
        CREATOR
    FROM {{ source('DB_OPERATIONS', 'TASKS') }}
    WHERE vector_cosine_similarity(
        snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', TITLE),
        snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', 'example task description')
    ) > 0.5
    ORDER BY vector_cosine_similarity(
        snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', TITLE),
        snowflake.cortex.embed_text_1024('snowflake-arctic-embed-l-v2.0', 'example task description