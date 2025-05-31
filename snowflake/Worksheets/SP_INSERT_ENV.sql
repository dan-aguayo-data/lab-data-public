CREATE OR REPLACE PROCEDURE CES_REF.CES_DBT.INSERT_ENVIRONMENTS_CONTROL("ENVIRONMENT_START" VARCHAR(16777216), "SOURCE_DATABASE" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216), "TARGET_ENTITY" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
    IF (UPPER(ENVIRONMENT_START) = ''DEV'') THEN  
        let dev_uat_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''DEV'''', ''''UAT'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''DEV'''', ''''UAT'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''DEV'''', ''''UAT'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';
        
        let dev_prod_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''DEV'''', ''''PROD'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''DEV'''', ''''PROD'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''DEV'''', ''''PROD'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';
        
        EXECUTE IMMEDIATE dev_uat_sql;
        EXECUTE IMMEDIATE dev_prod_sql;
        RETURN ''Insert on TABLE successful'';
        
    ELSEIF (UPPER(ENVIRONMENT_START) = ''UAT'') THEN  
        let uat_prod_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''UAT'''', ''''PROD'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''UAT'''', ''''PROD'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''UAT'''', ''''PROD'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
       FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';

         let uat_dev_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''UAT'''', ''''DEV'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''UAT'''', ''''DEV'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''UAT'''', ''''DEV'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';
        
        EXECUTE IMMEDIATE uat_dev_sql;
        EXECUTE IMMEDIATE uat_prod_sql;
        RETURN ''Insert on TABLE successful'';

     ELSEIF (UPPER(ENVIRONMENT_START) = ''PROD'') THEN  
        let prod_uat_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''PROD'''', ''''UAT'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''PROD'''', ''''UAT'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''PROD'''', ''''UAT'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';

         let prod_dev_sql := 
        ''INSERT INTO CES_DBT.DBT_SHARE_CONTROL
        (
            DBT_PROJECT,
            SHARE_TYPE,
            DATA_SHARE,
            SOURCE_ENVIRONMENT,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            TARGET_SHARE,
            TARGET_DB_ROLES,
            TARGET_ROLES,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        )
        SELECT
            DBT_PROJECT,
            SHARE_TYPE,
            0,
            REPLACE(SOURCE_ENVIRONMENT, ''''PROD'''', ''''DEV'''') AS SOURCE_ENVIRONMENT,
            REPLACE(SOURCE_DATABASE, ''''PROD'''', ''''DEV'''') AS SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            HAS_MSI_FILTER,
            MULTISCHEME_ID,
            TARGET_ENTITY_NAME,
            NULL,
            NULL,
            TARGET_ROLES,
            REPLACE(TARGET_DATABASE, ''''PROD'''', ''''DEV'''') AS TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            OTHER_FILTERS,
            COLUMNS_TO_SHARE,
            PROC_SCHEMA_EXCEPTIONS
        FROM CES_DBT.DBT_SHARE_CONTROL
        WHERE SOURCE_DATABASE = '''''' || SOURCE_DATABASE || '''''' 
        AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' 
        AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' 
        AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''';'';
        
        EXECUTE IMMEDIATE prod_dev_sql;
        EXECUTE IMMEDIATE prod_uat_sql;
        RETURN ''Insert on TABLE successful'';
        
    ELSE
        RETURN ''Error when replicating share to other environments'';
    END IF;
END;
';