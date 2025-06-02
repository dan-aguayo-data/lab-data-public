CREATE OR REPLACE PROCEDURE DATA_REF.INSERT_ACCESS_CONTROL(
    "PROJECT_NAME" VARCHAR(16777216),
    "ACCESS_TYPE" VARCHAR(16777216),
    "ENTITY_NAME" VARCHAR(16777216),
    "HAS_FILTER" VARCHAR(16777216),
    "SOURCE_DATABASE" VARCHAR(16777216),
    "SOURCE_SCHEMA" VARCHAR(16777216),
    "SOURCE_TABLE" VARCHAR(16777216),
    "ACCESS_ROLES" VARCHAR(16777216),
    "TARGET_DATABASE" VARCHAR(16777216),
    "TARGET_SCHEMA" VARCHAR(16777216),
    "TARGET_TABLE" VARCHAR(16777216),
    "CUSTOM_FILTERS" VARCHAR(16777216),
    "SELECTED_COLUMNS" VARCHAR(16777216)
)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    GROUP_ID STRING;
    DEFAULT_ROLE STRING;
    TARGET_DB STRING;
    TARGET_SCH STRING;
    TARGET_TBL STRING DEFAULT NULL;
    TARGET_TBL_DEFAULT STRING;
    r_table RESULTSET;
    existing_schema STRING;
    existing_access_type STRING;
    existing_table STRING;
    existing_db STRING;
    existing_entity_schema STRING;
    existing_entity_table STRING;
    existing_db_schema STRING;
    existing_db_table STRING;
    existing_table_schema STRING;
    valid_source STRING;

BEGIN
    -- Fetch group ID and default role
    SELECT GROUP_ID INTO GROUP_ID 
    FROM {{ ref('ENTITY_GROUP_REF') }} 
    WHERE ENTITY_NAME = :ENTITY_NAME;
    
    SELECT DEFAULT_ACCESS_ROLE INTO DEFAULT_ROLE 
    FROM {{ ref('ENTITY_GROUP_REF') }} 
    WHERE ENTITY_NAME = :ENTITY_NAME;

    -- Fetch existing access control metadata
    SELECT DISTINCT SOURCE_SCHEMA INTO existing_schema 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT TARGET_ENTITY_NAME INTO existing_entity_schema 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT TARGET_ENTITY_NAME INTO existing_entity_table 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND SOURCE_TABLE = :SOURCE_TABLE 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT SOURCE_DATABASE INTO existing_db_schema 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT SOURCE_DATABASE INTO existing_db_table 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND SOURCE_TABLE = :SOURCE_TABLE 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT SOURCE_SCHEMA INTO existing_table_schema 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND SOURCE_TABLE = :SOURCE_TABLE 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    SELECT DISTINCT ACCESS_TYPE INTO existing_access_type 
    FROM {{ ref('ACCESS_CONTROL') }} 
    WHERE SOURCE_DATABASE = :SOURCE_DATABASE 
        AND SOURCE_SCHEMA = :SOURCE_SCHEMA 
        AND TARGET_ENTITY_NAME = :ENTITY_NAME;

    -- Set default values for target database, schema, and roles
    TARGET_DB := IFF(TARGET_DATABASE IS NULL OR TARGET_DATABASE = '''', 
        ENTITY_NAME || ''_'' || SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(SOURCE_DATABASE), ''_'', 1)), ''_'', 1), 
        TARGET_DATABASE);
    TARGET_SCH := IFF(TARGET_SCHEMA IS NULL OR TARGET_SCHEMA = '''', :SOURCE_SCHEMA, :TARGET_SCHEMA);
    ACCESS_ROLES := IFF(ACCESS_ROLES IS NULL OR ACCESS_ROLES = '''', DEFAULT_ROLE, :ACCESS_ROLES);
    TARGET_TBL_DEFAULT := IFF(TARGET_TABLE IS NULL, :SOURCE_TABLE, :TARGET_TABLE);

    -- Validate source schema or table
    CALL DATA_REF.VALIDATE_SOURCE_SCHEMA(UPPER(:SOURCE_DATABASE), UPPER(:SOURCE_SCHEMA), UPPER(:ACCESS_TYPE), UPPER(:SOURCE_TABLE)) INTO :valid_source;

    IF (UPPER(valid_source) = ''VALID'') THEN
        -- Handle schema-level access
        IF (UPPER(ACCESS_TYPE) = ''SCHEMA'' 
            AND UPPER(SOURCE_SCHEMA) = UPPER(existing_schema) 
            AND UPPER(existing_access_type) = ''SCHEMA'' 
            AND UPPER(SOURCE_DATABASE) = UPPER(existing_db_schema) 
            AND UPPER(ENTITY_NAME) = UPPER(existing_entity_schema)) THEN 

            EXECUTE IMMEDIATE ''DELETE FROM {{ ref(''ACCESS_CONTROL'') }} 
                WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || :SOURCE_DATABASE || '''''', ''''_'''', 1) 
                AND SOURCE_SCHEMA = '''''' || :SOURCE_SCHEMA || '''''' 
                AND TARGET_ENTITY_NAME = '''''' || :ENTITY_NAME || '''''''';

            LET v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' 
                AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''''';
            r_table := (EXECUTE IMMEDIATE v_sql);
            LET c1 CURSOR FOR r_table;
            FOR row_variable IN c1 DO
                TARGET_TBL := row_variable.TABLE_NAME;
                CALL DATA_REF.GENERIC_INSERT_ACCESS_CONTROL(
                    UPPER(:PROJECT_NAME),
                    UPPER(:ACCESS_TYPE),
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                    UPPER(:SOURCE_DATABASE),
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:HAS_FILTER),
                    UPPER(:GROUP_ID),
                    UPPER(:ENTITY_NAME),
                    UPPER(:ACCESS_ROLES),
                    UPPER(:TARGET_DB),
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_TBL),
                    UPPER(:CUSTOM_FILTERS),
                    NULL
                );

                CALL DATA_REF.INSERT_ENV_CONTROL(
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                    UPPER(:SOURCE_DATABASE), 
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:ENTITY_NAME)
                );
            END FOR;

            CALL DATA_REF.CREATE_VIEWS_IF_NOT_EXIST(
                UPPER(:TARGET_SCH),
                UPPER(:ENTITY_NAME),
                UPPER(:PROJECT_NAME)
            );
            RETURN ''Schema access insert successful'';

        -- Handle table-level access
        ELSEIF (UPPER(ACCESS_TYPE) = ''TABLE'' 
            AND UPPER(SOURCE_SCHEMA) = UPPER(existing_table_schema) 
            AND UPPER(SOURCE_TABLE) = UPPER(existing_table) 
            AND UPPER(SOURCE_DATABASE) = UPPER(existing_db_table) 
            AND UPPER(ENTITY_NAME) = UPPER(existing_entity_table) 
            AND UPPER(existing_access_type) = ''TABLE'') THEN 

            EXECUTE IMMEDIATE ''DELETE FROM {{ ref(''ACCESS_CONTROL'') }} 
                WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || :SOURCE_DATABASE || '''''', ''''_'''', 1) 
                AND SOURCE_SCHEMA = '''''' || :SOURCE_SCHEMA || '''''' 
                AND SOURCE_TABLE = '''''' || :SOURCE_TABLE || '''''' 
                AND TARGET_ENTITY_NAME = '''''' || :ENTITY_NAME || '''''''';

            CALL DATA_REF.GENERIC_INSERT_ACCESS_CONTROL(
                UPPER(:PROJECT_NAME),
                UPPER(:ACCESS_TYPE),
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                UPPER(:SOURCE_DATABASE),
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:HAS_FILTER),
                UPPER(:GROUP_ID),
                UPPER(:ENTITY_NAME),
                UPPER(:ACCESS_ROLES),
                UPPER(:TARGET_DB),
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_TBL_DEFAULT),
                UPPER(:CUSTOM_FILTERS),
                UPPER(:SELECTED_COLUMNS)
            );

            CALL DATA_REF.INSERT_ENV_CONTROL(
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                UPPER(:SOURCE_DATABASE), 
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:ENTITY_NAME)
            );

            CALL DATA_REF.CREATE_VIEWS_IF_NOT_EXIST(
                UPPER(:TARGET_SCH),
                UPPER(:ENTITY_NAME),
                UPPER(:PROJECT_NAME)
            );
            RETURN ''Table access insert successful'';

        -- Handle conflicting access types
        ELSEIF (UPPER(ACCESS_TYPE) = ''SCHEMA'' AND UPPER(existing_access_type) = ''TABLE'') THEN 
            RETURN ''Table access exists on this schema; please remove it before proceeding'';

        ELSEIF (UPPER(ACCESS_TYPE) = ''TABLE'' AND UPPER(existing_access_type) = ''SCHEMA'') THEN 
            RETURN ''Schema access exists on this source; please remove it before proceeding'';

        -- Handle new access setup
        ELSE
            IF (UPPER(ACCESS_TYPE) = ''SCHEMA'') THEN
                LET v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' 
                    AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''''';
                r_table := (EXECUTE IMMEDIATE v_sql);
                LET c1 CURSOR FOR r_table;
                FOR row_variable IN c1 DO
                    TARGET_TBL := row_variable.TABLE_NAME;
                    CALL DATA_REF.GENERIC_INSERT_ACCESS_CONTROL(
                        UPPER(:PROJECT_NAME),
                        UPPER(:ACCESS_TYPE),
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                        UPPER(:SOURCE_DATABASE),
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:TARGET_TBL),
                        UPPER(:HAS_FILTER),
                        UPPER(:GROUP_ID),
                        UPPER(:ENTITY_NAME),
                        UPPER(:ACCESS_ROLES),
                        UPPER(:TARGET_DB),
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_TBL),
                        UPPER(:CUSTOM_FILTERS),
                        NULL
                    );

                    CALL DATA_REF.INSERT_ENV_CONTROL(
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                        UPPER(:SOURCE_DATABASE), 
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:TARGET_TBL),
                        UPPER(:ENTITY_NAME)
                    );
                END FOR;

                CALL DATA_REF.CREATE_VIEWS_IF_NOT_EXIST(
                    UPPER(:TARGET_SCH),
                    UPPER(:ENTITY_NAME),
                    UPPER(:PROJECT_NAME)
                );
                RETURN ''Schema access insert successful'';

            ELSEIF (UPPER(ACCESS_TYPE) = ''TABLE'') THEN
                CALL DATA_REF.GENERIC_INSERT_ACCESS_CONTROL(
                    UPPER(:PROJECT_NAME),
                    UPPER(:ACCESS_TYPE),
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                    UPPER(:SOURCE_DATABASE),
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:SOURCE_TABLE),
                    UPPER(:HAS_FILTER),
                    UPPER(:GROUP_ID),
                    UPPER(:ENTITY_NAME),
                    UPPER(:ACCESS_ROLES),
                    UPPER(:TARGET_DB),
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_TBL_DEFAULT),
                    UPPER(:CUSTOM_FILTERS),
                    UPPER(:SELECTED_COLUMNS)
                );

                CALL DATA_REF.INSERT_ENV_CONTROL(
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                    UPPER(:SOURCE_DATABASE), 
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:SOURCE_TABLE),
                    UPPER(:ENTITY_NAME)
                );

                CALL DATA_REF.CREATE_VIEWS_IF_NOT_EXIST(
                    UPPER(:TARGET_SCH),
                    UPPER(:ENTITY_NAME),
                    UPPER(:PROJECT_NAME)
                );
                RETURN ''Table access insert successful'';

            ELSE
                RETURN ''Invalid access type; only TABLE or SCHEMA allowed'';
            END IF;
        END IF;
    ELSE
        RETURN ''Invalid source; please review and correct it'';
    END IF;
END;
';