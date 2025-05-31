CREATE OR REPLACE PROCEDURE CES_REF.CES_DBT.INSERT_SHARE_CONTROL_MSI("DBT_PROJECT" VARCHAR(16777216), "SHARE_TYPE" VARCHAR(16777216), "DATA_SHARE" NUMBER(1,0), "TARGET_ENTITY" VARCHAR(16777216), "SOURCE_DATABASE" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216), "TARGET_ROLES" VARCHAR(16777216), "TARGET_DATABASE" VARCHAR(16777216), "TARGET_SCHEMA" VARCHAR(16777216), "TARGET_TABLE" VARCHAR(16777216), "TARGET_SHARE" VARCHAR(16777216), "TARGET_DB_ROLES" VARCHAR(16777216), "OTHER_FILTERS" VARCHAR(16777216), "COLUMNS_TO_SHARE" VARCHAR(16777216), "PROC_SCHEMA_EXCEPTIONS" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    MULTISCHEME_ID STRING; 
    DEFAULT_ROLE STRING;
    TARGET_DB STRING;
    TARGET_SCH STRING;
    TARGET_TBL STRING DEFAULT NULL;
    TARGET_TBL_ELSE STRING;
    r_table RESULTSET; 
    deletion_schema STRING ;
    control_share_type STRING;
    deletion_table STRING ;
    control_db STRING;
    deletion_target_entity_schema STRING ;
    deletion_target_entity_table STRING ;
    deletion_database_schema STRING ;
    deletion_database_table STRING;
    deletion_table_schema STRING;
    valid_source STRING;
    HAS_MSI_FILTER STRING;
    SCHEMA_EXCEPTION STRING;
    DEFAULT_DB_ROLES_REF STRING;
    DEFAULT_SHARE_REF STRING;
    TARGET_DB_ROLES_REF STRING;
    TARGET_SHARE_REF STRING;
       
BEGIN
    
    SELECT DEFAULT_SCHEME_ID INTO MULTISCHEME_ID FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DEFAULT_SHARE_ROLE INTO DEFAULT_ROLE FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DEFAULT_DB_ROLES INTO DEFAULT_DB_ROLES_REF FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DEFAULT_SHARES INTO DEFAULT_SHARE_REF FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DISTINCT SOURCE_SCHEMA INTO deletion_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
        SELECT DISTINCT SOURCE_SCHEMA INTO deletion_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    SELECT DISTINCT TARGET_ENTITY_NAME INTO deletion_target_entity_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT ;
    SELECT DISTINCT TARGET_ENTITY_NAME INTO deletion_target_entity_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE    AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
        SELECT DISTINCT SOURCE_DATABASE INTO deletion_database_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE    AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT ;
            SELECT DISTINCT SOURCE_DATABASE INTO deletion_database_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE    AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DISTINCT SOURCE_SCHEMA INTO deletion_table_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT;
    SELECT DISTINCT SHARE_TYPE INTO control_share_type FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND DBT_PROJECT = :DBT_PROJECT AND DATA_SHARE = :DATA_SHARE;
    TARGET_DB := IFF(TARGET_DATABASE IS NULL OR TARGET_DATABASE = '''', TARGET_ENTITY || ''_'' || SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(SOURCE_DATABASE), ''_'', 1)), ''_'', 1), TARGET_DATABASE);  
    TARGET_SCH := IFF(TARGET_SCHEMA IS NULL OR TARGET_SCHEMA = '''', :SOURCE_SCHEMA, :TARGET_SCHEMA);
    TARGET_ROLES := IFF(TARGET_ROLES IS NULL OR TARGET_ROLES = '''', DEFAULT_ROLE, :TARGET_ROLES);
    TARGET_DB_ROLES_REF := IFF(TARGET_DB_ROLES IS NULL OR TARGET_DB_ROLES = '''', DEFAULT_DB_ROLES_REF, :TARGET_DB_ROLES);
    TARGET_SHARE_REF := IFF(TARGET_SHARE IS NULL OR TARGET_SHARE = '''', DEFAULT_SHARE_REF, :TARGET_SHARE); 
    TARGET_TBL_ELSE := IFF(TARGET_TABLE IS NULL, :SOURCE_TABLE, :TARGET_TABLE);
    SELECT DISTINCT SOURCE_TABLE INTO deletion_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND  SOURCE_TABLE = :SOURCE_TABLE AND DBT_PROJECT = :DBT_PROJECT AND DATA_SHARE = :DATA_SHARE;
    HAS_MSI_FILTER := ''''''NO'''''';
    SCHEMA_EXCEPTION := IFF(PROC_SCHEMA_EXCEPTIONS IS NULL OR PROC_SCHEMA_EXCEPTIONS = '''''''',''''''DUMMY_VALUE_XYZ'''''',PROC_SCHEMA_EXCEPTIONS);

    
CALL CES_REF.CES_DBT.VALID_SOURCE_SCHEMA(UPPER(:SOURCE_DATABASE),UPPER(:SOURCE_SCHEMA),UPPER(:SHARE_TYPE),UPPER(:SOURCE_TABLE))  into :valid_source;

--FIRST CONDITIONAL CHECKS IF SOURCE IS VALID AND EXISTS. 
                                                        ---CHECKS THAT SHARE TYPE AND SOURCE ARE RIGHT
IF (UPPER(valid_source) = ''VALID'') THEN

    
    --DATA SHARE CONDITIONAL STARTS ******************************************
    IF (DATA_SHARE  = ''1'') THEN 

            --CONDITION OPTION 1. IF SHARE TYPE IS SCHEMA AND IT EXISTS. 
            --EXISTS WITHIN THE PRIMARY KEY SHARE, SOURCE, ENTITY 
            IF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_schema) AND UPPER(control_share_type) = ''SCHEMA'' AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_schema) AND UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_schema) AND DATA_SHARE = 1)  THEN 
        
        EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''' AND DATA_SHARE = ''''0'''';'';
            
                     let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND NOT(TABLE_NAME LIKE ANY(''|| SCHEMA_EXCEPTION ||''))'';
                    r_table := (EXECUTE IMMEDIATE v_sql);
                    let c1 CURSOR FOR r_table;
                    FOR row_variable IN c1 DO
                        TARGET_TBL := row_variable.TABLE_NAME;
                        
                        --DEFINES IF TABLE/VIEW CONTAINS MSI COLUMN TO FILTER ON
                        CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:TARGET_TBL) into :HAS_MSI_FILTER  ;
                        
                        --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                        CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                            UPPER(:DBT_PROJECT),
                            UPPER(:SHARE_TYPE),
                            1,
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                            UPPER(:SOURCE_DATABASE),
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:HAS_MSI_FILTER),
                            UPPER(:MULTISCHEME_ID),
                            UPPER(:TARGET_ENTITY),
                            UPPER(:TARGET_SHARE_REF),
                            UPPER(:TARGET_DB_ROLES_REF),
                            UPPER(:TARGET_ROLES),
                            UPPER(:TARGET_DB),
                            UPPER(:TARGET_SCH),
                            UPPER(:TARGET_TBL),
                            :OTHER_FILTERS,
                            NULL,
                            :PROC_SCHEMA_EXCEPTIONS
                        );
                                 
                    END FOR;
                       
                     RETURN ''Update on Schema successful'';        
             
            --CONDITION OPTION 2. IF SHARE TYPE IS SCHEMA AND IT EXISTS AS TABLE. 
                                                            
            ELSEIF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(control_share_type) = ''TABLE'' ) THEN 
        
                RETURN ''You have already created a table share on this schema, please truncate it before you can continue'';
        
            --CONDITION OPTION 3. IF SHARE TYPE IS TABLE AND IT EXISTS AS SCHEMA.
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(control_share_type) = ''SCHEMA'' ) THEN 
        
                RETURN ''You have already created a schema share on this schema_source, please truncate it before you can continue'';
        
            --CONDITION OPTION 4. IF SHARE TYPE IS TABLE AND IT EXISTS. 
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_table_schema) AND UPPER(SOURCE_TABLE) = UPPER(deletion_table) AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_table) AND  UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_table) AND UPPER(control_share_type) = ''TABLE'' AND DATA_SHARE = 1)  THEN 
                
        EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''' AND DATA_SHARE = ''''1'''';'';        
            
                    --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                    CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:SOURCE_TABLE) into :HAS_MSI_FILTER  ;
                    
                    --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                    CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                        UPPER(:DBT_PROJECT),
                        UPPER(:SHARE_TYPE),
                        1,
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                        UPPER(:SOURCE_DATABASE),
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:HAS_MSI_FILTER),
                        UPPER(:MULTISCHEME_ID),
                        UPPER(:TARGET_ENTITY),
                        UPPER(:TARGET_SHARE_REF),
                        UPPER(:TARGET_DB_ROLES_REF),
                        UPPER(:TARGET_ROLES),
                        UPPER(:TARGET_DB),
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_TBL_ELSE),
                        :OTHER_FILTERS,
                        UPPER(:COLUMNS_TO_SHARE),
                        :PROC_SCHEMA_EXCEPTIONS
                    ); 
                    
                    RETURN ''Update on TABLE successful'';
                    
                
            --CONDITION OPTION 5. IF SHARE TYPE IS TABLE OR SCHEMA AND THIS SOURCE DOESNT ALREADY EXIST. 
                                                                                     
             ELSE
             
                            IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
                     let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND NOT(TABLE_NAME LIKE ANY(''|| SCHEMA_EXCEPTION ||''))'';
                    r_table := (EXECUTE IMMEDIATE v_sql);
                    let c1 CURSOR FOR r_table;
                    FOR row_variable IN c1 DO
                        TARGET_TBL := row_variable.TABLE_NAME;
                        --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                        CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:TARGET_TBL) into :HAS_MSI_FILTER  ;
                        --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                        CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                            UPPER(:DBT_PROJECT),
                            UPPER(:SHARE_TYPE),
                            1,
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                            UPPER(:SOURCE_DATABASE),
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:HAS_MSI_FILTER),
                            UPPER(:MULTISCHEME_ID),
                            UPPER(:TARGET_ENTITY),
                            UPPER(:TARGET_SHARE_REF),
                            UPPER(:TARGET_DB_ROLES_REF),
                            UPPER(:TARGET_ROLES),
                            UPPER(:TARGET_DB),
                            UPPER(:TARGET_SCH),
                            UPPER(:TARGET_TBL),
                            :OTHER_FILTERS,
                            NULL,
                            :PROC_SCHEMA_EXCEPTIONS 
                        );
                    END FOR;
                    
                    RETURN ''Insert on Schema successful''; 
                    
                    ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'') THEN
                    --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                    
                    CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:SOURCE_TABLE) into :HAS_MSI_FILTER  ;
                    
                    --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                    
                    CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                        UPPER(:DBT_PROJECT),
                        UPPER(:SHARE_TYPE),
                        1,
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                        UPPER(:SOURCE_DATABASE),
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:HAS_MSI_FILTER),
                        UPPER(:MULTISCHEME_ID),
                        UPPER(:TARGET_ENTITY),
                        UPPER(:TARGET_SHARE_REF),
                        UPPER(:TARGET_DB_ROLES_REF),
                        UPPER(:TARGET_ROLES),
                        UPPER(:TARGET_DB),
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_TBL_ELSE),
                        :OTHER_FILTERS,
                        UPPER(:COLUMNS_TO_SHARE),
                        :PROC_SCHEMA_EXCEPTIONS
                    ); 

                    
                   RETURN ''Insert on TABLE successful'';
                    -- RETURN ''Insert on TABLE successful: '' || :SOURCE_DATABASE || '', '' || :SOURCE_SCHEMA || '', '' || :TARGET_TBL_ELSE;
                    
                ELSE
                    RETURN ''Unexpected Share Type, only TABLE or SCHEMA allowed, please retype parameter SHARE_TYPE '';
                END IF;
        
        END IF; --SECOND CONDITIONAL TO LOOP OPTIONS ENDS


    
    ELSE   -- DATA SHARE ELSE STATEMENT ********************************************

    
            
            --CONDITION OPTION 1. IF SHARE TYPE IS SCHEMA AND IT EXISTS. 
            --EXISTS WITHIN THE PRIMARY KEY SHARE, SOURE, ENTITY 
            IF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_schema) AND UPPER(control_share_type) = ''SCHEMA'' AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_schema) AND UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_schema) AND DATA_SHARE = 0 ) THEN 
        
        EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''' AND DATA_SHARE = ''''0'''';'';
            
                    --IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
                     let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND NOT(TABLE_NAME LIKE ANY(''|| SCHEMA_EXCEPTION ||''))'';
                    r_table := (EXECUTE IMMEDIATE v_sql);
                    let c1 CURSOR FOR r_table;
                    FOR row_variable IN c1 DO
                        TARGET_TBL := row_variable.TABLE_NAME;
                        --DEFINES IF TABLE/VIEW CONTAINS MSI COLUMN TO FILTER ON
                        CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:TARGET_TBL) into :HAS_MSI_FILTER  ;
                        
                        --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                        CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                            UPPER(:DBT_PROJECT),
                            UPPER(:SHARE_TYPE),
                            0,
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                            UPPER(:SOURCE_DATABASE),
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:HAS_MSI_FILTER),
                            UPPER(:MULTISCHEME_ID),
                            UPPER(:TARGET_ENTITY),
                            NULL,
                            NULL,
                            UPPER(:TARGET_ROLES),
                            UPPER(:TARGET_DB),
                            UPPER(:TARGET_SCH),
                            UPPER(:TARGET_TBL),
                            :OTHER_FILTERS,
                            NULL,
                            :PROC_SCHEMA_EXCEPTIONS
                        );
                        --MAKE SURE TO CREATES A RECORD PER ENVIRONMNET UAT,PROD,DEV
                        CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                            UPPER(:SOURCE_DATABASE), 
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:TARGET_ENTITY)
                        );
                      
                        
                    END FOR;
                        --CREATES SCHEMAS IN TARGET IF THEY DONT EXIST
                      CALL CES_REF.CES_DBT.CREATE_SCHEMAS_IF_NOT_EXIST(
                             UPPER(:TARGET_SCH),
                            UPPER(:TARGET_ENTITY),
                            UPPER(:DBT_PROJECT))  ;
                    
                    RETURN ''Update on Schema successful''; 
                    
             
               
            --CONDITION OPTION 2. IF SHARE TYPE IS SCHEMA AND IT EXISTS AS TABLE. 
                                                            
            ELSEIF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(control_share_type) = ''TABLE'' ) THEN 
        
                RETURN ''You have already created a table share on this schema, please truncate it before you can continue'';
        
            --CONDITION OPTION 3. IF SHARE TYPE IS TABLE AND IT EXISTS AS SCHEMA.
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(control_share_type) = ''SCHEMA'' ) THEN 
        
                RETURN ''You have already created a schema share on this schema_source, please truncate it before you can continue'';
        
            --CONDITION OPTION 4. IF SHARE TYPE IS TABLE AND IT EXISTS. 
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_table_schema) AND UPPER(SOURCE_TABLE) = UPPER(deletion_table) AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_table) AND  UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_table) AND UPPER(control_share_type) = ''TABLE'' AND DATA_SHARE = 0)  THEN 
                
        
        EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''' AND DATA_SHARE = ''''0'''';'';        
            
                    --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                    CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:SOURCE_TABLE) into :HAS_MSI_FILTER  ;
                    
                    --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                    CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                        UPPER(:DBT_PROJECT),
                        UPPER(:SHARE_TYPE),
                        0,
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                        UPPER(:SOURCE_DATABASE),
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:HAS_MSI_FILTER),
                        UPPER(:MULTISCHEME_ID),
                        UPPER(:TARGET_ENTITY),
                        NULL,
                        NULL,
                        UPPER(:TARGET_ROLES),
                        UPPER(:TARGET_DB),
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_TBL_ELSE),
                        :OTHER_FILTERS,
                        UPPER(:COLUMNS_TO_SHARE),
                        :PROC_SCHEMA_EXCEPTIONS
                    ); 
                    --MAKE SURE TO CREATES A RECORD PER ENVIRONMNET UAT,PROD,DEV
                    CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                        UPPER(:SOURCE_DATABASE), 
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:TARGET_ENTITY)
                    );
                        --CREATES SCHEMAS IN TARGET IF THEY DONT EXIST
                    CALL CES_REF.CES_DBT.CREATE_SCHEMAS_IF_NOT_EXIST(
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_ENTITY),
                        UPPER(:DBT_PROJECT))  ;
                    
                    RETURN ''Update on TABLE successful'';
                    
        
                
            --CONDITION OPTION 5. IF SHARE TYPE IS TABLE OR SCHEMA AND THIS SOURCE DOESNT ALREADY EXIST. 
                                                                                     
             ELSE
             
                            IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
                     let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND NOT(TABLE_NAME LIKE ANY(''|| SCHEMA_EXCEPTION ||''))'';
                    r_table := (EXECUTE IMMEDIATE v_sql);
                    let c1 CURSOR FOR r_table;
                    FOR row_variable IN c1 DO
                        TARGET_TBL := row_variable.TABLE_NAME;
                        --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                        CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:TARGET_TBL) into :HAS_MSI_FILTER  ;
                        --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                        CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                            UPPER(:DBT_PROJECT),
                            UPPER(:SHARE_TYPE),
                            0,
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                            UPPER(:SOURCE_DATABASE),
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:HAS_MSI_FILTER),
                            UPPER(:MULTISCHEME_ID),
                            UPPER(:TARGET_ENTITY),
                            NULL,
                            NULL,
                            UPPER(:TARGET_ROLES),
                            UPPER(:TARGET_DB),
                            UPPER(:TARGET_SCH),
                            UPPER(:TARGET_TBL),
                            :OTHER_FILTERS,
                            NULL,
                            :PROC_SCHEMA_EXCEPTIONS 
                        );
                        --MAKE SURE TO CREATES A RECORD PER ENVIRONMNET UAT,PROD,DEV
                        CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                            UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                            UPPER(:SOURCE_DATABASE), 
                            UPPER(:SOURCE_SCHEMA),
                            UPPER(:TARGET_TBL),
                            UPPER(:TARGET_ENTITY)
                        );
                      
                    END FOR;
                    
                    ----CREATES SCHEMAS IN TARGET IF THEY DONT EXIST
                          CALL CES_REF.CES_DBT.CREATE_SCHEMAS_IF_NOT_EXIST(
                            UPPER(:TARGET_SCH),
                            UPPER(:TARGET_ENTITY),
                            UPPER(:DBT_PROJECT))  ;
                    
                    RETURN ''Insert on Schema successful''; 
                    
                    ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'') THEN
                    --DEFINES IF SOURCE TABLE/VIEW HAS THE MSI COLUMN
                    
                    CALL CES_REF.CES_DBT.DEFINE_MSI_VALUE(:SOURCE_DATABASE,:SOURCE_SCHEMA,:SOURCE_TABLE) into :HAS_MSI_FILTER  ;
                    
                    --INSERTS THE SINGLE ENVIRONMENT DATABASE INPUT INTO THE CONTROL TABLE
                    
                    CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                        UPPER(:DBT_PROJECT),
                        UPPER(:SHARE_TYPE),
                        0,
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                        UPPER(:SOURCE_DATABASE),
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:HAS_MSI_FILTER),
                        UPPER(:MULTISCHEME_ID),
                        UPPER(:TARGET_ENTITY),
                        NULL,
                        NULL,
                        UPPER(:TARGET_ROLES),
                        UPPER(:TARGET_DB),
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_TBL_ELSE),
                        :OTHER_FILTERS,
                        UPPER(:COLUMNS_TO_SHARE),
                        :PROC_SCHEMA_EXCEPTIONS
                    ); 
                    --MAKE SURE TO CREATES A RECORD PER ENVIRONMNET UAT,PROD,DEV
                    CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                        UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                        UPPER(:SOURCE_DATABASE), 
                        UPPER(:SOURCE_SCHEMA),
                        UPPER(:SOURCE_TABLE),
                        UPPER(:TARGET_ENTITY)
                    );
                    ----CREATES SCHEMAS IN TARGET IF THEY DONT EXIST
                    CALL CES_REF.CES_DBT.CREATE_SCHEMAS_IF_NOT_EXIST(
                        UPPER(:TARGET_SCH),
                        UPPER(:TARGET_ENTITY),
                        UPPER(:DBT_PROJECT))  ;
                    
                   RETURN ''Insert on TABLE successful'';
                    -- RETURN ''Insert on TABLE successful: '' || :SOURCE_DATABASE || '', '' || :SOURCE_SCHEMA || '', '' || :TARGET_TBL_ELSE;
                    
                ELSE
                    RETURN ''Unexpected Share Type, only TABLE or SCHEMA allowed, please retype parameter SHARE_TYPE '';
                END IF;
        
        
        

        END IF; --SECOND CONDITIONAL TO LOOP OPTIONS ENDS
    
    END IF; --END DATA SHARE CONDITIONAL *****************************************

ELSE

RETURN ''Your source or share type is not valid, please review it and ammend it'';


END IF; --FIRST CONDITIONAL FOR VALID SOURCE ENDS 


END;
';