CREATE OR REPLACE PROCEDURE CES_REF.CES_DBT.INSERT_SHARE_CONTROL("DBT_PROJECT" VARCHAR(16777216), "SHARE_TYPE" VARCHAR(16777216), "TARGET_ENTITY" VARCHAR(16777216), "HAS_MSI_FILTER" VARCHAR(16777216), "SOURCE_DATABASE" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216), "TARGET_ROLES" VARCHAR(16777216), "TARGET_DATABASE" VARCHAR(16777216), "TARGET_SCHEMA" VARCHAR(16777216), "TARGET_TABLE" VARCHAR(16777216), "OTHER_FILTERS" VARCHAR(16777216), "COLUMNS_TO_SHARE" VARCHAR(16777216))
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
      
BEGIN
    
    SELECT DEFAULT_SCHEME_ID INTO MULTISCHEME_ID FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY;
    SELECT DEFAULT_SHARE_ROLE INTO DEFAULT_ROLE FROM CES_DBT.ENTITY_MULTI_SCHEME_ID_REF WHERE ENTITY_NAME = :TARGET_ENTITY;
    SELECT DISTINCT SOURCE_SCHEMA INTO deletion_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    SELECT DISTINCT TARGET_ENTITY_NAME INTO deletion_target_entity_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    SELECT DISTINCT TARGET_ENTITY_NAME INTO deletion_target_entity_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE    AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
        SELECT DISTINCT SOURCE_DATABASE INTO deletion_database_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE    AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
            SELECT DISTINCT SOURCE_DATABASE INTO deletion_database_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE    AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    SELECT DISTINCT SOURCE_SCHEMA INTO deletion_table_schema FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA AND SOURCE_TABLE = :SOURCE_TABLE AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    SELECT DISTINCT SHARE_TYPE INTO control_share_type FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY ;
    TARGET_DB := IFF(TARGET_DATABASE IS NULL OR TARGET_DATABASE = '''', TARGET_ENTITY || ''_'' || SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(SOURCE_DATABASE), ''_'', 1)), ''_'', 1), TARGET_DATABASE);  
    TARGET_SCH := IFF(TARGET_SCHEMA IS NULL OR TARGET_SCHEMA = '''', :SOURCE_SCHEMA, :TARGET_SCHEMA);
    TARGET_ROLES := IFF(TARGET_ROLES IS NULL OR TARGET_ROLES = '''', DEFAULT_ROLE, :TARGET_ROLES);
    TARGET_TBL_ELSE := IFF(TARGET_TABLE IS NULL, :SOURCE_TABLE, :TARGET_TABLE);
    SELECT DISTINCT SOURCE_TABLE INTO deletion_table FROM CES_DBT.DBT_SHARE_CONTROL WHERE SOURCE_DATABASE = :SOURCE_DATABASE  AND SOURCE_SCHEMA = :SOURCE_SCHEMA  AND  TARGET_ENTITY_NAME = :TARGET_ENTITY AND  SOURCE_TABLE = :SOURCE_TABLE ;

CALL CES_REF.CES_DBT.VALID_SOURCE_SCHEMA(UPPER(:SOURCE_DATABASE),UPPER(:SOURCE_SCHEMA),UPPER(:SHARE_TYPE),UPPER(:SOURCE_TABLE))  into :valid_source;
IF (UPPER(valid_source) = ''VALID'') THEN
    
IF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_schema) AND UPPER(control_share_type) = ''SCHEMA'' AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_schema) AND UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_schema) ) THEN 

EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''''';
    
            IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
            let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''''';
            r_table := (EXECUTE IMMEDIATE v_sql);
            let c1 CURSOR FOR r_table;
            FOR row_variable IN c1 DO
                TARGET_TBL := row_variable.TABLE_NAME;
                CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                    UPPER(:DBT_PROJECT),
                    UPPER(:SHARE_TYPE),
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                    UPPER(:SOURCE_DATABASE),
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:HAS_MSI_FILTER),
                    UPPER(:MULTISCHEME_ID),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:TARGET_ROLES),
                    UPPER(:TARGET_DB),
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_TBL),
                    UPPER(:OTHER_FILTERS),
                    NULL
                );
                
                CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                    UPPER(:SOURCE_DATABASE), 
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:TARGET_ENTITY)
                );
              
                
            END FOR;

              CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                     UPPER(:TARGET_SCH),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:DBT_PROJECT))  ;
            
            RETURN ''Insert on Schema successful''; 
            
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'') THEN
            CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                UPPER(:DBT_PROJECT),
                UPPER(:SHARE_TYPE),
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                UPPER(:SOURCE_DATABASE),
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:HAS_MSI_FILTER),
                UPPER(:MULTISCHEME_ID),
                UPPER(:TARGET_ENTITY),
                UPPER(:TARGET_ROLES),
                UPPER(:TARGET_DB),
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_TBL_ELSE),
                UPPER(:OTHER_FILTERS),
                UPPER(:COLUMNS_TO_SHARE)
            ); 
            
            CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                UPPER(:SOURCE_DATABASE), 
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:TARGET_ENTITY)
            );
            CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_ENTITY),
                UPPER(:DBT_PROJECT))  ;
            
            RETURN ''Insert on TABLE successful'';
            
        ELSE
            RETURN ''Unexpected Share Type, only TABLE or SCHEMA allowed, please retype parameter SHARE_TYPE'';
        END IF;
       

ELSEIF (UPPER(SHARE_TYPE) = ''SCHEMA'' AND UPPER(control_share_type) = ''TABLE'' ) THEN 

        RETURN ''You have already created a table share on this schema, please truncate it before you can continue'';

ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(control_share_type) = ''SCHEMA'' ) THEN 

        RETURN ''You have already created a schema share on this schema_source, please truncate it before you can continue'';

ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'' AND UPPER(SOURCE_SCHEMA) = UPPER(deletion_table_schema) AND UPPER(SOURCE_TABLE) = UPPER(deletion_table) AND UPPER(SOURCE_DATABASE) = UPPER(deletion_database_table) AND  UPPER(TARGET_ENTITY) = UPPER(deletion_target_entity_table) AND UPPER(control_share_type) = ''TABLE'')  THEN 
        

EXECUTE IMMEDIATE ''DELETE FROM CES_DBT.DBT_SHARE_CONTROL WHERE SPLIT_PART(SOURCE_DATABASE, ''''_'''', 1) = SPLIT_PART('''''' || SOURCE_DATABASE || '''''', ''''_'''', 1) AND SOURCE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''' AND SOURCE_TABLE = '''''' || SOURCE_TABLE || '''''' AND TARGET_ENTITY_NAME = '''''' || TARGET_ENTITY || '''''''';        
     IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
            let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''''';
            r_table := (EXECUTE IMMEDIATE v_sql);
            let c1 CURSOR FOR r_table;
            FOR row_variable IN c1 DO
                TARGET_TBL := row_variable.TABLE_NAME;
                CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                    UPPER(:DBT_PROJECT),
                    UPPER(:SHARE_TYPE),
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                    UPPER(:SOURCE_DATABASE),
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:HAS_MSI_FILTER),
                    UPPER(:MULTISCHEME_ID),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:TARGET_ROLES),
                    UPPER(:TARGET_DB),
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_TBL),
                    UPPER(:OTHER_FILTERS),
                    NULL
                );
                
                CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                    UPPER(:SOURCE_DATABASE), 
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:TARGET_ENTITY)
                );
                
        
            END FOR;
            
             CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:DBT_PROJECT))  ;
            RETURN ''Insert on Schema successful''; 
            
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'') THEN
            CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                UPPER(:DBT_PROJECT),
                UPPER(:SHARE_TYPE),
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                UPPER(:SOURCE_DATABASE),
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:HAS_MSI_FILTER),
                UPPER(:MULTISCHEME_ID),
                UPPER(:TARGET_ENTITY),
                UPPER(:TARGET_ROLES),
                UPPER(:TARGET_DB),
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_TBL_ELSE),
                UPPER(:OTHER_FILTERS),
                UPPER(:COLUMNS_TO_SHARE)
            ); 
            
            CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                UPPER(:SOURCE_DATABASE), 
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:TARGET_ENTITY)
            );

            CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_ENTITY),
                UPPER(:DBT_PROJECT))  ;
            
            RETURN ''Insert on TABLE successful'';
            
        ELSE
            RETURN ''Unexpected Share Type, only TABLE or SCHEMA allowed, please retype parameter SHARE_TYPE'';
        END IF;    
        
ELSE
                    IF (UPPER(SHARE_TYPE) = ''SCHEMA'') THEN  
            let v_sql := ''SELECT TABLE_NAME FROM "'' || SOURCE_DATABASE || ''".INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = '''''' || SOURCE_DATABASE || '''''' AND TABLE_SCHEMA = '''''' || SOURCE_SCHEMA || '''''''';
            r_table := (EXECUTE IMMEDIATE v_sql);
            let c1 CURSOR FOR r_table;
            FOR row_variable IN c1 DO
                TARGET_TBL := row_variable.TABLE_NAME;
                CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                    UPPER(:DBT_PROJECT),
                    UPPER(:SHARE_TYPE),
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                    UPPER(:SOURCE_DATABASE),
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:HAS_MSI_FILTER),
                    UPPER(:MULTISCHEME_ID),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:TARGET_ROLES),
                    UPPER(:TARGET_DB),
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_TBL),
                    UPPER(:OTHER_FILTERS),
                    NULL 
                );
                
                CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                    UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                    UPPER(:SOURCE_DATABASE), 
                    UPPER(:SOURCE_SCHEMA),
                    UPPER(:TARGET_TBL),
                    UPPER(:TARGET_ENTITY)
                );
              
    
            END FOR;
                  CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                    UPPER(:TARGET_SCH),
                    UPPER(:TARGET_ENTITY),
                    UPPER(:DBT_PROJECT))  ;
            
            RETURN ''Insert on Schema successful''; 
            
            ELSEIF (UPPER(SHARE_TYPE) = ''TABLE'') THEN
            CALL CES_REF.CES_DBT.GENERIC_INSERT_TO_SHARE_CONTROL(
                UPPER(:DBT_PROJECT),
                UPPER(:SHARE_TYPE),
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)),
                UPPER(:SOURCE_DATABASE),
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:HAS_MSI_FILTER),
                UPPER(:MULTISCHEME_ID),
                UPPER(:TARGET_ENTITY),
                UPPER(:TARGET_ROLES),
                UPPER(:TARGET_DB),
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_TBL_ELSE),
                UPPER(:OTHER_FILTERS),
                UPPER(:COLUMNS_TO_SHARE)
            ); 
            
            CALL CES_DBT.INSERT_ENVIRONMENTS_CONTROL(
                UPPER(SPLIT_PART(REVERSE(SPLIT_PART(REVERSE(:SOURCE_DATABASE), ''_'', 1)), ''_'', 1)), 
                UPPER(:SOURCE_DATABASE), 
                UPPER(:SOURCE_SCHEMA),
                UPPER(:SOURCE_TABLE),
                UPPER(:TARGET_ENTITY)
            );
            CALL CES_REF.CES_DBT.CREATE_VIEWS_IF_NOT_EXIST(
                UPPER(:TARGET_SCH),
                UPPER(:TARGET_ENTITY),
                UPPER(:DBT_PROJECT))  ;
            
            RETURN ''Insert on TABLE successful'';
            
        ELSE
            RETURN ''Unexpected Share Type, only TABLE or SCHEMA allowed, please retype parameter SHARE_TYPE'';
        END IF;
   

END IF;

ELSE

RETURN ''Your source is not valid, please review it and ammend it'';

END IF;


END;
';