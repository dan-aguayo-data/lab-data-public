{% macro share_with_control_table_manual(users,databases,schemas,tables) %}

{% set USER_check = users[0] %} 
{% set database_check = databases[0] %} 
{% set schema_check = schemas[0] %}
{% set table_check = tables[0] %}  
{% set environment_name = target.name %}
{% set dbt_project_name = var('share_dbt_project_name') %}


/*USER CONDITIONAL WRAP STARTS */
{% if USER_check == "*"  %}   --IF USER IS MARKED AS SELECT ALL
    {{ log("GETTING LIST OF ENTITIES FROM REFERENCE TABLE USER_MULTI_USER_CODE_ID_REF...." , info=True) }}
    {% set ref_USER_sql %}
        SELECT DISTINCT USER_NAME FROM
        "REF_TABLE_NAME_HERE"
        WHERE UPPER(DBT_PROJECT) = '{{dbt_project_name}}';
    {% endset %}
    {% set ref_USER_results = run_query(ref_USER_sql) %}
    {% if execute %}
        {% set ref_USER_results_list = ref_USER_results.rows %}
    {% else %}
        {% set ref_USER_results_list = [] %}
    {% endif %}
        /*FOR EACH USER COMING FROM SQL REF STARTS */
        {%- for item in ref_USER_results_list -%}  
            {% set USER_name = item['USER_NAME'] %}
            /*DATABASE CONDITIONAL WRAPPER STARTS */
            {% if database_check == "*"  %}
                {{ log("GETTING LIST OF DATABASES FROM CRONTOL TABLE FOR USER: " ~ USER_name , info=True) }}
                {% set control_databases_sql %}
                    SELECT DISTINCT SOURCE_DATABASE FROM
                    "REF_TABLE_HERE"
                    WHERE DBT_PROJECT = '{{dbt_project_name}}' AND TARGET_USER_NAME = '{{USER_name}}' 
                    AND SOURCE_ENVIRONMENT = '{{environment_name}}'  ;
                {% endset %}
                {% set control_databases_results = run_query(control_databases_sql) %}
                {% if execute %}
                    {% set control_databases_results_list = control_databases_results.rows %}
                {% else %}
                    {% set control_databases_results_list = [] %}
                {% endif %}
                {% if control_databases_results_list == "" %}
                    {% set control_databases_results_list = ["1"] %}
                {% endif %}
                {% set database_list = control_databases_results_list %}
                {% set db_list_type = 'table' %}
            {% else %}
                {% set database_list = databases %}
                {% set db_list_type = 'list' %}
            {% endif %}
            /*LOOPING DATABASES IN THE LIST */  
            {% for database in database_list %} 
                {% if db_list_type == 'list' %}
                    {% set database_name = database %}
                {% else %}
                    {% set database_name =  database['SOURCE_DATABASE'] %}
                {% endif %} /*DATABASE CONDITIONAL WRAPPER ENDS */
                /*SCHEMA CONDITIONAL WRAPPER STARTS */
                {% if schema_check == "*"  %}
                    {{ log("GETTING LIST OF SCHEMAS FROM DATABASE: " ~ database_name , info=True) }}
                    {% set control_schemas_sql %}
                        SELECT DISTINCT SOURCE_SCHEMA FROM
                        "REF_TABLE_HERE"
                        WHERE DBT_PROJECT = '{{dbt_project_name}}' AND TARGET_USER_NAME = '{{USER_name}}' 
                        AND SOURCE_ENVIRONMENT = '{{environment_name}}' AND SOURCE_DATABASE = '{{database_name}}'  ;
                    {% endset %}
                    {% set control_schemas_results = run_query(control_schemas_sql) %}
                    {% if execute %}
                        {% set control_schemas_results_list = control_schemas_results.rows %}
                    {% else %}
                        {% set control_schemas_results_list = [] %}
                    {% endif %}
                    {% if control_schemas_results_list == "" %}
                        {% set control_schemas_results_list = ["1"] %}
                    {% endif %}
                    {% set schema_list = control_schemas_results_list %}
                    {% set schema_list_type = 'table' %}
                {% else %}
                    {% set schema_list = schemas %}
                    {% set schema_list_type = 'list' %}
                {% endif %}
                /*LOOPING SCHEMAS IN THE LIST */
                {% for schema in schema_list %}  
                    {% if schema_list_type == 'list' %}
                        {% set schema_name = schema %}
                    {% else %}
                        {% set schema_name =  schema['SOURCE_SCHEMA'] %}
                    {% endif %} /*SCHEMA CONDITIONAL WRAPPER ENDS */
                    /*ASSESSMENT OF SHARE IN EACH DATABASE.SCHEMA*/
                    {% if table_check == "*"  %}
                        /*START TABLE SELECT ALL CONDITION */
                        {{ log("ASSESSING IF SHARE ON " ~ database_name ~ "." ~ schema_name ~ " FOR " ~USER_name, info=True) }}
                        {% set flag_query %}
                            SELECT 
                            SUM(DB_SHCEMA_FLAG) AS DB_SCHEMA_FLAG
                            FROM(
                            SELECT DISTINCT
                            CASE WHEN (SOURCE_DATABASE || '.' || SOURCE_SCHEMA) IS NULL THEN 0 ELSE 1 END AS DB_SHCEMA_FLAG 
                            FROM "REF_TABLE_HERE"
                            WHERE SOURCE_DATABASE = '{{database_name}}' AND SOURCE_SCHEMA = '{{schema_name}}' 
                            AND TARGET_USER_NAME = '{{USER_name}}'  AND SOURCE_ENVIRONMENT = '{{environment_name}}'  
                            AND DBT_PROJECT = '{{dbt_project_name}}'
                            UNION
                            SELECT 
                            0 AS DB_SHCEMA_FLAG ) X;
                        {% endset %}
                        {% set flag_query_results = run_query(flag_query) %}
                        {% if execute %}
                            {% set flag_output = flag_query_results.columns[0].values()[0] %}
                        {% else %}
                            {% set flag_output = None %}
                        {% endif %}
                        /* SHARE CONDITIONAL WHEN THERE IS CONTENT IN DATABASE.SCHEMA UNDER THE REFERENCE TABLE */
                        {%- if flag_output == 1 %}
                            {% set control_sql %}
                                SELECT * FROM "REF_TABLE_HERE"
                                WHERE SOURCE_DATABASE = '{{database_name}}' AND SOURCE_SCHEMA = '{{schema_name}}' 
                                AND TARGET_USER_NAME = '{{USER_name}}' AND DBT_PROJECT = '{{dbt_project_name}}' ;
                            {% endset %}
                            {% set control_results = run_query(control_sql) %}
                            {% if execute %}
                                {% set control_results_list = control_results.rows %}
                            {% else %}
                                {% set control_results_list = [] %}
                            {% endif %}
                            {% set share_query %}
                                {%- for item in control_results_list -%}
                                    {% set TGT_USER_CODE =  item['TARGET_USER_NAME'] %}
                                    {% set TGT_DATABASE =  item['TARGET_DATABASE'] %}
                                    {% set TGT_SCHEMA =  item['TARGET_SCHEMA'] %}
                                    {% set TGT_TABLE =  item['TARGET_TABLE'] %}
                                    {% set DATA_SHARE =  item['DATA_SHARE'] %}
                                    {% set SOURCE_DATABASE =  item['SOURCE_DATABASE'] %}
                                    {{ log("Generating share for USER: " ~TGT_USER_CODE ~ " on object: " ~ TGT_DATABASE ~ "." ~ TGT_SCHEMA  ~ "."  ~ TGT_TABLE, info=True) }}
                                    {%- if item['DATA_SHARE'] == 1 %}
                                        CREATE OR REPLACE SECURE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                        SELECT 
                                        {% if item['COLUMNS_TO_SHARE'] is not none %}
                                            {{ item['COLUMNS_TO_SHARE'] }}
                                        {% else %}
                                            *
                                        {% endif %}
                                        FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                        WHERE 1 = 1
                                        {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                            AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                        {% endif %}
                                        {% if item['OTHER_FILTERS'] is not none %}
                                            AND {{ item['OTHER_FILTERS'] }}
                                        {% endif %}
                                        )
                                        ;
                                                
                                        {%- if item['TARGET_DB_ROLES'] -%}        
                                            {%- set db_roles = item['TARGET_DB_ROLES'].split(',') -%}
                                            {%- for db_role in db_roles -%}
                                                GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO DATABASE ROLE {{db_role.strip()}}
                                                ;
                                            {%- endfor -%}
                                        {%- endif -%} 
                                    {% else %}  
                                        CREATE OR REPLACE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                        SELECT 
                                        {% if item['COLUMNS_TO_SHARE'] is not none %}
                                            {{ item['COLUMNS_TO_SHARE'] }}
                                        {% else %}
                                            *
                                        {% endif %}
                                        FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                        WHERE 1 = 1
                                        {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                            AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                        {% endif %}
                                        {% if item['OTHER_FILTERS'] is not none %}
                                            AND {{ item['OTHER_FILTERS'] }}
                                        {% endif %}
                                        )
                                        ;
                                        {%- set target_roles = item['TARGET_ROLES'].split(',') -%}
                                        {%- for role in target_roles -%}
                                            GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO ROLE {{role.strip()}}
                                            ;
                                        {%- endfor -%} 
                                    {% endif %} 
                                {%- endfor -%}
                            {% endset %}
                                    {{ log("Query to execute for " ~ USER_name ~" on "~ database_name ~ "." ~ schema_name ~ ": " ~ share_query, info=True) }}
                                    {% do run_query(share_query) %}
                                    {% do log('Query executed.', info=True) %}/*SHARE ON ROLE ENDS */       
                        {%- else %}
                            {{ log("Nothing to share for: " ~USER_name  , info=True) }}
                        {%- endif %}  /*END TABLE SELECT ALL CONDITION */       
                    {%- else %} 
                    	/*START TABLE SPECIFIC SELECT CONDITION */
                        {% for table in tables %}
                            {% set table_name = table %}
                            {{ log("ASSESSING IF TABLE SHARE ON " ~ database_name ~ "." ~ schema_name ~ "." ~ table_name ~" FOR " ~USER_name, info=True) }}
                            {% set flag_query %}
                                SELECT 
                                SUM(DB_SHCEMA_FLAG) AS DB_SCHEMA_FLAG
                                FROM(
                                SELECT DISTINCT
                                CASE WHEN (SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE) IS NULL THEN 0 ELSE 1 END AS DB_SHCEMA_FLAG 
                                FROM "REF_TABLE_HERE"
                                WHERE SOURCE_DATABASE = '{{database_name}}' 
                                AND SOURCE_SCHEMA = '{{schema_name}}' 
                                AND TARGET_USER_NAME = '{{USER_name}}'  
                                AND SOURCE_ENVIRONMENT = '{{environment_name}}'
                                AND SOURCE_TABLE =  '{{table_name}}'
                                AND DBT_PROJECT = '{{dbt_project_name}}'
                                UNION
                                SELECT 
                                0 AS DB_SHCEMA_FLAG ) X;
                            {% endset %}
                            {% set flag_query_results = run_query(flag_query) %}
                            {% if execute %}
                                {% set flag_output = flag_query_results.columns[0].values()[0] %}
                            {% else %}
                                {% set flag_output = None %}
                            {% endif %}
                            {%- if flag_output == 1 %}
                                {% set control_sql %}
                                    SELECT * FROM "REF_TABLE_HERE"
                                    WHERE SOURCE_DATABASE = '{{database_name}}' 
                                    AND SOURCE_SCHEMA = '{{schema_name}}' 
                                    AND SOURCE_TABLE =  '{{table_name}}'
                                    AND TARGET_USER_NAME = '{{USER_name}}' 
                                    AND DBT_PROJECT = '{{dbt_project_name}}' ;
                                {% endset %}
                                {% set control_results = run_query(control_sql) %}
                                {% if execute %}
                                    {% set control_results_list = control_results.rows %}
                                {% else %}
                                    {% set control_results_list = [] %}
                                {% endif %}
                                {% set share_query %}
                                    {%- for item in control_results_list -%}
                                        {% set TGT_USER_CODE =  item['TARGET_USER_NAME'] %}
                                        {% set TGT_DATABASE =  item['TARGET_DATABASE'] %}
                                        {% set TGT_SCHEMA =  item['TARGET_SCHEMA'] %}
                                        {% set TGT_TABLE =  item['TARGET_TABLE'] %}
                                        {% set DATA_SHARE =  item['DATA_SHARE'] %}
                                        {% set SOURCE_DATABASE =  item['SOURCE_DATABASE'] %}
                                        {{ log("Generating share for USER: " ~TGT_USER_CODE ~ " on object: " ~ TGT_DATABASE ~ "." ~ TGT_SCHEMA  ~ "."  ~ TGT_TABLE, info=True) }}
                                        {%- if item['DATA_SHARE'] == 1 %}
                                            CREATE OR REPLACE SECURE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                            SELECT 
                                            {% if item['COLUMNS_TO_SHARE'] is not none %}
                                                {{ item['COLUMNS_TO_SHARE'] }}
                                            {% else %}
                                                *
                                            {% endif %}
                                            FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                            WHERE 1 = 1
                                            {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                                AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                            {% endif %}
                                            {% if item['OTHER_FILTERS'] is not none %}
                                                AND {{ item['OTHER_FILTERS'] }}
                                            {% endif %}
                                            )
                                            ;
                                            {%- if item['TARGET_DB_ROLES'] -%}        
                                                {%- set db_roles = item['TARGET_DB_ROLES'].split(',') -%}
                                                {%- for db_role in db_roles -%}
                                                    GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO DATABASE ROLE {{db_role.strip()}}
                                                    ;
                                                {%- endfor -%}
                                            {%- endif -%} 
                                        {% else %}  
                                            CREATE OR REPLACE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                            SELECT 
                                            {% if item['COLUMNS_TO_SHARE'] is not none %}
                                                {{ item['COLUMNS_TO_SHARE'] }}
                                            {% else %}
                                                *
                                            {% endif %}
                                            FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                            WHERE 1 = 1
                                            {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                                AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                            {% endif %}
                                            {% if item['OTHER_FILTERS'] is not none %}
                                                AND {{ item['OTHER_FILTERS'] }}
                                            {% endif %}
                                            )
                                            ;
                                            {%- set target_roles = item['TARGET_ROLES'].split(',') -%}
                                            {%- for role in target_roles -%}
                                                GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO ROLE {{role.strip()}}
                                                ;
                                            {%- endfor -%} 
                                        {% endif %} 
                                    {%- endfor -%}
                                {% endset %}
                                        {{ log("Query to execute for " ~ USER_name ~" on "~ database_name ~ "." ~ schema_name ~ ": " ~ share_query, info=True) }}
                                        {% do run_query(share_query) %}
                                        {% do log('Query executed.', info=True) %}/*SHARE ON ROLE ENDS */ 
                            {%- else %}
                                {{ log("Nothing to share for: " ~USER_name ~ " for table " ~ table_name , info=True) }}
                            {%- endif %} /*END TABLE SPECIFIC SELECT CONDITION */
                        {% endfor %}  --/*ENDS TABLE SPECIFIC SELECT LOOP*/    
                    {%- endif %} --/*END TABLE CONDITION ASSESSMENT */
                {% endfor %}  --/*ENDS SCHEMA LOOP*/     
            {% endfor %}  --/*END DATABASE LOOP*/
        {% endfor %}  --/*FOR EACH USER COMING FROM SQL REF ENDS */
/*USER CONDITIONAL WRAP CONTINUES */
{% else %} --IF USER IS MARKED AS SPEFICIC 
        /*LOOP EACH USER COMING FROM DBT STATEMENT */
        {% for USER in entities %}  
            {% set USER_name = USER %}
            /*DATABASE CONDITIONAL WRAPPER STARTS */
            {% if database_check == "*"  %}
                {{ log("GETTING LIST OF DATABASES FROM CRONTOL TABLE FOR USER: " ~ USER_name , info=True) }}
                {% set control_databases_sql %}
                    SELECT DISTINCT SOURCE_DATABASE 
                    FROM "REF_TABLE_HERE"
                    WHERE DBT_PROJECT = '{{dbt_project_name}}' AND TARGET_USER_NAME = '{{USER_name}}' 
                    AND SOURCE_ENVIRONMENT = '{{environment_name}}'  ;
                {% endset %}
                {% set control_databases_results = run_query(control_databases_sql) %}
                {% if execute %}
                    {% set control_databases_results_list = control_databases_results.rows %}
                {% else %}
                    {% set control_databases_results_list = [] %}
                {% endif %}
                {% if control_databases_results_list == "" %}
                    {% set control_databases_results_list = ["1"] %}
                {% endif %}
                {% set database_list = control_databases_results_list %}
                {% set db_list_type = 'table' %}
            {% else %}
                {% set database_list = databases %}
                {% set db_list_type = 'list' %}
            {% endif %}
            /*LOOPING DATABASES IN THE LIST */  
            {% for database in database_list %}
                {% if db_list_type == 'list' %}
                    {% set database_name = database %}
                {% else %}
                    {% set database_name =  database['SOURCE_DATABASE'] %}
                {% endif %} /*DATABASE CONDITIONAL WRAPPER ENDS */
                /*SCHEMA CONDITIONAL WRAPPER STARTS */
                {% if schema_check == "*"  %}
                    {{ log("GETTING LIST OF SCHEMAS FROM DATABASE: " ~ database_name , info=True) }}
                    {% set control_schemas_sql %}
                        SELECT DISTINCT SOURCE_SCHEMA 
                        FROM "REF_TABLE_HERE"
                        WHERE DBT_PROJECT = '{{dbt_project_name}}' AND TARGET_USER_NAME = '{{USER_name}}' 
                        AND SOURCE_ENVIRONMENT = '{{environment_name}}' AND SOURCE_DATABASE = '{{database_name}}'  ;
                    {% endset %}
                    {% set control_schemas_results = run_query(control_schemas_sql) %}
                    {% if execute %}
                        {% set control_schemas_results_list = control_schemas_results.rows %}
                    {% else %}
                        {% set control_schemas_results_list = [] %}
                    {% endif %}
                    {% if control_schemas_results_list == "" %}
                        {% set control_schemas_results_list = ["1"] %}
                    {% endif %}
                    {% set schema_list = control_schemas_results_list %}
                    {% set schema_list_type = 'table' %}
                {% else %}
                    {% set schema_list = schemas %}
                    {% set schema_list_type = 'list' %}
                {% endif %}
                /*LOOPING SCHEMAS IN THE LIST */
                {% for schema in schema_list %}
                    {% if schema_list_type == 'list' %}
                        {% set schema_name = schema %}
                    {% else %}
                        {% set schema_name =  schema['SOURCE_SCHEMA'] %}
                    {% endif %}  /*SCHEMA CONDITIONAL WRAPPER ENDS */
                    /*ASSESSMENT OF SHARE IN EACH DATABASE.SCHEMA*/
                    {% if table_check == "*"  %}                    
                        /*ASSESSMENT OF SHARE IN EACH DATABASE.SCHEMA*/
                        {{ log("ASSESSING IF SHARE ON " ~ database_name ~ "." ~ schema_name ~ " FOR " ~USER_name, info=True) }}
                        {% set flag_query %}
                            SELECT 
                            SUM(DB_SHCEMA_FLAG) AS DB_SCHEMA_FLAG
                            FROM(
                            SELECT DISTINCT
                            CASE WHEN (SOURCE_DATABASE || '.' || SOURCE_SCHEMA) IS NULL THEN 0 ELSE 1 END AS DB_SHCEMA_FLAG 
                            FROM "REF_TABLE_HERE"
                            WHERE SOURCE_DATABASE = '{{database_name}}' AND SOURCE_SCHEMA = '{{schema_name}}' 
                            AND TARGET_USER_NAME = '{{USER_name}}'  AND SOURCE_ENVIRONMENT = '{{environment_name}}'  
                            AND DBT_PROJECT = '{{dbt_project_name}}'
                            UNION
                            SELECT 
                            0 AS DB_SHCEMA_FLAG ) X;
                        {% endset %}
                        {% set flag_query_results = run_query(flag_query) %}
                        {% if execute %}
                            {% set flag_output = flag_query_results.columns[0].values()[0] %}
                        {% else %}
                            {% set flag_output = None %}
                        {% endif %}
                        /* SHARE CONDITIONAL WHEN THERE IS CONTENT IN DATABASE.SCHEMA UNDER THE REFERENCE TABLE */
                        {%- if flag_output == 1 %}
                            {% set control_sql %}
                                SELECT * FROM "REF_TABLE_HERE"
                                WHERE SOURCE_DATABASE = '{{database_name}}' AND TARGET_USER_NAME = '{{USER_name}}' 
                                AND SOURCE_SCHEMA = '{{schema_name}}' AND DBT_PROJECT = '{{dbt_project_name}}' ;
                            {% endset %}
                            {% set control_results = run_query(control_sql) %}
                            {% if execute %}
                                {% set control_results_list = control_results.rows %}
                            {% else %}
                                {% set control_results_list = [] %}
                            {% endif %}
                            {% set share_query %}
                                {%- for item in control_results_list -%}
                                    {% set TGT_USER_CODE =  item['TARGET_USER_NAME'] %}
                                    {% set TGT_DATABASE =  item['TARGET_DATABASE'] %}
                                    {% set TGT_SCHEMA =  item['TARGET_SCHEMA'] %}
                                    {% set TGT_TABLE =  item['TARGET_TABLE'] %}
                                    {% set DATA_SHARE =  item['DATA_SHARE'] %}
                                    {% set SOURCE_DATABASE =  item['SOURCE_DATABASE'] %}
                                    {%- if item['DATA_SHARE'] == 1 %}
                                        CREATE OR REPLACE SECURE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                        SELECT 
                                        {% if item['COLUMNS_TO_SHARE'] is not none %}
                                            {{ item['COLUMNS_TO_SHARE'] }}
                                        {% else %}
                                            *
                                        {% endif %}
                                        FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                        WHERE 1 = 1
                                        {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                            AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                        {% endif %}
                                        {% if item['OTHER_FILTERS'] is not none %}
                                            AND {{ item['OTHER_FILTERS'] }}
                                        {% endif %}
                                        )
                                        ;
                                        {%- if item['TARGET_DB_ROLES'] -%}        
                                            {%- set db_roles = item['TARGET_DB_ROLES'].split(',') -%}
                                            {%- for db_role in db_roles -%}
                                                GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO DATABASE ROLE {{db_role.strip()}}
                                                ;
                                            {%- endfor -%}
                                        {%- endif -%} 
                                    {% else %} 
                                        {{ log("Generating sahre for USER: " ~TGT_USER_CODE ~ " on object: " ~ TGT_DATABASE ~ "." ~ TGT_SCHEMA  ~ "."  ~ TGT_TABLE, info=True) }}
                                        CREATE OR REPLACE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                        SELECT 
                                        {% if item['COLUMNS_TO_SHARE'] is not none %}
                                            {{ item['COLUMNS_TO_SHARE'] }}
                                        {% else %}
                                            *
                                        {% endif %}
                                        FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                        WHERE 1 = 1
                                        {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                            AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                        {% endif %}
                                        {% if item['OTHER_FILTERS'] is not none %}
                                            AND {{ item['OTHER_FILTERS'] }}
                                        {% endif %}
                                        )
                                        ;
                                        {%- set target_roles = item['TARGET_ROLES'].split(',') -%}
                                        
                                        {%- for role in target_roles -%}
                                            GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO ROLE {{ role.strip() }}
                                        ;
                                        {%- endfor -%} 
                                    {% endif %} 
                                {%- endfor -%}
                            {% endset %}
                                    {{ log("Query to execute for " ~ USER_name ~" on "~ database_name ~ "." ~ schema_name ~ ": " ~ share_query, info=True) }}
                                    {% do run_query(share_query) %}
                                    {% do log('Query executed.', info=True) %} /*SHARE ON ROLE ENDS */
                        {%- else %}
                            {{ log("No Views or Schema Shares to create for: " ~USER_name  , info=True) }}
                        {%- endif %}    --/*SHARE CONDITIONAL WHEN THERE IS CONTENT IN DATABASE.SCHEMA UNDER THE REFERENCE TABLE */     
                    {%- else %}
                    	/*START TABLE SPECIFIC SELECT CONDITION */
                        {% for table in tables %}
                            {% set table_name = table %}
                            {{ log("ASSESSING IF TABLE SHARE ON " ~ database_name ~ "." ~ schema_name ~ "." ~ table_name ~" FOR " ~USER_name, info=True) }}
                            {% set flag_query %}
                                SELECT 
                                SUM(DB_SHCEMA_FLAG) AS DB_SCHEMA_FLAG
                                FROM(
                                SELECT DISTINCT
                                CASE WHEN (SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE) IS NULL THEN 0 ELSE 1 END AS DB_SHCEMA_FLAG 
                                FROM "REF_TABLE_HERE"
                                WHERE SOURCE_DATABASE = '{{database_name}}' 
                                AND SOURCE_SCHEMA = '{{schema_name}}' 
                                AND TARGET_USER_NAME = '{{USER_name}}'  
                                AND SOURCE_ENVIRONMENT = '{{environment_name}}'
                                AND SOURCE_TABLE =  '{{table_name}}'
                                AND DBT_PROJECT = '{{dbt_project_name}}'
                                UNION
                                SELECT 
                                0 AS DB_SHCEMA_FLAG ) X;
                            {% endset %}
                            {% set flag_query_results = run_query(flag_query) %}
                            {% if execute %}
                                {% set flag_output = flag_query_results.columns[0].values()[0] %}
                            {% else %}
                                {% set flag_output = None %}
                            {% endif %}
                            {%- if flag_output == 1 %}
                                {% set control_sql %}
                                    SELECT * FROM "REF_TABLE_HERE"
                                    WHERE SOURCE_DATABASE = '{{database_name}}' 
                                    AND SOURCE_SCHEMA = '{{schema_name}}' 
                                    AND SOURCE_TABLE =  '{{table_name}}'
                                    AND TARGET_USER_NAME = '{{USER_name}}' 
                                    AND DBT_PROJECT = '{{dbt_project_name}}' ;
                                {% endset %}
                                {% set control_results = run_query(control_sql) %}
                                {% if execute %}
                                    {% set control_results_list = control_results.rows %}
                                {% else %}
                                    {% set control_results_list = [] %}
                                {% endif %}
                                {% set share_query %}
                                    {%- for item in control_results_list -%}
                                        {% set TGT_USER_CODE =  item['TARGET_USER_NAME'] %}
                                        {% set TGT_DATABASE =  item['TARGET_DATABASE'] %}
                                        {% set TGT_SCHEMA =  item['TARGET_SCHEMA'] %}
                                        {% set TGT_TABLE =  item['TARGET_TABLE'] %}
                                        {% set DATA_SHARE =  item['DATA_SHARE'] %}
                                        {% set SOURCE_DATABASE =  item['SOURCE_DATABASE'] %}
                                        {{ log("Generating share for USER: " ~TGT_USER_CODE ~ " on object: " ~ TGT_DATABASE ~ "." ~ TGT_SCHEMA  ~ "."  ~ TGT_TABLE, info=True) }}
                                        {%- if item['DATA_SHARE'] == 1 %}
                                            CREATE OR REPLACE SECURE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                            SELECT 
                                            {% if item['COLUMNS_TO_SHARE'] is not none %}
                                                {{ item['COLUMNS_TO_SHARE'] }}
                                            {% else %}
                                                *
                                            {% endif %}
                                            FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                            WHERE 1 = 1
                                            {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                                AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                            {% endif %}
                                            {% if item['OTHER_FILTERS'] is not none %}
                                                AND {{ item['OTHER_FILTERS'] }}
                                            {% endif %}
                                            )
                                            ;
                                                    
                                            {%- if item['TARGET_DB_ROLES'] -%}        
                                                {%- set db_roles = item['TARGET_DB_ROLES'].split(',') -%}
                                                {%- for db_role in db_roles -%}
                                                    GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO DATABASE ROLE {{db_role.strip()}}
                                                    ;
                                                {%- endfor -%}
                                            {%- endif -%} 
                                        {% else %}  
                                            CREATE OR REPLACE VIEW {{ item['TARGET_DATABASE'] }}.{{ item['TARGET_SCHEMA'] }}.{{ item['TARGET_TABLE'] }} as (
                                            SELECT 
                                            {% if item['COLUMNS_TO_SHARE'] is not none %}
                                                {{ item['COLUMNS_TO_SHARE'] }}
                                            {% else %}
                                                *
                                            {% endif %}
                                            FROM {{ item['SOURCE_DATABASE'] }}.{{ item['SOURCE_SCHEMA'] }}.{{ item['SOURCE_TABLE'] }}
                                            WHERE 1 = 1
                                            {% if item['HAS_MSI_FILTER'] != 'NO' %}
                                                AND (multi_USER_CODE_id IN ({{ item['MULTIUSER_CODE_ID'] }}) or multi_USER_CODE_id is null)
                                            {% endif %}
                                            {% if item['OTHER_FILTERS'] is not none %}
                                                AND {{ item['OTHER_FILTERS'] }}
                                            {% endif %}
                                            )
                                            ;
                                            {%- set target_roles = item['TARGET_ROLES'].split(',') -%}
                                            {%- for role in target_roles -%}
                                                GRANT SELECT ON VIEW {{ TGT_DATABASE }}.{{ TGT_SCHEMA }}.{{ TGT_TABLE }} TO ROLE {{role.strip()}}
                                                ;
                                            {%- endfor -%} 
                                        {% endif %} 
                                    {%- endfor -%}
                                {% endset %}
                                        {{ log("Query to execute for " ~ USER_name ~" on "~ database_name ~ "." ~ schema_name ~ ": " ~ share_query, info=True) }}
                                        {% do run_query(share_query) %}
                                        {% do log('Query executed.', info=True) %}/*SHARE ON ROLE ENDS */ 
                            {%- else %}
                                {{ log("Nothing to share for: " ~USER_name ~ " for table " ~ table_name , info=True) }}
                            {%- endif %} /*END TABLE SPECIFIC SELECT CONDITION */
                        {% endfor %}  --/*ENDS TABLE SPECIFIC SELECT LOOP*/    
                    {%- endif %} --/*END TABLE CONDITION ASSESSMENT */
                {% endfor %}  --/*ENDS SCHEMA LOOP*/      
            {% endfor %}  --/*ENDS DATABASE LOOP*/ 
        {% endfor %}   --/*ENDS USER LOOP*/ 
        {{ log("END OF MACRO. "  , info=True) }}
{% endif %}  /*USER CONDITIONAL WRAP ENDS */
{% endmacro %}

