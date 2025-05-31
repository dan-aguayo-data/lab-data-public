CREATE OR REPLACE PROCEDURE CES_REF.CES_DBT.DEFINE_MSI_VALUE("SOURCE_DATABASE" VARCHAR(16777216), "SOURCE_SCHEMA" VARCHAR(16777216), "SOURCE_TABLE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE

has_msi_sql STRING;
msi_result RESULTSET;
MSI_FLAG STRING;

BEGIN

has_msi_sql :=  ''SELECT CASE WHEN has_msi = 1 THEN ''''YES'''' ELSE ''''NO'''' END AS has_msi FROM (SELECT sum(case when column_name = ''''MULTI_SCHEME_ID'''' then 1 else 0 end) as has_msi FROM "'' || SOURCE_DATABASE || ''"."INFORMATION_SCHEMA"."COLUMNS" WHERE table_schema = '''''' || SOURCE_SCHEMA || '''''' and table_name = '''''' || SOURCE_TABLE || '''''')X''  ;
msi_result := (EXECUTE IMMEDIATE :has_msi_sql);

FOR record IN msi_result DO
    MSI_FLAG := record.has_msi;
END FOR;

Return :MSI_FLAG;

END;
';