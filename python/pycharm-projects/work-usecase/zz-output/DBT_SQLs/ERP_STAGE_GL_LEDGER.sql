{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}


SELECT DISTINCT
    t288159.c336050406 AS CHART_OF_ACCOUNT,
    t288159.c380357280 AS LEDGER_DESC,
    t288159.c10223035  AS LEDGER_NAME
FROM
    (
        SELECT
            v498378102.name1050              AS c336050406,
            v498378102.description303        AS c380357280,
            v498378102.name496               AS c10223035,
            v498378102.ledger_id             AS pka_ledgerid0,
            v498378102.structure_instance_id AS pka_keyflexfieldstructureinst0
        FROM
            (
                SELECT 
                    ledger.LEDGER_DESCRIPTION                AS description303,
                    ledger.LEDGER_ID as ledger_id,
                    ledger.LEDGER_NAME                       AS name496,
                    ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_NAME AS name1050,
                    ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_STRUCTURE_INSTANCE_ID as structure_instance_id
                FROM 
                    CES_ERP.FSCM_PROD_FINGLLEDGERDEFN.LEDGER_PVO ledger
                WHERE
                    ( 
                       ( 101 ) = ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_APPLICATION_ID
                      AND ( 'GL#' ) = ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_KEY_FLEXFIELD_CODE )
                    AND ( ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) ) )
            ) v498378102
    ) t288159
ORDER BY t288159.c10223035