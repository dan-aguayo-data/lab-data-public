{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}


WITH sawith0 AS (
    SELECT DISTINCT
        t3167591.c523721043 AS c2
    FROM
        (
            SELECT
                v385483278.currency_code       AS c523721043,
                v385483278.ledger_id           AS pka_ledgerid0,
                v385483278.code_combination_id AS pka_codecombinationid0,
                v385483278.period_name         AS pka_periodname0,
                v385483278.actual_flag         AS pka_actualflag0,
                v385483278.budget_version_id   AS pka_budgetversionid0,
                v385483278.encumbrance_type_id AS pka_encumbrancetypeid0,
                v385483278.translated_flag     AS pka_translatedflag0
            FROM
                (
                    SELECT 
                        glbalances.ACTUAL_FLAG as actual_flag,
                        glbalances.BUDGET_VERSION_ID as budget_version_id,
                        glbalances.CODE_COMBINATION_ID as code_combination_id,
                        glbalances.CURRENCY_CODE as currency_code,
                        glbalances.ENCUMBRANCE_TYPE_ID as encumbrance_type_id,
                        glbalances.LEDGER_ID as ledger_id,
                        glbalances.PERIOD_NAME as period_name,
                        glbalances.TRANSLATED_FLAG as translated_flag
                    FROM
                        CES_ERP.FSCM_PROD_FINGLINQUIRYBALANCES.BALANCE_PVO glbalances
                    WHERE
                        ( glbalances.LEDGER_ID IN ( 300000026463625, 300000035198785, 300000126421633, 300000026463631, 300000001398016, 300000001407023,
                                                    300000001410020, 300000001410050 ) )
                ) v385483278
          
          
        ) t3167591
), sawith1 AS (
    SELECT
        t3167592.c185454959 AS c1,
        t3167592.c103205687 AS c2
    FROM
        (
            SELECT
                v248136131.NAME          AS c185454959,
                v248136131.CURRENCY_CODE AS c103205687,
                v248136131.LANGUAGE      AS c315124100
            FROM
            CES_ERP.FSCM_PROD_ANALYTICSSERVICE.CURRENCIES_TLPVO v248136131
            WHERE
                ( ( ( v248136131.LANGUAGE  = 'US' ) ) )
        ) t3167592
), sawith2 AS (
    SELECT
        d2.c1 AS c1,
        d1.c2 AS c2
    FROM
        sawith0 d1
        LEFT OUTER JOIN sawith1 d2 ON d1.c2 = d2.c2
)
SELECT
    nvl(d901.c1, d901.c2) AS CURRENCY_DESC,
    d901.c2               AS CURRENCY_CODE
FROM
    sawith2 d901
ORDER BY d901.c2