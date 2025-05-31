{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}



WITH sawith0 AS (
    SELECT
        t5533395.c180575348 AS c25,
        t5533395.c10223035  AS c26,
        t5533395.c523721043 AS c27,
        t5533395.c355935403 AS c28,
        t5533395.c233929166 AS c29,
        t5533395.c31199523  AS c30,
        t5533395.c7207427   AS c31,
        t5533395.c433723576 AS c32,
        t5533395.c417910867 AS c33,
        t5533395.c276323548 AS c34,
        t5533395.c420124855 AS c35,
        t5533395.c123644693 AS c37,
        t5533395.c210356818 AS c38,
        t5533395.c15439104  AS c39,
        t5533395.c32297949  AS c41,
        t5533395.c510997710 AS c60,
        t5533395.c422791590 AS c61,
        t5533395.c355931595 AS c62,
        t5533395.c42913078  AS c63,
        t5533395.c337144646 AS c64,
        t5533395.c286871080 AS c65,
        t5533395.c175416157 AS c66,
        t5533395.c167910701 AS c67,
        t5533395.c201571443 AS c68,
        t5533395.c20458166  AS c69,
        t5533395.c62026046  AS c70,
        t5533395.c63229774  AS c71,
        t5533395.c93215482  AS c72,
        t5533395.c146132031 AS c73,
        t5533395.c374292536 AS c74,
        t5533395.c372899400 AS c75,
        t5533395.c245530055 AS c76,
        t5533395.c60418816  AS c77
    FROM
        (
            SELECT
                v208318186.concat_values          AS c180575348,
                v498378102.name496                AS c10223035,
                v385483278.currency_code          AS c523721043,
                v385483278.period_name            AS c355935403,
                v208318186.gl_balancing_          AS c233929166,
                v208318186.fa_cost_ctr_           AS c31199523,
                v208318186.gl_account_            AS c7207427,
                v208318186.gl_intercompany_       AS c433723576,
                v208318186.gl_mat_type_           AS c417910867,
                v208318186.gl_scheme_             AS c276323548,
                v208318186.gl_spare_              AS c420124855,
                v385483278.last_update_date1      AS c123644693,
                v208318186.s_g_0                  AS c210356818,
                v498378102.ledger_id              AS c15439104,
                v385483278.actual_flag            AS c32297949,
                v212661565.fiscal_year_number     AS c510997710,
                v212661565.calendar_id            AS c422791590,
                v212661565.fiscal_quarter_number  AS c355931595,
                v212661565.fiscal_period_number   AS c42913078,
                v385483278.currency_code1         AS c337144646,
                v385483278.translated_flag        AS c286871080,
                v385483278.period_net_dr_beq      AS c175416157,
                v385483278.period_net_cr_beq      AS c167910701,
                v385483278.period_net_dr          AS c201571443,
                v385483278.period_net_cr          AS c20458166,
                v385483278.begin_balance_dr_beq   AS c62026046,
                v385483278.begin_balance_cr_beq   AS c63229774,
                v385483278.begin_balance_dr       AS c93215482,
                v385483278.begin_balance_cr       AS c146132031,
                v385483278.quarter_to_date_dr_beq AS c374292536,
                v385483278.quarter_to_date_cr_beq AS c372899400,
                v385483278.quarter_to_date_dr     AS c245530055,
                v385483278.quarter_to_date_cr     AS c60418816,
                v385483278.code_combination_id    AS c50749628,
                v385483278.ledger_id              AS pka_ledgerid0,
                v385483278.budget_version_id      AS pka_budgetversionid0,
                v385483278.encumbrance_type_id    AS pka_encumbrancetypeid0,
                v385483278.ledger_id1             AS pka_glledgerledgerid0,
                v212661565.fiscal_period_set_id   AS pka_fiscalperiodsetid0,
                v212661565.fiscal_period_set_name AS pka_fiscalperiodsetname0,
                v212661565.fiscal_period_type     AS pka_fiscalperiodtype0,
                v212661565.fiscal_period_name     AS pka_fiscalperiodname0,
                v212661565.period_set_id          AS pka_glcalendarsperiodsetid0,
                v212661565.period_type_id         AS pka_glcalendarsperiodtypeid0
                --,
                --v470773127.value_id               AS pka_valueid0,
                --v343808451.value_id               AS pka_valueid1,
                --v278434713.value_id               AS pka_valueid2,
                --v344303243.value_id               AS pka_valueid3,
                --v34792864.value_id                AS pka_valueid4,
                --v412550833.value_id               AS pka_valueid5,
                --v435856572.value_id               AS pka_valueid6
            FROM
                (
                    SELECT 
                        glbalances.ACTUAL_FLAG as actual_flag,
                        glbalances.BEGIN_BALANCE_CR as begin_balance_cr,
                        glbalances.BEGIN_BALANCE_CR_BEQ as begin_balance_cr_beq,
                        glbalances.BEGIN_BALANCE_DR as begin_balance_dr,
                        glbalances.BEGIN_BALANCE_DR_BEQ as begin_balance_dr_beq,
                        glbalances.BUDGET_VERSION_ID as budget_version_id,
                        glbalances.CODE_COMBINATION_ID as code_combination_id,
                        glbalances.CURRENCY_CODE as currency_code,
                        glbalances.ENCUMBRANCE_TYPE_ID as encumbrance_type_id,
                        glbalances.LAST_UPDATE_DATE_BAL AS last_update_date1,
                        glbalances.LEDGER_ID as ledger_id,
                        glbalances.PERIOD_NAME as period_name,
                        glbalances.PERIOD_NET_CR as period_net_cr,
                        glbalances.PERIOD_NET_CR_BEQ as period_net_cr_beq,
                        glbalances.PERIOD_NET_DR as period_net_dr,
                        glbalances.PERIOD_NET_DR_BEQ as period_net_dr_beq,
                        glbalances.GL_BALANCES_QUARTER_TO_DATE_CR as quarter_to_date_cr,
                        glbalances.GL_BALANCES_QUARTER_TO_DATE_CR_BEQ as quarter_to_date_cr_beq,
                        glbalances.GL_BALANCES_QUARTER_TO_DATE_DR as quarter_to_date_dr,
                        glbalances.GL_BALANCES_QUARTER_TO_DATE_DR_BEQ as quarter_to_date_dr_beq,
                        glbalances.TRANSLATED_FLAG as translated_flag,
                        glbalances.CHART_OF_ACCOUNTS_ID as chart_of_accounts_id, --glbalances
                        glbalances.LEDGER_CURRENCY_CODE     AS currency_code1, --glbalances
                        glbalances.LEDGER_ID         AS ledger_id1 --glbalances
                    FROM
                        CES_ERP.FREQ_FSCM_PROD_FINGLINQUIRYBALANCES.BALANCE_PVO glbalances
                 --   WHERE
                 -- ( ( glbalances.ledger_id IN ( 300000026463625, 300000035198785, 300000126421633, 300000026463631, 300000001398016, 300000001407023, 300000001410020, 300000001410050, 300000094032925 ) ) )
                 --glbalances.ledger_id =300000126421633
                ) v385483278,
                (

                    SELECT       
                    fiscalperiod.FISCAL_PERIOD_NAME as fiscal_period_name,
                    fiscalperiod.FISCAL_PERIOD_FISCAL_PERIOD_NUMBER as fiscal_period_number,
                    fiscalperiod.FISCAL_PERIOD_SET_ID  as fiscal_period_set_id,
                    fiscalperiod.FISCAL_PERIOD_SET_NAME as fiscal_period_set_name,
                    fiscalperiod.FISCAL_PERIOD_TYPE as fiscal_period_type,
                    fiscalperiod.FISCAL_QUARTER_FISCAL_QUARTER_NUMBER as fiscal_quarter_number,
                    fiscalperiod.FISCAL_YEAR_FISCAL_YEAR_NUMBER as fiscal_year_number,
                    fiscalperiod.LEDGER_LEDGER_ID as l_ledger_id,
                    fiscalperiod.GL_CALENDARS_CALENDAR_ID as calendar_id,
                    fiscalperiod.GL_CALENDARS_PERIOD_SET_ID as period_set_id,
                    fiscalperiod.GL_CALENDARS_PERIOD_TYPE_ID as period_type_id
                    FROM
                    CES_ERP.FREQ_FSCM_PROD_FINGLCALACC.GLFISCAL_PERIOD_PVO fiscalperiod
                  --where  fiscalperiod.LEDGER_LEDGER_ID =300000126421633
                  
                      
                ) v212661565,
                (
                    SELECT 
                        ledger.LEDGER_ID as ledger_id,
                        ledger.LEDGER_NAME AS name496
                    FROM
                        CES_ERP.FREQ_FSCM_PROD_FINGLLEDGERDEFN.LEDGER_PVO ledger --FscmTopModelAM.FinGlLedgerDefnAM.LedgerPVO
                    WHERE
                        ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) )
                ) v498378102,
                (
                
      SELECT 
            biflexfieldeo.CODE_COMBINATION_CODE_COMBINATION_ID                                                      AS s_g_0,
            biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID                                                     AS s_g_1,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, NULL) )                    AS fa_cost_ctr_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Cost Center Scheme Ledger', 18063, 'Cost Center Scheme VIC Ledger', 23063, 'Cost Center Scheme VIC NOP Ledger', 3001, 'Cost Center Service Co Ledger', 7037, 'Cost Center WARRRL WA Ledger', NULL) )   AS fa_cost_ctr_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, NULL) )                    AS gl_account_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Account Scheme Ledger', 18063, 'Account Scheme VIC Ledger', 23063, 'Account Scheme VIC NOP Ledger', 3001, 'Account Service Co Ledger', 7037, 'Account WARRRL WA Ledger', NULL) )           AS gl_account_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, NULL) )                    AS gl_balancing_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 18063, 'Company Scheme VIC Ledger', 23063, 'Company Scheme VIC NOP Ledger', 3001, 'Company Service Co Ledger', 7037, 'Company WARRRL WA Ledger', NULL) )           AS gl_balancing_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, NULL) )                    AS gl_intercompany_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 18063, 'Company Scheme VIC Ledger', 23063, 'Company Scheme VIC NOP Ledger', 3001, 'Company Service Co Ledger', 7037, 'Company WARRRL WA Ledger', NULL) )           AS gl_intercompany_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL) )                                                  AS gl_mat_type_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Mat Type Scheme Ledger', 18063, 'Mat Type Scheme VIC Ledger', 23063, 'Mat Type Scheme VIC NOP Ledger', 7037, 'Mat Type WARRRL WA Ledger', NULL) )                                             AS gl_mat_type_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL) )     AS gl_scheme_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, 'Scheme Service Co Ledger', NULL) ) AS gl_scheme_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL) )                    AS gl_spare_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Spare Scheme Ledger', 18063, 'Spare Scheme VIC Ledger', 23063, 'Spare Scheme VIC NOP Ledger', 3001, 'Spare Service Co Ledger', 7037, 'Spare WARRRL WA Ledger', NULL) )               AS gl_spare_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 
                                                        18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_6,
                                                         23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                                || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                                || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                                || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                                || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                                || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_6,                                                               
                                                        3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 
                                                        7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                               || '-' || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL) ) AS
                                                                concat_values,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_2,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_5,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_6
        FROM CES_ERP.FREQ_FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO biflexfieldeo
    ) v208318186
            WHERE
                ( v385483278.ledger_id = v212661565.l_ledger_id
                  AND v385483278.period_name = v212661565.fiscal_period_name
                  AND v385483278.ledger_id = v498378102.ledger_id
                  AND v385483278.code_combination_id = v208318186.s_g_0
                  AND v385483278.chart_of_accounts_id = v208318186.s_g_1
                  )
                --AND ( ( NOT ( ( v385483278.last_update_date1 < TO_TIMESTAMP(:P_EXTRACT_DATE_TS, 'yyyy-mm-dd hh24:mi:ss.ff') - interval '7' day ) ) ) )
        ) t5533395
    WHERE
        ( t5533395.c210356818 = t5533395.c50749628 )
), sawith1 AS (
    SELECT
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c66 - d1.c67
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68 - d1.c69
            END
        )                                       AS c7,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c66
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68
            END
        )                                       AS c8,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c67
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c69
            END
        )                                       AS c9,
        SUM(d1.c68 - d1.c69)                    AS c19,
        SUM(d1.c68)                             AS c20,
        SUM(d1.c69)                             AS c21,
        d1.c25                                  AS c25,
        d1.c26                                  AS c26,
        d1.c27                                  AS c27,
        d1.c28                                  AS c28,
        d1.c29                                  AS c29,
        d1.c30                                  AS c30,
        d1.c31                                  AS c31,
        d1.c32                                  AS c32,
        d1.c33                                  AS c33,
        d1.c34                                  AS c34,
        d1.c35                                  AS c35,
        d1.c37                                  AS c37,
        d1.c38                                  AS c38,
        d1.c39                                  AS c39,
        d1.c41                                  AS c41,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c70 + d1.c66 -(d1.c71 + d1.c67)
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68 + d1.c72 -(d1.c69 + d1.c73)
            END
        )                                       AS c42,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c70 + d1.c66
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68 + d1.c72
            END
        )                                       AS c43,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c71 + d1.c67
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c69 + d1.c73
            END
        )                                       AS c44,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c66 + d1.c74 -(d1.c67 + d1.c75)
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68 + d1.c76 -(d1.c69 + d1.c77)
            END
        )                                       AS c45,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c66 + d1.c74
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c68 + d1.c76
            END
        )                                       AS c46,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c67 + d1.c75
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c69 + d1.c77
            END
        )                                       AS c47,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c70 - d1.c71
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c72 - d1.c73
            END
        )                                       AS c48,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c70
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c72
            END
        )                                       AS c49,
        SUM(
            CASE
                WHEN d1.c27 = d1.c64
                     AND d1.c65 IS NULL THEN
                    d1.c71
                WHEN d1.c65 = 'R'
                     AND d1.c64 <> d1.c27 THEN
                    d1.c73
            END
        )                                       AS c50,
        SUM(d1.c68 + d1.c72 -(d1.c69 + d1.c73)) AS c51,
        SUM(d1.c68 + d1.c72)                    AS c52,
        SUM(d1.c69 + d1.c73)                    AS c53,
        SUM(d1.c68 + d1.c76 -(d1.c69 + d1.c77)) AS c54,
        SUM(d1.c68 + d1.c76)                    AS c55,
        SUM(d1.c69 + d1.c77)                    AS c56,
        SUM(d1.c72 - d1.c73)                    AS c57,
        SUM(d1.c72)                             AS c58,
        SUM(d1.c73)                             AS c59,
        d1.c60                                  AS c60,
        d1.c61                                  AS c61,
        d1.c62                                  AS c62,
        d1.c63                                  AS c63
    FROM
        sawith0 d1
    GROUP BY
        d1.c25,
        d1.c26,
        d1.c27,
        d1.c28,
        d1.c29,
        d1.c30,
        d1.c31,
        d1.c32,
        d1.c33,
        d1.c34,
        d1.c35,
        d1.c37,
        d1.c38,
        d1.c39,
        d1.c41,
        d1.c60,
        d1.c61,
        d1.c62,
        d1.c63
), sawith2 AS (
    SELECT
        SUM(d1.c7)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c7,
        SUM(d1.c8)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c8,
        SUM(d1.c9)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c9,
        SUM(d1.c19)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c19,
        SUM(d1.c20)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c20,
        SUM(d1.c21)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28)         AS c21,
        d1.c25                              AS c25,
        d1.c26                              AS c26,
        d1.c27                              AS c27,
        d1.c28                              AS c28,
        d1.c29                              AS c29,
        d1.c30                              AS c30,
        d1.c31                              AS c31,
        d1.c32                              AS c32,
        d1.c33                              AS c33,
        d1.c34                              AS c34,
        d1.c35                              AS c35,
        d1.c37                              AS c37,
        d1.c38                              AS c38,
        d1.c39                              AS c39,
        d1.c41                              AS c41,
        SUM(d1.c42)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c42,
        SUM(d1.c43)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c43,
        SUM(d1.c44)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c44,
        SUM(d1.c45)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c45,
        SUM(d1.c46)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c46,
        SUM(d1.c47)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c47,
        SUM(d1.c48)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c48,
        SUM(d1.c49)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c49,
        SUM(d1.c50)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c50,
        SUM(d1.c51)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c51,
        SUM(d1.c52)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c52,
        SUM(d1.c53)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c53,
        SUM(d1.c54)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c54,
        SUM(d1.c55)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c55,
        SUM(d1.c56)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c56,
        SUM(d1.c57)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c57,
        SUM(d1.c58)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c58,
        SUM(d1.c59)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63,
                          d1.c37, d1.c41, d1.c27, d1.c28) AS c59,
        d1.c60                              AS c60,
        d1.c61                              AS c61,
        d1.c62                              AS c62,
        d1.c63                              AS c63
    FROM
        sawith1 d1
), sawith3 AS (
    SELECT
        LAST_VALUE(d1.c42 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c1,
        LAST_VALUE(d1.c43 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c2,
        LAST_VALUE(d1.c44 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c3,
        LAST_VALUE(d1.c45 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c4,
        LAST_VALUE(d1.c46 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c5,
        LAST_VALUE(d1.c47 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c6,
        d1.c7  AS c7,
        d1.c8  AS c8,
        d1.c9  AS c9,
        FIRST_VALUE(d1.c48 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c10,
        FIRST_VALUE(d1.c49 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c11,
        FIRST_VALUE(d1.c50 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c12,
        LAST_VALUE(d1.c51 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c13,
        LAST_VALUE(d1.c52 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c14,
        LAST_VALUE(d1.c53 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c15,
        LAST_VALUE(d1.c54 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c16,
        LAST_VALUE(d1.c55 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c17,
        LAST_VALUE(d1.c56 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS FIRST, d1.c33 NULLS FIRST, d1.c38 NULLS FIRST,
                d1.c30 NULLS FIRST, d1.c29 NULLS FIRST, d1.c39 NULLS FIRST, d1.c34 NULLS FIRST, d1.c35 NULLS FIRST,
                d1.c32 NULLS FIRST, d1.c37 NULLS FIRST, d1.c41 NULLS FIRST, d1.c27 NULLS FIRST, d1.c28 NULLS FIRST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS FIRST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c18,
        d1.c19 AS c19,
        d1.c20 AS c20,
        d1.c21 AS c21,
        FIRST_VALUE(d1.c57 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c22,
        FIRST_VALUE(d1.c58 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c23,
        FIRST_VALUE(d1.c59 IGNORE NULLS)
        OVER(PARTITION BY d1.c31, d1.c33, d1.c38, d1.c30, d1.c29,
                          d1.c39, d1.c34, d1.c35, d1.c32, d1.c37,
                          d1.c41, d1.c27, d1.c28
            ORDER BY
                d1.c31 NULLS LAST, d1.c33 NULLS LAST, d1.c38 NULLS LAST,
                d1.c30 NULLS LAST, d1.c29 NULLS LAST, d1.c39 NULLS LAST, d1.c34 NULLS LAST, d1.c35 NULLS LAST,
                d1.c32 NULLS LAST, d1.c37 NULLS LAST, d1.c41 NULLS LAST, d1.c27 NULLS LAST, d1.c28 NULLS LAST,
                d1.c60 * 10000 + d1.c61 * 100000000 + d1.c62 * 100 + d1.c63 NULLS LAST
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )      AS c24,
        d1.c25 AS c25,
        d1.c26 AS c26,
        d1.c27 AS c27,
        d1.c28 AS c28,
        d1.c29 AS c29,
        d1.c30 AS c30,
        d1.c31 AS c31,
        d1.c32 AS c32,
        d1.c33 AS c33,
        d1.c34 AS c34,
        d1.c35 AS c35,
        d1.c37 AS c37,
        d1.c38 AS c38,
        d1.c39 AS c39,
        d1.c41 AS c41
    FROM
        sawith2 d1
), sawith4 AS (
    SELECT
        t5533396.c389230568 AS c1,
        t5533396.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.MEANING             AS c389230568,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID             AS c497682495
            FROM
             CES_ERP.FREQ_FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE  = 'BATCH_TYPE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE  = 'US' ) ) )
        ) t5533396
), sawith5 AS (
    SELECT
        d1.c1  AS c1,
        d1.c2  AS c2,
        d1.c3  AS c3,
        d1.c4  AS c4,
        d1.c5  AS c5,
        d1.c6  AS c6,
        d1.c7  AS c7,
        d1.c8  AS c8,
        d1.c9  AS c9,
        d1.c10 AS c10,
        d1.c11 AS c11,
        d1.c12 AS c12,
        d1.c13 AS c13,
        d1.c14 AS c14,
        d1.c15 AS c15,
        d1.c16 AS c16,
        d1.c17 AS c17,
        d1.c18 AS c18,
        d1.c19 AS c19,
        d1.c20 AS c20,
        d1.c21 AS c21,
        d1.c22 AS c22,
        d1.c23 AS c23,
        d1.c24 AS c24,
        d1.c25 AS c25,
        d1.c26 AS c26,
        d1.c27 AS c27,
        d1.c28 AS c28,
        d1.c29 AS c29,
        d1.c30 AS c30,
        d1.c31 AS c31,
        d1.c32 AS c32,
        d1.c33 AS c33,
        d1.c34 AS c34,
        d1.c35 AS c35,
        d2.c1  AS c36,
        d1.c41 AS c37,
        d1.c37 AS c38,
        d1.c38 AS c39,
        d1.c39 AS c40
    FROM
        sawith3 d1
        LEFT OUTER JOIN sawith4 d2 ON d1.c41 = d2.c2
), sawith6 AS (
    SELECT
        d1.c1  AS c1,
        d1.c2  AS c2,
        d1.c3  AS c3,
        d1.c4  AS c4,
        d1.c5  AS c5,
        d1.c6  AS c6,
        d1.c7  AS c7,
        d1.c8  AS c8,
        d1.c9  AS c9,
        d1.c10 AS c10,
        d1.c11 AS c11,
        d1.c12 AS c12,
        d1.c13 AS c13,
        d1.c14 AS c14,
        d1.c15 AS c15,
        d1.c16 AS c16,
        d1.c17 AS c17,
        d1.c18 AS c18,
        d1.c19 AS c19,
        d1.c20 AS c20,
        d1.c21 AS c21,
        d1.c22 AS c22,
        d1.c23 AS c23,
        d1.c24 AS c24,
        d1.c25 AS c25,
        d1.c26 AS c26,
        d1.c27 AS c27,
        d1.c28 AS c28,
        d1.c29 AS c29,
        d1.c30 AS c30,
        d1.c31 AS c31,
        d1.c32 AS c32,
        d1.c33 AS c33,
        d1.c34 AS c34,
        d1.c35 AS c35,
        d1.c36 AS c36,
        d1.c37 AS c37,
        d1.c38 AS c38,
        d1.c39 AS c39,
        d1.c40 AS c40
    FROM
        (
            SELECT
                d901.c25                AS c1,
                d901.c26                AS c2,
                d901.c27                AS c3,
                d901.c28                AS c4,
                d901.c29                AS c5,
                d901.c30                AS c6,
                d901.c31                AS c7,
                d901.c32                AS c8,
                d901.c33                AS c9,
                d901.c34                AS c10,
                d901.c35                AS c11,
                nvl(d901.c36, d901.c37) AS c12,
                d901.c24                AS c13,
                d901.c23                AS c14,
                d901.c22                AS c15,
                d901.c21                AS c16,
                d901.c20                AS c17,
                d901.c19                AS c18,
                d901.c18                AS c19,
                d901.c17                AS c20,
                d901.c16                AS c21,
                d901.c15                AS c22,
                d901.c14                AS c23,
                d901.c13                AS c24,
                d901.c12                AS c25,
                d901.c11                AS c26,
                d901.c10                AS c27,
                d901.c9                 AS c28,
                d901.c8                 AS c29,
                d901.c7                 AS c30,
                d901.c6                 AS c31,
                d901.c5                 AS c32,
                d901.c4                 AS c33,
                d901.c3                 AS c34,
                d901.c2                 AS c35,
                d901.c1                 AS c36,
                d901.c38                AS c37,
                d901.c39                AS c38,
                d901.c40                AS c39,
                d901.c37                AS c40,
                ROW_NUMBER()
                OVER(PARTITION BY d901.c25, d901.c26, d901.c27, d901.c28, d901.c29,
                                  d901.c30, d901.c31, d901.c32, d901.c33, d901.c34,
                                  d901.c35, d901.c37, d901.c38, d901.c39, d901.c40
                     ORDER BY
                         d901.c25 ASC,
                         d901.c26 ASC, d901.c27 ASC, d901.c28 ASC, d901.c29 ASC, d901.c30 ASC,
                         d901.c31 ASC, d901.c32 ASC, d901.c33 ASC, d901.c34 ASC, d901.c35 ASC,
                         d901.c37 ASC, d901.c38 ASC, d901.c39 ASC, d901.c40 ASC
                )                       AS c41
            FROM
                sawith5 d901
        ) d1
    WHERE
        ( d1.c41 = 1 )
)
SELECT
    d1.c1  AS Concatenated_Segments,
    d1.c2  AS ledger_name,
    d1.c3  AS Currency_code,
    d1.c4  AS Period_Name,
    d1.c5  AS Entity_Code,
    d1.c6  AS Cost_Center_Code,
    d1.c7  AS Account_Code,
    d1.c8  AS Intercompany_Code,
    d1.c9  AS Material_Type_Code,
    d1.c10 AS Scheme_Code,
    d1.c11 AS Project_Code,
    d1.c12 AS Balance_Type,
    d1.c13 AS Base_begin_balance_cr,
    d1.c14 AS Base_begin_balance_dr,
    d1.c15 AS Base_begin_balance,
    d1.c16 AS Base_period_net_activity_cr,
    d1.c17 AS Base_period_net_activity_dr,
    d1.c18 AS Base_period_net_activity,
    d1.c19 AS base_quarter_to_date_cr,
    d1.c20 AS base_quarter_to_date_dr,
    d1.c21 AS base_quarter_to_date,
    d1.c22 AS base_year_to_date_cr,
    d1.c23 AS base_year_to_date_dr,
    d1.c24 AS base_year_to_date,
    d1.c25 AS Entered_beginning_balance_cr,
    d1.c26 AS Entered_beginning_balance_dr,
    d1.c27 AS Entered_beginning_balance,
    d1.c28 AS Entered_period_net_activity_cr,
    d1.c29 AS Entered_period_net_activity_dr,
    d1.c30 AS Entered_period_net_activity,
    d1.c31 AS Entered_quarter_to_date_cr,
    d1.c32 AS Entered_quarter_to_date_dr,
    d1.c33 AS Entered_quarter_to_date,
    d1.c34 AS Entered_year_to_date_cr,
    d1.c35 AS Entered_year_to_date_dr,
    d1.c36 AS Entered_year_to_date ,
    d1.c37 AS last_update_date
FROM
    (
        SELECT
            d1.c1  AS c1,
            d1.c2  AS c2,
            d1.c3  AS c3,
            d1.c4  AS c4,
            d1.c5  AS c5,
            d1.c6  AS c6,
            d1.c7  AS c7,
            d1.c8  AS c8,
            d1.c9  AS c9,
            d1.c10 AS c10,
            d1.c11 AS c11,
            d1.c12 AS c12,
            d1.c13 AS c13,
            d1.c14 AS c14,
            d1.c15 AS c15,
            d1.c16 AS c16,
            d1.c17 AS c17,
            d1.c18 AS c18,
            d1.c19 AS c19,
            d1.c20 AS c20,
            d1.c21 AS c21,
            d1.c22 AS c22,
            d1.c23 AS c23,
            d1.c24 AS c24,
            d1.c25 AS c25,
            d1.c26 AS c26,
            d1.c27 AS c27,
            d1.c28 AS c28,
            d1.c29 AS c29,
            d1.c30 AS c30,
            d1.c31 AS c31,
            d1.c32 AS c32,
            d1.c33 AS c33,
            d1.c34 AS c34,
            d1.c35 AS c35,
            d1.c36 AS c36,
            d1.c37 AS c37
        FROM
            sawith6 d1
    ) d1
ORDER BY d1.c1, d1.c2, d1.c4