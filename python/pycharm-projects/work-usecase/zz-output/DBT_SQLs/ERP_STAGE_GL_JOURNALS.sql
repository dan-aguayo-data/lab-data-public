{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'ID',
        post_hook=[
         "UPDATE COMMON_{{target.name.upper()}}.ERP.CONTROL_TIME_INCREMENTAL_LOAD
            SET LAST_UPDATED_TIMESTAMP = CAST(CONVERT_TIMEZONE('Australia/Sydney', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
            WHERE TABLE_NAME = '{{( model.name | replace('ERP_STAGE_',''))}}'"
                  ]
    )
}}


WITH sawith0 AS (
    SELECT
        t5824690.c233929166 AS c6,  -- v470773127   ENTITY_CODE   --> gl_balancing_  
        t5824690.c31199523  AS c7,  -- v343808451  COST_CENTER_CODE   -->   fa_cost_ctr_
        t5824690.c7207427   AS c8, -- v435856572   ACCOUNT_CODE  -->  gl_account_
        t5824690.c433723576 AS c9,   -- v34792864   INTERCOMPANY_CODE   -->  gl_intercompany_ 
        t5824690.c417910867 AS c10,  -- v412550833  MATERIAL_TYPE_CODE  -->  gl_mat_type_  
        t5824690.c276323548 AS c11,   -- v278434713  SCHEME_CODE   -->   gl_scheme_  
        t5824690.c420124855 AS c12,  -- v344303243   PROJECT_CODE   --> gl_spare_  
        t5824690.c180575348 AS c13,
        t5824690.c314602715 AS c14,
        t5824690.c245980176 AS c15,
        t5824690.c491712020 AS c17,
        t5824690.c66811258  AS c18,
        t5824690.c447332746 AS c19,
        t5824690.c153935039 AS c20,
        t5824690.c313162680 AS c21,
        t5824690.c286802066 AS c22,
        t5824690.c216620139 AS c23,
        t5824690.c418094036 AS c24,
        t5824690.c383269766 AS c26,
        t5824690.c21832610  AS c27,
        t5824690.c217737072 AS c28,
        t5824690.c357783127 AS c29,
        t5824690.c48294840  AS c30,
        t5824690.c80292603  AS c31,
        t5824690.c50963819  AS c32,
        t5824690.c212206954 AS c33,
        t5824690.c336050406 AS c35,
        t5824690.c10223035  AS c36,
        t5824690.c515194955 AS c37,
        t5824690.c373114315 AS c38,
        t5824690.c45898248  AS c40,
        t5824690.c214435915 AS c42,
        t5824690.c292967897 AS c43,
        t5824690.c151241961 AS c45,
        t5824690.c338098357 AS c46,
        t5824690.c317218077 AS c48,
        t5824690.c175996246 AS c49,
        t5824690.c210356818 AS c51,
        t5824690.c15439104  AS c52,
        t5824690.c311905569 AS c53,
        t5824690.c41736306  AS c54,
        t5824690.c81718684  AS c55,
        t5824690.c264948699 AS c58,
        t5824690.c459156460 AS c61,
        t5824690.c148776435 AS c63,
        t5824690.c273274315 AS c66,
        t5824690.c135733684 AS c67,
        t5824690.c158495236 AS c68,
        t5824690.c71951555  AS c69,
        t5824690.c475585834 AS c70,
        t5824690.c291720175 AS c71,
        t5824690.INCREMENTAL_LATEST_DATE,
        t5824690.ID
    FROM
        (
            SELECT
                v208318186.gl_balancing_           AS c233929166,  --gl_balancing_  v470773127.value
                v208318186.fa_cost_ctr_            AS c31199523,  --fa_cost_ctr_   v343808451.value
                v208318186.gl_account_             AS c7207427,  --gl_account_      v435856572.value 
                v208318186.gl_intercompany_         AS c433723576,  --gl_intercompany_ 	_v34792864.value  
                v208318186.gl_mat_type_            AS c417910867,  --gl_mat_type_	v412550833.value
                v208318186.gl_scheme_              AS c276323548, -- gl_scheme_	v278434713.value
                v208318186.gl_spare_               AS c420124855, -- gl_spare_	v344303243.value 
                v208318186.concat_values              AS c180575348,	
                v204948627.posted_date1               AS c314602715,
                v204948627.effective_date             AS c245980176,
                v204948627.currency_conversion_date   AS c491712020,
                v204948627.funds_status_code20        AS c66811258,
                v204948627.period_name401             AS c447332746,
                v376774072.user_je_category_name_lang AS c153935039,
                v204948627.created_by1                AS c313162680,
                v204948627.display_name03             AS c286802066,
                v204948627.date_created               AS c216620139,
                v204948627.description1               AS c418094036,
                v204948627.last_updated_by2           AS c383269766,
                v204948627.display_name04             AS c21832610,
                v204948627.last_update_date1          AS c217737072,
                v204948627.description                AS c357783127,
                v204948627.je_line_num                AS c48294840,
                v204948627.le_name1                   AS c80292603,
                v399845456.lang_description           AS c50963819,
                v399845456.lang_user_je_source_name   AS c212206954,
                v498378102.name1050                   AS c336050406,
                v498378102.name496                    AS c10223035,
                v457566647.supplier_nr_name_          AS c515194955,
                trim(v204948627.name)                 AS c373114315,
                v204948627.name1                      AS c45898248,
                v498378102.currency_code289           AS c214435915,
                v204948627.currency_code              AS c292967897,
                v204948627.user_conversion_type1      AS c151241961,
                v204948627.currency_conversion_rate   AS c338098357,
                v376774072.je_category_name_lang      AS c317218077,
                v399845456.lang_je_source_name1       AS c175996246,
                v208318186.s_g_0                      AS c210356818,
                v498378102.ledger_id                  AS c15439104,
                v204948627.currency_code              AS c311905569,
                v204948627.je_header_id               AS c41736306,
                v204948627.actual_flag                AS c81718684,
                v204948627.encumbrance_type_id1       AS c264948699,
                v204948627.status1                    AS c459156460,
                v498378102.ledger_category_code       AS c148776435,
                v204948627.posting_status_code        AS c273274315,
                v204948627.stat_amount                AS c135733684,
                v204948627.entered_dr                 AS c158495236,
                v204948627.entered_cr                 AS c71951555,
                v204948627.accounted_dr               AS c475585834,
                v204948627.accounted_cr               AS c291720175,
                v204948627.code_combination_id        AS c209142713,
                v204948627.je_header_id1              AS pka_jrnlhdrjeheaderid0,
                v204948627.je_batch_id1               AS pka_jrnlbatchjebatchid0,
                v204948627.conversion_type2           AS pka_conversiontypelineconvers0,
                v204948627.legal_entity_id1           AS pka_jrnlhdrlegalentityid0,
                v204948627.person_name_id03           AS pka_createdbypersonnameperson0,
                v204948627.effective_start_date03     AS pka_createdbypersonnameeffect0,
                v204948627.effective_end_date03       AS pka_createdbypersonnameeffect1,
                v204948627.person_name_id04           AS pka_lastupdatedbypersonnamepe0,
                v204948627.effective_start_date04     AS pka_lastupdatedbypersonnameef0,
                v204948627.effective_end_date04       AS pka_lastupdatedbypersonnameef1,
                v498378102.structure_instance_id      AS pka_keyflexfieldstructureinst0,
                v399845456.lang_language              AS pka_jrnlsrctranslanglanguage0,
                v376774072.language_lang              AS pka_jrnlcattranslatedlanglang0,
                NVL(v204948627.INCREMENTAL_LATEST_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS INCREMENTAL_LATEST_DATE,
                v204948627.ID

            FROM
                (
                    SELECT 
                        xleentityprofiles.LEGAL_ENTITY_NAME                                                                      AS le_name1,
                        xleentityprofiles.LEGAL_ENTITY_LEGAL_ENTITY_ID                                                           AS legal_entity_id1,
                        jrnlline.GL_JE_LINES_ACCOUNTED_CR as accounted_cr,
                        jrnlline.GL_JE_LINES_ACCOUNTED_DR as accounted_dr,
                        jrnlline.GL_JE_LINES_CODE_COMBINATION_ID as code_combination_id,
                        jrnlline.GL_JE_LINES_CURRENCY_CODE as currency_code,
                        jrnlline.GL_JE_LINES_CURRENCY_CONVERSION_DATE as currency_conversion_date,
                        jrnlline.GL_JE_LINES_CURRENCY_CONVERSION_RATE as currency_conversion_rate,
                        jrnlline.GL_JE_LINES_DESCRIPTION as description,
                        jrnlline.GL_JE_LINES_EFFECTIVE_DATE as effective_date,
                        jrnlline.GL_JE_LINES_ENTERED_CR as entered_cr,
                        jrnlline.GL_JE_LINES_ENTERED_DR as entered_dr,
                        jrnlline.JE_HEADER_ID as je_header_id,
                        jrnlline.JE_LINE_NUM as je_line_num,
                        jrnlline.GL_JE_LINES_LEDGER_ID                                                                          AS ledger_id371,
                        jrnlline.GL_JE_LINES_PERIOD_NAME                                                                        AS period_name401,
                        jrnlline.GL_JE_LINES_STAT_AMOUNT as stat_amount,
                        jrnlhdr.GL_JE_HEADERS_ACTUAL_FLAG as actual_flag,
                        jrnlhdr.GL_JE_HEADERS_CREATED_BY                                                                          AS created_by1,
                        jrnlhdr.GL_JE_HEADERS_DATE_CREATED as date_created,
                        jrnlhdr.GL_JE_HEADERS_DESCRIPTION                                                                         AS description1,
                        jrnlhdr.GL_JE_HEADERS_JE_CATEGORY as je_category,
                        jrnlhdr.JE_HEADER_ID                                                                        AS je_header_id1,  --check if right column
                        jrnlhdr.GL_JE_HEADERS_JE_SOURCE as je_source,
                        jrnlhdr.GL_JE_HEADERS_LAST_UPDATE_DATE                                                                    AS last_update_date1,
                        jrnlhdr.GL_JE_HEADERS_NAME as name,
                        jrnlhdr.GL_JE_HEADERS_STATUS                                                                              AS status1,  --two columns to refer to it
                        jrnlbatch.JOURNAL_BATCH_JE_BATCH_ID                                                                       AS je_batch_id1,
                        jrnlbatch.JOURNAL_BATCH_LAST_UPDATED_BY                                                                   AS last_updated_by2,
                        jrnlbatch.JOURNAL_BATCH_NAME                                                                              AS name1,
                        jrnlbatch.JOURNAL_BATCH_POSTED_DATE                                                                       AS posted_date1,
                        jrnlbatch.JOURNAL_BATCH_STATUS                                                                            AS status2,
                        conversiontypeline.DAILY_CONVERSION_TYPE_CONVERSION_TYPE                                                          AS conversion_type2,
                        conversiontypeline.DAILY_CONVERSION_TYPE_USER_CONVERSION_TYPE                                                     AS user_conversion_type1,
                        ledger.LEDGER_CHART_OF_ACCOUNTS_ID                                                                 AS l_chart_of_accounts_id,
                        ( decode(jrnlbatch.JOURNAL_BATCH_FUNDS_STATUS_CODE, NULL, 'NOT_ATTEMPTED', jrnlbatch.JOURNAL_BATCH_FUNDS_STATUS_CODE) ) AS funds_status_code20,
                        journalencumbrancetype.ENCUMBRANCE_TYPE_ID                                                  AS encumbrance_type_id1,
                        createdbypersonname.PERSON_NAME_PEODISPLAY_NAME                                                            AS display_name03,
                        createdbypersonname.EFFECTIVE_END_DATE                                                      AS effective_end_date03,
                        createdbypersonname.EFFECTIVE_START_DATE                                                    AS effective_start_date03,
                        createdbypersonname.PERSON_NAME_ID                                                          AS person_name_id03,
                        lastupdatedbypersonname.PERSON_NAME_PEODISPLAY_NAME                                                        AS display_name04,
                        lastupdatedbypersonname.EFFECTIVE_END_DATE                                                  AS effective_end_date04,
                        lastupdatedbypersonname.EFFECTIVE_START_DATE                                                AS effective_start_date04,
                        lastupdatedbypersonname.PERSON_NAME_ID                                                      AS person_name_id04,
                        ( decode(jrnlbatch.JOURNAL_BATCH_STATUS, 'u', 'U', jrnlbatch.JOURNAL_BATCH_STATUS) )                                    AS posting_status_code,
                        GREATEST(NVL(ledger.LEDGER_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')), NVL(jrnlhdr.GL_JE_HEADERS_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) AS INCREMENTAL_LATEST_DATE,
                        CONCAT(jrnlhdr.je_header_id,'-',jrnlline.je_line_num) AS ID   
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_LINE_EXTRACT_PVO               jrnlline,   --gl_je_lines
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_HEADER_EXTRACT_PVO             jrnlhdr,   --gl_je_headers
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_BATCH_EXTRACT_PVO             jrnlbatch,   --gl_je_batches
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.DAILY_CONVERSION_TYPE_EXTRACT_PVO conversiontypeline,  --gl_daily_conversion_types
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO                ledger,   -- gl_ledgers
                        CES_ERP.FSCM_PROD_FINGLJRNLENTRIES.JOURNAL_ENCUMBRANCE_PVO   journalencumbrancetype,  -- gl_encumbrance_types_vl  --this is a view that form from table b and tl, using Envumberance PVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLEBICCEXTRACT.LEGAL_ENTITY_EXTRACT_PVO       xleentityprofiles,  -- xle_entity_profiles
                        (SELECT USER_PEOPERSON_ID, USER_PEOUSERNAME, USER_HISTORY_PEOCREATION_DATE, USER_HISTORY_PEOLAST_UPDATE_DATE,USER_HISTORY_PEOUSER_HISTORY_ID, USER_PEOACTIVE_FLAG  FROM CES_ERP.FSCM_PROD_USER.USER_PVO 
QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_PEOUSERNAME ORDER BY USER_HISTORY_PEOCREATION_DATE DESC) = 1 )                 createdbyuser,  -- per_users      
                        (SELECT USER_PEOPERSON_ID, USER_PEOUSERNAME, USER_HISTORY_PEOCREATION_DATE, USER_HISTORY_PEOLAST_UPDATE_DATE,USER_HISTORY_PEOUSER_HISTORY_ID, USER_PEOACTIVE_FLAG  FROM CES_ERP.FSCM_PROD_USER.USER_PVO 
QUALIFY ROW_NUMBER() OVER (PARTITION BY USER_PEOUSERNAME ORDER BY USER_HISTORY_PEOCREATION_DATE DESC) = 1 )                 lastupdatedbyuser,   -- per_use
                        CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL      createdbypersonname,  -- per_person_names_f_v
                        CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL      lastupdatedbypersonname   -- per_person_names_f_v
                    WHERE
                        ( jrnlline.JE_HEADER_ID = jrnlhdr.JE_HEADER_ID
                          AND jrnlhdr.GL_JE_HEADERS_JE_BATCH_ID = jrnlbatch.JOURNAL_BATCH_JE_BATCH_ID
                          AND jrnlline.GL_JE_LINES_CURRENCY_CONVERSION_TYPE = conversiontypeline.DAILY_CONVERSION_TYPE_CONVERSION_TYPE (+)
                          AND jrnlline.GL_JE_LINES_LEDGER_ID = ledger.LEDGER_LEDGER_ID (+)
                          AND jrnlhdr.GL_JE_HEADERS_ENCUMBRANCE_TYPE_ID = journalencumbrancetype.ENCUMBRANCE_TYPE_ID (+)
                          AND jrnlhdr.GL_JE_HEADERS_LEGAL_ENTITY_ID = xleentityprofiles.LEGAL_ENTITY_LEGAL_ENTITY_ID (+)
                          AND jrnlhdr.GL_JE_HEADERS_CREATED_BY = createdbyuser.USER_PEOUSERNAME (+)
                          AND ( 'Y' ) = createdbyuser.USER_PEOACTIVE_FLAG (+)
                          AND jrnlhdr.GL_JE_HEADERS_LAST_UPDATED_BY = lastupdatedbyuser.USER_PEOUSERNAME (+)
                          AND ( 'Y' ) = lastupdatedbyuser.USER_PEOACTIVE_FLAG (+)
                          AND createdbyuser.USER_PEOPERSON_ID = createdbypersonname.PERSON_NAME_PEOPERSON_ID (+)
                          AND lastupdatedbyuser.USER_PEOPERSON_ID = lastupdatedbypersonname.PERSON_NAME_PEOPERSON_ID (+)
                          AND ( DATE(SYSDATE()) BETWEEN createdbypersonname.EFFECTIVE_START_DATE (+) AND createdbypersonname.EFFECTIVE_END_DATE (+) )
                          AND ( DATE(SYSDATE()) BETWEEN lastupdatedbypersonname.EFFECTIVE_START_DATE (+) AND lastupdatedbypersonname.
                          EFFECTIVE_END_DATE (+) ) )
                --        AND ( ( jrnlline.GL_JE_LINES_LEDGER_ID IN ( 300000026463625, 300000035198785, 300000126421633,300000026463631, 300000001398016, 300000001407023,300000001410020, 300000001410050, 300000094032925 ) ) )
                ) v204948627,
                (
                    SELECT /*+ qb_name(LedgerPVO) */
                        ledger.LEDGER_CURRENCY_CODE              AS currency_code289,
                        ledger.LEDGER_LEDGER_CATEGORY_CODE as ledger_category_code,
                        ledger.LEDGER_LEDGER_ID as ledger_id,
                        ledger.LEDGER_NAME                       AS name496,
                        keyflexfieldstructureinstanc.KEY_FLEXFIELD_STRUCTURE_INSTANC_NAME AS name1050,
                        keyflexfieldstructureinstanc.KEY_FLEXFIELD_STRUCTURE_INSTANC_STRUCTURE_INSTANCE_ID as structure_instance_id
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO              ledger,  --gl_ledgers
                        CES_ERP.FSCM_PROD_FINGLLEDGERDEFN.LEDGER_PVO keyflexfieldstructureinstanc  --fnd_kf_str_instances_vl
                    WHERE
                        ( ledger.LEDGER_CHART_OF_ACCOUNTS_ID = keyflexfieldstructureinstanc.KEY_FLEXFIELD_STRUCTURE_INSTANC_STRUCTURE_INSTANCE_NUMBER
                        AND  ledger.LEDGER_LEDGER_ID = keyflexfieldstructureinstanc.LEDGER_ID
                          AND ( 101 ) = keyflexfieldstructureinstanc.KEY_FLEXFIELD_STRUCTURE_INSTANC_APPLICATION_ID
                          AND ( 'GL#' ) = keyflexfieldstructureinstanc.KEY_FLEXFIELD_STRUCTURE_INSTANC_KEY_FLEXFIELD_CODE )
                        AND ( ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) ) )
                ) v498378102,
                ( --*/ONLY HAS US so no filter is needed, original has a filter with db variable*/ 
                    SELECT
                        jrnlsrc.JE_SOURCE_NAME,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_DESCRIPTION         AS lang_description,
                        jrnlsrc.JE_SOURCE_NAME     AS lang_je_source_name1,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_LANGUAGE            AS lang_language,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_JE_SOURCE_NAME AS lang_user_je_source_name
                    FROM
                        CES_ERP.FSCM_PROD_FINGLJRNLSETUPSRC.JOURNAL_SOURCE_BPVO  jrnlsrc  -- gl_je_sources_b  
--                       ,gl_je_sources_tl jrnlsrctranslang   -- gl_je_sources_tl
--                    WHERE
--                            jrnlsrc.JE_SOURCE_NAME = jrnlsrctranslang.je_source_name
--                        AND ( userenv('LANG') ) = jrnlsrctranslang.JRNL_SRC_TRANS_LANG_LANGUAGE
                ) v399845456,
                ( --*/ONLY HAS US so no filter is needed, original has a filter with db variable*/ 
                    SELECT
                        jrnlcat.JE_CATEGORY_NAME as je_category_name,
                        jrnlcat.JE_CATEGORY_NAME      AS je_category_name_lang,
                        jrnlcat.JRNL_CAT_TRANSLATED_LANG_LANGUAGE              AS language_lang,
                        jrnlcat.JRNL_CAT_TRANSLATED_LANG_JE_CATEGORY_NAME AS user_je_category_name_lang
                    FROM
                        CES_ERP.FSCM_PROD_FINGLJRNLSETUPCAT.JOURNAL_CATEGORY_BPVO jrnlcat   
                        ---gl_je_categories_b
--                        ,gl_je_categories_tl jrnlcattranslatedlang   --gl_je_categories_tl
--                    WHERE
--                            jrnlcat.je_category_name = jrnlcattranslatedlang.je_category_name
--                        AND ( userenv('LANG') ) = jrnlcattranslatedlang.language
                ) v376774072,
                (
SELECT 
    biflexfieldeo.CODE_COMBINATION_CODE_COMBINATION_ID AS s_g_0,
    biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID AS s_g_1,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 7037,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_3, NULL)) AS fa_cost_ctr_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Cost Center Scheme Ledger', 18063, 'Cost Center Scheme VIC Ledger',
            23063, 'Cost Center Scheme VIC NOP Ledger', 3001, 'Cost Center Service Co Ledger', 7037,
            'Cost Center WARRRL WA Ledger', NULL)) AS fa_cost_ctr_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 7037,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_4, NULL)) AS gl_account_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Account Scheme Ledger', 18063, 'Account Scheme VIC Ledger',
            23063, 'Account Scheme VIC NOP Ledger', 3001, 'Account Service Co Ledger', 7037,
            'Account WARRRL WA Ledger', NULL)) AS gl_account_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 7037,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_1, NULL)) AS gl_balancing_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 18063, 'Company Scheme VIC Ledger',
            23063, 'Company Scheme VIC NOP Ledger', 3001, 'Company Service Co Ledger', 7037,
            'Company WARRRL WA Ledger', NULL)) AS gl_balancing_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 7037,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_5, NULL)) AS gl_intercompany_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 18063, 'Company Scheme VIC Ledger',
            23063, 'Company Scheme VIC NOP Ledger', 3001, 'Company Service Co Ledger', 7037,
            'Company WARRRL WA Ledger', NULL)) AS gl_intercompany_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL)) AS gl_mat_type_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Mat Type Scheme Ledger', 18063, 'Mat Type Scheme VIC Ledger',
            23063, 'Mat Type Scheme VIC NOP Ledger', 7037, 'Mat Type WARRRL WA Ledger', NULL)) AS gl_mat_type_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL)) AS gl_scheme_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, 'Scheme Service Co Ledger', NULL)) AS gl_scheme_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 7037,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL)) AS gl_spare_,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Spare Scheme Ledger', 18063, 'Spare Scheme VIC Ledger',
            23063, 'Spare Scheme VIC NOP Ledger', 3001, 'Spare Service Co Ledger', 7037,
            'Spare WARRRL WA Ledger', NULL)) AS gl_spare_c,
    (decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                               || '-'
                                               || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                               || '-'
                                               || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                               || '-'
                                               || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                               || '-'
                                               || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                               || '-'
                                               || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_6,
            23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                   || '-'
                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                   || '-'
                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                   || '-'
                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                   || '-'
                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                   || '-'
                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                   || '-'
                                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                   || '-'
                                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                   || '-'
                                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                   || '-'
                                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                   || '-'
                                   || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                                         || '-'
                                                                         || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL)) AS concat_values,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_1 AS segment1,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_2 AS segment2,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_3 AS segment3,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_4 AS segment4,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_5 AS segment5,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_6 AS segment6
FROM
    CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO biflexfieldeo   -- gl_code_combinations
                ) v208318186,
                (
                    SELECT /*+ qb_name(JournalLineDFFBIVO) */
                        biflexfieldeo.JE_HEADER_ID AS s_k_5000,
                        biflexfieldeo.JE_LINE_NUM  AS s_k_5001,
                        biflexfieldeo.GL_JE_LINES_ATTRIBUTE_1   AS supplier_nr_name_
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_LINE_EXTRACT_PVO biflexfieldeo  -- gl_je_lines
                ) v457566647
            WHERE
                ( v204948627.ledger_id371 = v498378102.ledger_id
                  AND v204948627.je_source = v399845456.je_source_name
                  AND v204948627.je_category = v376774072.je_category_name
                  AND v204948627.code_combination_id = v208318186.s_g_0
                  AND v204948627.l_chart_of_accounts_id = v208318186.s_g_1
                  AND v204948627.je_header_id = v457566647.s_k_5000
                  AND v204948627.je_line_num = v457566647.s_k_5001 )


 {% if is_incremental() %}

            AND NVL(v204948627.INCREMENTAL_LATEST_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) >= (SELECT dateadd(days, {{var('erp_delta_load_days')}}, LAST_UPDATED_TIMESTAMP) FROM  COMMON_{{target.name.upper()}}.ERP.CONTROL_TIME_INCREMENTAL_LOAD WHERE TABLE_NAME = '{{trimmed_source_name}}' )
               
   {% endif %} 
        ) t5824690
    WHERE
        ( t5824690.c209142713 = t5824690.c210356818 )


), sawith1 AS (
    SELECT
        SUM(d1.c67)                                                          AS c1,
        SUM(d1.c68)                                                          AS c2,
        SUM(d1.c69)                                                          AS c3,
        SUM(d1.c70)                                                          AS c4,
        SUM(d1.c71)                                                          AS c5,
        d1.c6                                                                AS c6,
        d1.c7                                                                AS c7,
        d1.c8                                                                AS c8,
        d1.c9                                                                AS c9,
        d1.c10                                                               AS c10,
        d1.c11                                                               AS c11,
        d1.c12                                                               AS c12,
        d1.c13                                                               AS c13,
        d1.c14                                                               AS c14,
        d1.c15                                                               AS c15,
        d1.c17                                                               AS c17,
        d1.c18                                                               AS c18,
        d1.c19                                                               AS c19,
        d1.c20                                                               AS c20,
        d1.c21                                                               AS c21,
        d1.c22                                                               AS c22,
        d1.c23                                                               AS c23,
        d1.c24                                                               AS c24,
        CAST(d1.c54 AS VARCHAR(100))                                         AS c25,
        d1.c26                                                               AS c26,
        d1.c27                                                               AS c27,
        d1.c28                                                               AS c28,
        d1.c29                                                               AS c29,
        d1.c30                                                               AS c30,
        d1.c31                                                               AS c31,
        d1.c32                                                               AS c32,
        d1.c33                                                               AS c33,
        d1.c35                                                               AS c35,
        d1.c36                                                               AS c36,
        d1.c37                                                               AS c37,
        d1.c38                                                               AS c38,
        d1.c40                                                               AS c40,
        d1.c42                                                               AS c42,
        d1.c43                                                               AS c43,
        d1.c45                                                               AS c45,
        d1.c46                                                               AS c46,
        d1.c48                                                               AS c48,
        d1.c49                                                               AS c49,
        concat(CAST(d1.c54 AS CHARACTER(30)), CAST(d1.c30 AS CHARACTER(30))) AS c50,
        d1.c51                                                               AS c51,
        d1.c52                                                               AS c52,
        d1.c53                                                               AS c53,
        d1.c54                                                               AS c54,
        d1.c55                                                               AS c55,
        d1.c58                                                               AS c58,
        d1.c61                                                               AS c61,
        d1.c63                                                               AS c63,
        d1.c66                                                               AS c66,
        d1.INCREMENTAL_LATEST_DATE,
        d1.ID
    FROM
        sawith0 d1
    GROUP BY
        d1.c6,
        d1.c7,
        d1.c8,
        d1.c9,
        d1.c10,
        d1.c11,
        d1.c12,
        d1.c13,
        d1.c14,
        d1.c15,
        d1.c17,
        d1.c18,
        d1.c19,
        d1.c20,
        d1.c21,
        d1.c22,
        d1.c23,
        d1.c24,
        d1.c26,
        d1.c27,
        d1.c28,
        d1.c29,
        d1.c30,
        d1.c31,
        d1.c32,
        d1.c33,
        d1.c35,
        d1.c36,
        d1.c37,
        d1.c38,
        d1.c40,
        d1.c42,
        d1.c43,
        d1.c45,
        d1.c46,
        d1.c48,
        d1.c49,
        d1.c51,
        d1.c52,
        d1.c53,
        d1.c54,
        d1.c55,
        d1.c58,
        d1.c61,
        d1.c63,
        d1.c66,
        d1.INCREMENTAL_LATEST_DATE,
        d1.ID
), sawith2 AS (
    SELECT
        t5824691.c389230568 AS c1,
        t5824691.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.MEANING             AS c389230568,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID              AS c497682495
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585   --fnd_lookup_values_tl
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'BATCH_TYPE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
                  AND ( ( v72673585.SET_ID  = 0 ) )
                  AND ( ( v72673585.LANGUAGE  = 'US' ) ) )
        ) t5824691
), sawith3 AS (
    SELECT
        t5824692.c218495811 AS c1,
        t5824692.c5543891   AS c2
    FROM
        (  /*REMOVING LANGUAGE FILTER AS IT DOESNT EXIST IN CES_ERP.FSCM_PROD_FINGLJRNLENTRIES.JOURNAL_ENCUMBRANCE_PVO*/
            SELECT
                v127704993.ENCUMBRANCE_TYPE_CODE    AS c218495811,  --there is no name only code
                v127704993.ENCUMBRANCE_TYPE_ID              AS c5543891,
                v127704993.ENCUMBRANCE_TYPE_ID AS pka_journalencumbrancetlpeoen0
--                ,journalencumbrancetlpeo.language            AS pka_journalencumbrancetlpeola0
            FROM
                CES_ERP.FSCM_PROD_FINGLJRNLENTRIES.JOURNAL_ENCUMBRANCE_PVO  v127704993   --gl_encumbrance_types_b
 --               ,gl_encumbrance_types_tl journalencumbrancetlpeo   --gl_encumbrance_types_tl 
 --           WHERE
 --                   v127704993.encumbrance_type_id = journalencumbrancetlpeo.encumbrance_type_id (+)
 --               AND ( userenv('LANG') ) = journalencumbrancetlpeo.language (+)
        ) t5824692
), sawith4 AS (
    SELECT
        t5824693.c185454959 AS c1,
        t5824693.c103205687 AS c2
    FROM
        (
            SELECT
                v248136131.NAME          AS c185454959,
                v248136131.CURRENCY_CODE AS c103205687,
                v248136131.LANGUAGE      AS c315124100
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.CURRENCIES_TLPVO v248136131   --fnd_currencies_tl
            WHERE
                ( ( ( v248136131.LANGUAGE = 'US' ) ) )
        ) t5824693
), sawith5 AS (
    SELECT
        t5824694.c389230568 AS c1,
        t5824694.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.MEANING             AS c389230568,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID              AS c497682495
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585  --fnd_lookup_values_tl
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'MJE_BATCH_STATUS' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t5824694
), sawith6 AS (
    SELECT
        t5824695.c389230568 AS c1,
        t5824695.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.MEANING             AS c389230568,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID              AS c497682495
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585  --fnd_lookup_values_tl
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE= 'GL_ASF_LEDGER_CATEGORY' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID= 101 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE  = 'US' ) ) )
        ) t5824695
), sawith7 AS (
    SELECT
        t5824696.c389230568 AS c1,
        t5824696.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.MEANING             AS c389230568,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID              AS c497682495
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585  --fnd_lookup_values_tl
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'XCC_BC_FUNDS_STATUSES' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 0 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t5824696
), sawith8 AS (
    SELECT
        t5824697.c13174257  AS c1,
        t5824697.c164164071 AS c2
    FROM
        (
            SELECT
                v72673585.DESCRIPTION             AS c13174257,
                v72673585.LOOKUP_CODE         AS c164164071,
                v72673585.LANGUAGE            AS c343259318,
                v72673585.LOOKUP_TYPE         AS c417433804,
                v72673585.VIEW_APPLICATION_ID AS c456636657,
                v72673585.SET_ID              AS c497682495
            FROM
            CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585  --fnd_lookup_values_tl
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'MJE_BATCH_STATUS' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE  = 'US' ) ) )
        ) t5824697
), sawith9 AS (
    SELECT
        d1.c1            AS c1,
        d1.c2            AS c2,
        d1.c3            AS c3,
        d1.c4            AS c4,
        d1.c5            AS c5,
        d1.c6            AS c6,
        d1.c7            AS c7,
        d1.c8            AS c8,
        d1.c9            AS c9,
        d1.c10           AS c10,
        d1.c11           AS c11,
        d1.c12           AS c12,
        d1.c13           AS c13,
        d1.c14           AS c14,
        d1.c15           AS c15,
        d1.c55           AS c16,
        d2.c1            AS c17,
        nvl(d3.c1, NULL) AS c18,
        d1.c17           AS c19,
        d1.c18           AS c20,
        d1.c19           AS c21,
        d1.c20           AS c22,
        d1.c21           AS c23,
        d1.c22           AS c24,
        d1.c23           AS c25,
        d1.c24           AS c26,
        d1.c25           AS c27,
        d1.c26           AS c28,
        d1.c27           AS c29,
        d1.c28           AS c30,
        d1.c29           AS c31,
        d1.c30           AS c32,
        d1.c31           AS c33,
        d1.c32           AS c34,
        d1.c33           AS c35,
        d4.c1            AS c36,
        d1.c53           AS c37,
        d1.c35           AS c38,
        d1.c36           AS c39,
        d1.c37           AS c40,
        d1.c38           AS c41,
        d5.c1            AS c42,
        d1.c61           AS c43,
        d1.c40           AS c44,
        d6.c1            AS c45,
        d1.c63           AS c46,
        d1.c42           AS c47,
        d1.c43           AS c48,
        d7.c1            AS c49,
        d1.c45           AS c50,
        d1.c46           AS c51,
        d8.c1            AS c52,
        d1.c66           AS c53,
        d1.c48           AS c54,
        d1.c49           AS c55,
        d1.c50           AS c56,
        d1.c51           AS c57,
        d1.c52           AS c58,
        d1.c54           AS c59,
        d1.INCREMENTAL_LATEST_DATE,
        d1.ID
    FROM
        (
            (
                (
                    (
                        (
                            (
                                sawith1 d1
                                LEFT OUTER JOIN sawith2 d2 ON d1.c55 = d2.c2
                            )
                            LEFT OUTER JOIN sawith3 d3 ON EQUAL_NULL(c58,d3.c2)
                        )
                        LEFT OUTER JOIN sawith4 d4 ON d1.c53 = d4.c2
                    )
                    LEFT OUTER JOIN sawith5 d5 ON d1.c61 = d5.c2
                )
                LEFT OUTER JOIN sawith6 d6 ON d1.c63 = d6.c2
            )
            LEFT OUTER JOIN sawith7 d7 ON d1.c18 = d7.c2
        )
        LEFT OUTER JOIN sawith8 d8 ON d1.c66 = d8.c2
), sawith10 AS (
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
        d1.c40 AS c40,
        d1.c41 AS c41,
        d1.c42 AS c42,
        d1.c43 AS c43,
        d1.c44 AS c44,
        d1.c45 AS c45,
        d1.c46 AS c46,
        d1.c47 AS c47,
        d1.c48 AS c48,
        d1.c49 AS c49,
        d1.c50 AS c50,
        d1.c51 AS c51,
        d1.c52 AS c52,
        d1.c53 AS c53,
        d1.c54 AS c54,
        d1.c55 AS c55,
        d1.c56 AS c56,
        d1.c57 AS c57,
        d1.INCREMENTAL_LATEST_DATE,
        d1.ID
    FROM
        (
            SELECT
                d901.c6                 AS c1,
                d901.c7                 AS c2,
                d901.c8                 AS c3,
                d901.c9                 AS c4,
                d901.c10                AS c5,
                d901.c11                AS c6,
                d901.c12                AS c7,
                d901.c13                AS c8,
                d901.c14                AS c9,
                d901.c15                AS c10,
                CASE
                    WHEN d901.c16 = 'A' THEN
                        nvl(d901.c17, d901.c16)
                    WHEN d901.c16 = 'E' THEN
                        d901.c18
                END                     AS c11,
                d901.c19                AS c12,
                d901.c20                AS c13,
                d901.c21                AS c14,
                d901.c22                AS c15,
                d901.c23                AS c16,
                d901.c24                AS c17,
                d901.c25                AS c18,
                d901.c26                AS c19,
                d901.c27                AS c20,
                d901.c28                AS c21,
                d901.c29                AS c22,
                d901.c30                AS c23,
                d901.c31                AS c24,
                d901.c32                AS c25,
                d901.c33                AS c26,
                d901.c34                AS c27,
                d901.c35                AS c28,
                nvl(d901.c36, d901.c37) AS c29,
                d901.c38                AS c30,
                d901.c39                AS c31,
                d901.c5                 AS c32,
                d901.c4                 AS c33,
                d901.c3                 AS c34,
                d901.c2                 AS c35,
                d901.c1                 AS c36,
                d901.c40                AS c37,
                d901.c41                AS c38,
                nvl(d901.c42, d901.c43) AS c39,
                d901.c44                AS c40,
                nvl(d901.c45, d901.c46) AS c41,
                d901.c47                AS c42,
                d901.c48                AS c43,
                nvl(d901.c49, d901.c20) AS c44,
                d901.c50                AS c45,
                d901.c51                AS c46,
                nvl(d901.c52, d901.c53) AS c47,
                d901.c54                AS c48,
                d901.c55                AS c49,
                d901.c56                AS c50,
                d901.c57                AS c51,
                d901.c58                AS c52,
                d901.c37                AS c53,
                d901.c59                AS c54,
                d901.c43                AS c55,
                d901.c46                AS c56,
                d901.c53                AS c57,
                d901.INCREMENTAL_LATEST_DATE,
                d901.ID,
                ROW_NUMBER()
                OVER(PARTITION BY d901.c6, d901.c7, d901.c8, d901.c9, d901.c10,
                                  d901.c11, d901.c12, d901.c13, d901.c14, d901.c15,
                                  d901.c19, d901.c20, d901.c21, d901.c22, d901.c23,
                                  d901.c24, d901.c25, d901.c26, d901.c27, d901.c28,
                                  d901.c29, d901.c30, d901.c31, d901.c32, d901.c33,
                                  d901.c34, d901.c35, d901.c37, d901.c38, d901.c39,
                                  d901.c40, d901.c41, d901.c43, d901.c44, d901.c46,
                                  d901.c47, d901.c48, d901.c50, d901.c51, d901.c53,
                                  d901.c54, d901.c55, d901.c56, d901.c57, d901.INCREMENTAL_LATEST_DATE, d901.ID, d901.c58,
                                  d901.c59,
                    CASE
                        WHEN d901.c16 = 'A' THEN
                            nvl(d901.c17, d901.c16)
                        WHEN d901.c16 = 'E' THEN
                            d901.c18
                    END
                     ORDER BY
                         d901.c6 ASC, d901.c7 ASC, d901.c8 ASC, d901.c9 ASC, d901.c10 ASC,
                         d901.c11 ASC, d901.c12 ASC, d901.c13 ASC, d901.c14 ASC, d901.c15 ASC,
                         d901.c19 ASC, d901.c20 ASC, d901.c21 ASC, d901.c22 ASC, d901.c23 ASC,
                         d901.c24 ASC, d901.c25 ASC, d901.c26 ASC, d901.c27 ASC, d901.c28 ASC,
                         d901.c29 ASC, d901.c30 ASC, d901.c31 ASC, d901.c32 ASC, d901.c33 ASC,
                         d901.c34 ASC, d901.c35 ASC, d901.c37 ASC, d901.c38 ASC, d901.c39 ASC,
                         d901.c40 ASC, d901.c41 ASC, d901.c43 ASC, d901.c44 ASC, d901.c46 ASC,
                         d901.c47 ASC, d901.c48 ASC, d901.c50 ASC, d901.c51 ASC, d901.c53 ASC,
                         d901.c54 ASC, d901.c55 ASC, d901.c56 ASC, d901.c57 ASC, d901.c58 ASC, d901.INCREMENTAL_LATEST_DATE ASC, d901.ID ASC,
                         d901.c59 ASC,
                         CASE
                                 WHEN d901.c16 = 'A' THEN
                                     nvl(d901.c17, d901.c16)
                                 WHEN d901.c16 = 'E' THEN
                                     d901.c18
                         END
                         ASC
                )                       AS c58
            FROM
                sawith9 d901
        ) d1
    WHERE
        ( d1.c58 = 1 )
)
SELECT
    d1.ID,
    d1.c1 AS ENTITY_CODE,
    d1.c2 AS COST_CENTER_CODE,
    d1.c3 AS ACCOUNT_CODE,
    d1.c4 AS INTERCOMPANY_CODE,
    d1.c5 AS MATERIAL_TYPE_CODE,
    d1.c6 AS SCHEME_CODE,
    d1.c7 AS PROJECT_CODE,
    d1.c8 AS CONCATENATED_SEGMENTS,
    d1.c9 AS POSTING_DATE,
    d1.c10 AS ACCOUNTING_DATE,
    d1.c11 AS BALANCE_TYPE,
    d1.c12 AS CURRENCY_CONVERSION_DATE,
    d1.c13 AS FUNDS_STATUS,
    d1.c14 AS ACCOUNTING_PERIOD_NAME,
    d1.c15 AS JOURNAL_CATEGORY_NAME,
    d1.c16 AS JOURNAL_CREATED_BY_USERNAME,
    d1.c17 AS JOURNAL_CREATED_BY,
    d1.c18 AS JOURNAL_CREATED_DATE,
    d1.c19 AS JOURNAL_DESC,
    d1.c20 AS JOURNAL_HEADER_ID,
    d1.c21 AS LAST_UPDATED_BY_USERNAME,
    d1.c22 AS LAST_UPDATED_BY,
    to_char(d1.c23,'YYYY-MM-DD HH24:MI:SS') AS LAST_UPDATE_DATE,
    d1.c24 AS JOURNAL_LINE_DESC,
    d1.c25 AS JOURNAL_LINE_NUM,
    d1.c26 AS LEGAL_ENTITY_NAME,
    d1.c27 AS SOURCE_JOURNAL_SOURCE_DESC,
    d1.c28 AS SOURCE_JOURNAL_SOURCE_NAME,
    d1.c29 AS DOCUMENT_CURRENCY_NAME,
    d1.c30 AS CHART_OF_ACCOUNT,
    d1.c31 AS LEDGER_NAME,
    d1.c32 AS AMOUNT_CR,
    d1.c33 AS AMOUNT_DR,
    d1.c34 AS AMOUNT_ENTERED_CR,
    d1.c35 AS AMOUNT_ENTERED_DR,
    d1.c36 AS STATISTICAL_QUANTITY, -- STATISTICAL_AMOUNT
    d1.c37 AS SUPPLIER_NUMBER_NAME,
    d1.c38 AS JOURNAL_NAME,
    d1.c39 AS JOURNAL_STATUS,
    d1.c40 AS JOURNAL_BATCH_NAME,
    d1.c41 AS LEDGER_CATEGORY_NAME,
    d1.c42 AS LEDGER_CURRENCY,
    d1.c43 AS LINE_CURRENCY_CODE,
    d1.c44 AS FUND_STATUS_MEANING,
    d1.c45 AS CONVERSION_RATE_TYPE,
    d1.c46 AS CONVERSION_RATE,
    d1.c47 AS POSTING_STATUS_DESC,
    d1.INCREMENTAL_LATEST_DATE
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
            d1.c37 AS c37,
            d1.c38 AS c38,
            d1.c39 AS c39,
            d1.c40 AS c40,
            d1.c41 AS c41,
            d1.c42 AS c42,
            d1.c43 AS c43,
            d1.c44 AS c44,
            d1.c45 AS c45,
            d1.c46 AS c46,
            d1.c47 AS c47,
            d1.INCREMENTAL_LATEST_DATE,
            d1.ID
        FROM
            sawith10 d1
    ) d1












