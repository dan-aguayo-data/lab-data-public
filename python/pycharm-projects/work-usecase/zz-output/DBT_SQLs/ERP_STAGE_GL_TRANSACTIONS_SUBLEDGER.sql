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
        t6080001.c233929166 AS c6,
        t6080001.c31199523  AS c7,
        t6080001.c7207427   AS c8,
        t6080001.c433723576 AS c9,
        t6080001.c417910867 AS c10,
        t6080001.c276323548 AS c11,
        t6080001.c420124855 AS c12,
        t6080001.c108237864 AS c13,
        t6080001.c256343553 AS c14,
        t6080001.c61858107  AS c15,
        t6080001.c129222644 AS c16,
        t6080001.c167111848 AS c17,
        t6080001.c184047305 AS c18,
        t6080001.c180575348 AS c19,
        t6080001.c160216612 AS c20,
        t6080001.c59463633  AS c21,
        t6080001.c365264516 AS c25,
        t6080001.c251011232 AS c26,
        t6080001.c518020395 AS c27,
        t6080001.c127809081 AS c28,
        t6080001.c137685037 AS c29,
        t6080001.c212309508 AS c30,
        t6080001.c118386160 AS c31,
        t6080001.c49156084  AS c32,
        t6080001.c347627410 AS c33,
        t6080001.c255064400 AS c34,
        t6080001.c244133595 AS c36,
        t6080001.c134746288 AS c37,
        t6080001.c46063845  AS c38,
        t6080001.c447183386 AS c39,
        t6080001.c368182810 AS c40,
        t6080001.c388699098 AS c41,
        t6080001.c102172980 AS c43,
        t6080001.c193419056 AS c44,
        t6080001.c278600876 AS c45,
        t6080001.c115551971 AS c46,
        t6080001.c485614450 AS c47,
        t6080001.c252212243 AS c49,
        t6080001.c23635656  AS c50,
        t6080001.c50963819  AS c51,
        t6080001.c175996246 AS c52,
        t6080001.c212206954 AS c53,
        t6080001.c336050406 AS c55,
        t6080001.c10223035  AS c56,
        t6080001.c11610457  AS c57,
        t6080001.c323161287 AS c58,
        t6080001.c203825585 AS c59,
        t6080001.c513506196 AS c60,
        t6080001.c290837901 AS c61,
        t6080001.c129803501 AS c62,
        t6080001.c113217367 AS c64,
        t6080001.c224725749 AS c65,
        t6080001.c308010007 AS c67,
        t6080001.c433082877 AS c68,
        t6080001.c55576836  AS c69,
        t6080001.c210356818 AS c71,
        t6080001.c15439104  AS c72,
        t6080001.c311905569 AS c73,
        t6080001.c460889912 AS c74,
        t6080001.c219432938 AS c76,
        t6080001.c156833788 AS c78,
        t6080001.c308939070 AS c80,
        t6080001.c194460937 AS c82,
        t6080001.c475767516 AS c85,
        t6080001.c448341863 AS c87,
        t6080001.c95546219  AS c88,
        t6080001.c232764903 AS c89,
        t6080001.c76051773  AS c90,
        t6080001.c475794859 AS c91,
        t6080001.c306331383 AS c92,
        t6080001.c528066610 AS c93,
        t6080001.c266904990 AS c94,
        t6080001.c47395675  AS c95,
        t6080001.ID,
        t6080001.INCREMENTAL_LATEST_DATE
        
    FROM
        (
            SELECT                  
                v208318186.gl_balancing_             AS c233929166,
                v208318186.fa_cost_ctr_              AS c31199523,
                v208318186.gl_account_               AS c7207427,
                v208318186.gl_intercompany_          AS c433723576,
                v208318186.gl_mat_type_              AS c417910867,
                v208318186.gl_scheme_                AS c276323548,
                v208318186.gl_spare_                 AS c420124855,
                v440969301.segment1                  AS c108237864,
                v440969301.invoice_date              AS c256343553,
                v440969301.description               AS c61858107,
                v440969301.invoice_num               AS c129222644,
                v440969301.party_party_name          AS c167111848,
                v440969301.voucher_num               AS c184047305,
                v208318186.concat_values             AS c180575348,
                v169022212.posted_date1              AS c160216612,
                v97717822.accounting_date            AS c59463633,
                v86023658.name                       AS c365264516,
                v97717822.currency_conversion_date   AS c251011232,
                v97717822.description2               AS c518020395,
                v97717822.currency_conversion_rate   AS c127809081,
                v97717822.currency_conversion_type   AS c137685037,
                v97717822.completed_date             AS c212309508,
                v97717822.gl_transfer_date           AS c118386160,
                v97717822.encumbrance_type20         AS c49156084,
                v97717822.event_type_code            AS c347627410,
                v97717822.name1                      AS c255064400,
                v97717822.period_name                AS c244133595,
                v97717822.je_category_name           AS c134746288,
                v97717822.created_by1                AS c46063845,
                v97717822.display_name1              AS c447183386,
                v97717822.creation_date1             AS c368182810,
                v97717822.description1               AS c388699098,
                v97717822.last_updated_by1           AS c102172980,
                v97717822.display_name               AS c193419056,
                v97717822.last_update_date_389       AS c278600876,
                v97717822.description                AS c115551971,
                v97717822.displayed_line_number      AS c485614450,
                v97717822.override_reason            AS c252212243,
                v97717822.transaction_number         AS c23635656,
                v399845456.lang_description          AS c50963819,
                v399845456.lang_je_source_name1      AS c175996246,
                v399845456.lang_user_je_source_name  AS c212206954,
                v498378102.name1050                  AS c336050406,
                v498378102.name496                   AS c10223035,
                v291284714.trx_number                AS c11610457,
                v448314251.segment1                  AS c323161287,
                v448314251.party_name1               AS c203825585,
                v169022212.je_header_id              AS c513506196,
                v169022212.je_line_num               AS c290837901,
                v169022212.name                      AS c129803501,
                v211460917.meaning                   AS c113217367,
                v211460917.name                      AS c224725749,
                v74173908.organization_name          AS c308010007,
                v86023658.organization_id1           AS c433082877,
                v211460917.cust_trx_type_seq_id      AS c55576836,
                v208318186.s_g_0                     AS c210356818,
                v498378102.ledger_id                 AS c15439104,
                v220341495.currency_code             AS c311905569,
                v211460917.type                      AS c460889912,
                v97717822.accounting_entry_type_code AS c219432938,
                v97717822.balance_type_code          AS c156833788,
                v97717822.business_class_code        AS c308939070,
                v97717822.funds_status_code1         AS c194460937,
                v169022212.posting_status_code       AS c475767516,
                v291284714.complete_flag             AS c448341863,
                v97717822.statistical_amount         AS c95546219,
                v97717822.le_name1                   AS c232764903,
                v97717822.ent_name                   AS c76051773,
                v97717822.ae_header_id               AS c475794859,
                v97717822.entered_dr                 AS c306331383,
                v97717822.entered_cr                 AS c528066610,
                v97717822.accounted_dr               AS c266904990,
                v97717822.accounted_cr               AS c47395675,
               -- v327536137.criterion_id              AS c229190129,
                v367001394.gl_sl_link_id             AS c190639067,
                v367001394.gl_sl_link_table          AS c363364264,
                v367001394.je_header_id              AS c306833588,
                v367001394.je_line_num               AS c483234663,
                v97717822.ae_line_num                AS pka_aelinenum0,
                v97717822.ae_header_id1              AS pka_xlahdraeheaderid0,
                v97717822.conversion_type            AS pka_conversiontypexlalineconv0,
                v97717822.entity_id1                 AS pka_transactionentityentityid0,
                v97717822.ent_legal_entity_id        AS pka_legalentlegalentityid0,
                v97717822.application_id7            AS pka_evnttypexlahdrvlapplicati0,
                v97717822.entity_code1               AS pka_evnttypexlahdrvlentitycod0,
                v97717822.event_class_code           AS pka_evnttypexlahdrvleventclas0,
                v97717822.event_type_code1           AS pka_evnttypexlahdrvleventtype0,
                v97717822.event_type_lang            AS pka_evnttypexlahdrvllanguage0,
                v97717822.encumbrance_type_id_717    AS pka_journalencumbrancetypeenc0,
                v97717822.legal_entity_id1           AS pka_xlahdrlegalentityid0,
                v97717822.person_name_id             AS pka_personcreatedbypersonname0,
                v97717822.effective_start_date1      AS pka_personcreatedbyeffectives0,
                v97717822.effective_end_date1        AS pka_personcreatedbyeffectivee0,
                v97717822.person_name_id1            AS pka_personupdatedbypersonname0,
                v97717822.effective_start_date       AS pka_personupdatedbyeffectives0,
                v97717822.effective_end_date         AS pka_personupdatedbyeffectivee0,
                v448314251.vendor_id557              AS pka_vendorid0,
                v448314251.party_id824               AS pka_partypartyid0,
                v498378102.structure_instance_id     AS pka_keyflexfieldstructureinst0,
                v74173908.organization_profile_id    AS pka_organizationprofileid0,
                v74173908.effective_start_date       AS pka_effectivestartdate0,
                v74173908.effective_end_date         AS pka_effectiveenddate0,
                v399845456.lang_language             AS pka_jrnlsrctranslanglanguage0,
                v86023658.effective_start_date651    AS pka_businessunitdatefrom0,
                v86023658.effective_end_date8        AS pka_businessunitdateto0,
                v86023658.organization_id            AS pka_organizationunittranslati0,
                v86023658.effective_start_date       AS pka_organizationunittranslati1,
                v86023658.effective_end_date         AS pka_organizationunittranslati2,
                v86023658.language                   AS pka_organizationunittranslati3,
                v291284714.customer_trx_id           AS pka_customertrxid0,
                v211460917.lookup_type               AS pka_arlookuplookuptype0,
                v211460917.lookup_code               AS pka_arlookuplookupcode0,
--                v470773127.value_id                  AS pka_valueid0,
--                v343808451.value_id                  AS pka_valueid1,
--                v278434713.value_id                  AS pka_valueid2,
--                v344303243.value_id                  AS pka_valueid3,
--                v34792864.value_id                   AS pka_valueid4,
--                v412550833.value_id                  AS pka_valueid5,
--                v435856572.value_id                  AS pka_valueid6,
                v169022212.je_header_id1             AS pka_jrnlhdrjeheaderid0,
                v169022212.je_batch_id1              AS pka_jrnlbatchjebatchid0,
                v440969301.invoice_id                AS pka_invoiceid0,
                v440969301.po_header_id1             AS pka_purchaseorderpoheaderid0,
                v440969301.party_party_id            AS pka_partypartyid1,
                v97717822.ID,
                v97717822.JOURNAL_LATEST_UPDATE_DATE,
                v291284714.TRX_LAST_UPDATE_DATE,
                v440969301.INVOICES_LAST_UPDATE_DATE,
                GREATEST(NVL(v440969301.INVOICES_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v291284714.TRX_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v97717822.JOURNAL_LATEST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) ) AS INCREMENTAL_LATEST_DATE

            FROM
                (
                    SELECT /*+ qb_name(JournalEntryLinePVO) */
                        personupdatedby.PERSON_NAME_ID             AS person_name_id1,
                        personupdatedby.EFFECTIVE_START_DATE as effective_start_date,
                        personupdatedby.EFFECTIVE_END_DATE as effective_end_date,
                        personupdatedby.PERSON_NAME_PEODISPLAY_NAME as display_name,
                        personcreatedby.PERSON_NAME_PEODISPLAY_NAME               AS display_name1,
                        personcreatedby.EFFECTIVE_START_DATE       AS effective_start_date1,
                        personcreatedby.EFFECTIVE_END_DATE         AS effective_end_date1,
                        personcreatedby.PERSON_NAME_ID as person_name_id,
                        xleentityprofiles.LEGAL_ENTITY_NAME                     AS le_name1,
                        xleentityprofiles.LEGAL_ENTITY_LEGAL_ENTITY_ID          AS legal_entity_id1,
                        xlalines.JOURNAL_ENTRY_LINE_ACCOUNTED_CR as accounted_cr,
                        xlalines.JOURNAL_ENTRY_LINE_ACCOUNTED_DR as accounted_dr,
                        xlalines.JOURNAL_ENTRY_LINE_ACCOUNTING_DATE as accounting_date,
                        xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID as ae_header_id,
                        xlalines.JOURNAL_ENTRY_LINE_AE_LINE_NUM as ae_line_num,
                        xlalines.JOURNAL_ENTRY_LINE_BUSINESS_CLASS_CODE as business_class_code,
                        xlalines.JOURNAL_ENTRY_LINE_CODE_COMBINATION_ID as code_combination_id,
                        xlalines.JOURNAL_ENTRY_LINE_CURRENCY_CODE as currency_code,
                        xlalines.JOURNAL_ENTRY_LINE_CURRENCY_CONVERSION_DATE as currency_conversion_date,
                        xlalines.JOURNAL_ENTRY_LINE_CURRENCY_CONVERSION_RATE as currency_conversion_rate,
                        xlalines.JOURNAL_ENTRY_LINE_CURRENCY_CONVERSION_TYPE as currency_conversion_type,
                        xlalines.JOURNAL_ENTRY_LINE_DESCRIPTION as description,
                        xlalines.JOURNAL_ENTRY_LINE_DISPLAYED_LINE_NUMBER as displayed_line_number,
                        xlalines.JOURNAL_ENTRY_LINE_ENTERED_CR as entered_cr,
                        xlalines.JOURNAL_ENTRY_LINE_ENTERED_DR as entered_dr,
                        xlalines.JOURNAL_ENTRY_LINE_GL_SL_LINK_ID as gl_sl_link_id,
                        xlalines.JOURNAL_ENTRY_LINE_GL_SL_LINK_TABLE as gl_sl_link_table,
                        xlalines.JOURNAL_ENTRY_LINE_LAST_UPDATE_DATE                  AS last_update_date_389,
                        xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID as ledger_id,
                        xlalines.JOURNAL_ENTRY_LINE_PARTY_ID as party_id,
                        xlalines.JOURNAL_ENTRY_LINE_STATISTICAL_AMOUNT as statistical_amount,
                        CONCAT(xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID,'-',xlalines.JOURNAL_ENTRY_LINE_DISPLAYED_LINE_NUMBER) as ID,
                        xlahdr.JOURNAL_ENTRY_HEADER_ACCOUNTING_ENTRY_TYPE_CODE as accounting_entry_type_code,
                        xlahdr.JOURNAL_ENTRY_HEADER_AE_HEADER_ID                        AS ae_header_id1,
                        xlahdr.JOURNAL_ENTRY_HEADER_BALANCE_TYPE_CODE as balance_type_code,
                        xlahdr.JOURNAL_ENTRY_HEADER_COMPLETED_DATE as completed_date,
                        xlahdr.JOURNAL_ENTRY_HEADER_CREATED_BY                          AS created_by1,
                        xlahdr.JOURNAL_ENTRY_HEADER_CREATION_DATE                       AS creation_date1,
                        xlahdr.JOURNAL_ENTRY_HEADER_DESCRIPTION                         AS description1,
                        xlahdr.JOURNAL_ENTRY_HEADER_EVENT_TYPE_CODE as event_type_code,
                        xlahdr.JOURNAL_ENTRY_HEADER_FUNDS_STATUS_CODE                   AS funds_status_code1,
                        xlahdr.JOURNAL_ENTRY_HEADER_GL_TRANSFER_DATE as gl_transfer_date,
                        xlahdr.JOURNAL_ENTRY_HEADER_JE_CATEGORY_NAME as je_category_name,
                        xlahdr.JOURNAL_ENTRY_HEADER_LAST_UPDATED_BY                     AS last_updated_by1,
                        xlahdr.JOURNAL_ENTRY_HEADER_PERIOD_NAME as period_name,
                        subledgerapplnsetup.SUBLEDGER_APPLICATION_SETUP_JE_SOURCE_NAME as je_source_name,
                        conversiontypexlaline.DAILY_CONVERSION_TYPE_CONVERSION_TYPE as conversion_type,
                        conversiontypexlaline.DAILY_CONVERSION_TYPE_DESCRIPTION          AS description2,
                        transactionentity.TRANSACTION_ENTITY_CODE as entity_code,
                        transactionentity.TRANSACTION_ENTITY_ID                AS entity_id1,
                        transactionentity.TRANSACTION_SECURITY_ID_INT_1 as security_id_int_1,
                        transactionentity.TRANSACTION_SOURCE_APPLICATION_ID as source_application_id,
                        transactionentity.TRANSACTION_SOURCE_ID_INT_1 as source_id_int_1,
                        transactionentity.TRANSACTION_TRANSACTION_NUMBER as transaction_number,
                        (
                            CASE
                                WHEN transactionentity.TRANSACTION_SOURCE_APPLICATION_ID  = 200
                                     AND transactionentity.TRANSACTION_ENTITY_CODE = 'AP_INVOICES' THEN
                                    transactionentity.TRANSACTION_SOURCE_ID_INT_1
                                ELSE
                                    NULL
                            END
                        )                                          AS ap_invoice_id,
                        (
                            CASE
                                WHEN transactionentity.TRANSACTION_SOURCE_APPLICATION_ID  = 222
                                     AND transactionentity.TRANSACTION_ENTITY_CODE = 'TRANSACTIONS' THEN
                                    transactionentity.TRANSACTION_SOURCE_ID_INT_1
                                ELSE
                                    NULL
                            END
                        )                                          AS ar_customer_trx_id,
                        ledgers.LEDGER_CHART_OF_ACCOUNTS_ID as chart_of_accounts_id,
                        legalent.LEGAL_ENTITY_LEGAL_ENTITY_ID                   AS ent_legal_entity_id,
                        legalent.LEGAL_ENTITY_NAME                              AS ent_name,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_NAME                      AS name1,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_APPLICATION_ID            AS application_id7,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_EVENT_TYPE_CODE           AS event_type_code1,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_ENTITY_CODE               AS entity_code1,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_LANGUAGE                  AS event_type_lang,
                        evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_EVENT_CLASS_CODE as event_class_code,
                        custacct.PARTY_ID                         AS party_id1,
                        xlalines.JOURNAL_ENTRY_LINE_OVERRIDE_REASON as override_reason,
                        journalencumbrancetype.ENCUMBRANCE_TYPE    AS encumbrance_type20,
                        journalencumbrancetype.ENCUMBRANCE_TYPE_ID AS encumbrance_type_id_717,
                        GREATEST(NVL(xlahdr.JOURNAL_ENTRY_HEADER_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(ledgers.LEDGER_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) AS JOURNAL_LATEST_UPDATE_DATE

            
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_LINE_EXTRACT_PVO              xlalines, --FscmTopModelAM.FinExtractAM.XlaBiccExtractAM.SubledgerJournalLineExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_HEADER_EXTRACT_PVO            xlahdr, --FscmTopModelAM.FinExtractAM.XlaBiccExtractAM.SubledgerJournalHeaderExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_APPLICATION_EXTRACT_PVO            subledgerapplnsetup, --FscmTopModelAM.FinExtractAM.XlaBiccExtractAM.SubledgerApplicationExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.DAILY_CONVERSION_TYPE_EXTRACT_PVO conversiontypexlaline, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.DailyConversionTypeExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_TRANSACTION_ENTITY_EXTRACT_PVO  transactionentity, --FscmTopModelAM.FinExtractAM.XlaBiccExtractAM.SubledgerJournalTransactionEntityExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO                ledgers, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.LedgerExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLEBICCEXTRACT.LEGAL_ENTITY_EXTRACT_PVO       legalent, --FscmTopModelAM.FinExtractAM.XleBiccExtractAM.LegalEntityExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.EVENT_TYPE_TLEXTRACT_PVO        evnttypexlahdrvl, --FscmTopModelAM.FinExtractAM.XlaBiccExtractAM.EventTypeTLExtractPVO
                        CES_ERP.FSCM_PROD_PARTIESANALYTICS.CUSTOMER_ACCOUNT          custacct, --FscmTopModelAM.PartiesAnalyticsAM.CustomerAccount
                        CES_ERP.FSCM_PROD_FINGLJRNLENTRIES.JOURNAL_ENCUMBRANCE_PVO   journalencumbrancetype, --FscmTopModelAM.FinGlJrnlEntriesAM.JournalEncumbrancePVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_XLEBICCEXTRACT.LEGAL_ENTITY_EXTRACT_PVO       xleentityprofiles, --FscmTopModelAM.FinExtractAM.XleBiccExtractAM.LegalEntityExtractPVO
                        --CES_ERP.FSCM_PROD_FINXLABALINQSUPPREFBAL.SUPPORTING_REFERENCE_BALANCE_PVO            xlaglledgers, --FscmTopModelAM.FinXlaBalInqSuppRefBalAM.SupportingReferenceBalancePVO NO DATA check if required
                        (select * from CES_ERP.FSCM_PROD_USER.USER_PVO QUALIFY row_number() OVER (PARTITION BY USER_PEOUSERNAME ORDER BY USER_HISTORY_PEOCREATION_DATE DESC) = 1) usercreatedby,  --FscmTopModelAM.ChangeObjectsAM.UserPVO
                        (select * from CES_ERP.FSCM_PROD_USER.USER_PVO QUALIFY row_number() OVER (PARTITION BY USER_PEOUSERNAME ORDER BY USER_HISTORY_PEOCREATION_DATE DESC) = 1) userupdatedby,  --FscmTopModelAM.ChangeObjectsAM.UserPVO
                        CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL      personcreatedby, --FscmTopModelAM.PersonAM.PersonNamePVOViewAll
                        CES_ERP.FSCM_PROD_PERSON.PERSON_NAME_PVOVIEW_ALL      personupdatedby --FscmTopModelAM.PersonAM.PersonNamePVOViewAll
                    WHERE
                        ( xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID = xlahdr.JOURNAL_ENTRY_HEADER_AE_HEADER_ID
                          AND xlalines.JOURNAL_ENTRY_LINE_APPLICATION_ID = subledgerapplnsetup.SUBLEDGER_APPLICATION_SETUP_APPLICATION_ID
                          AND xlalines.JOURNAL_ENTRY_LINE_CURRENCY_CONVERSION_TYPE = conversiontypexlaline.DAILY_CONVERSION_TYPE_CONVERSION_TYPE (+)
                          AND xlahdr.JOURNAL_ENTRY_HEADER_ENTITY_ID = transactionentity.TRANSACTION_ENTITY_ID
                          AND xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID = ledgers.LEDGER_LEDGER_ID
                          AND transactionentity.TRANSACTION_LEGAL_ENTITY_ID = legalent.LEGAL_ENTITY_LEGAL_ENTITY_ID (+)
                          AND xlahdr.JOURNAL_ENTRY_HEADER_APPLICATION_ID = evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_APPLICATION_ID
                          AND xlahdr.JOURNAL_ENTRY_HEADER_EVENT_TYPE_CODE = evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_EVENT_TYPE_CODE
                          AND 'US' = evnttypexlahdrvl.EVENT_TYPE_TRANSLATION_LANGUAGE
                          AND xlalines.JOURNAL_ENTRY_LINE_PARTY_ID = custacct.CUST_ACCOUNT_ID (+)
                          AND xlalines.JOURNAL_ENTRY_LINE_ENCUMBRANCE_TYPE_ID = journalencumbrancetype.ENCUMBRANCE_TYPE_ID (+)
                          AND xlahdr.JOURNAL_ENTRY_HEADER_LEGAL_ENTITY_ID = xleentityprofiles.LEGAL_ENTITY_LEGAL_ENTITY_ID (+)
                          --AND xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID = xlaglledgers.LEDGERS_LEDGER_ID
                          AND xlahdr.JOURNAL_ENTRY_HEADER_CREATED_BY = usercreatedby.USER_PEOUSERNAME (+)
                          AND ( 'Y' ) = usercreatedby.USER_PEOACTIVE_FLAG (+)
                          AND xlahdr.JOURNAL_ENTRY_HEADER_LAST_UPDATED_BY = userupdatedby.USER_PEOUSERNAME (+)
                          AND ( 'Y' ) = userupdatedby.USER_PEOACTIVE_FLAG (+)
                          AND usercreatedby.USER_PEOPERSON_ID = personcreatedby.PERSON_NAME_PEOPERSON_ID (+)
                          AND userupdatedby.USER_PEOPERSON_ID = personupdatedby.PERSON_NAME_PEOPERSON_ID (+)
                          AND ( SYSDATE() BETWEEN personcreatedby.EFFECTIVE_START_DATE (+) AND personcreatedby.EFFECTIVE_END_DATE (+) )
                          AND ( SYSDATE() BETWEEN personupdatedby.EFFECTIVE_START_DATE (+) AND personupdatedby.EFFECTIVE_END_DATE (+) ) )
                        AND ( ( 1 = 1 ) )
                ) v97717822,
                (
                    SELECT
                        supplier.SUPPLIER_SEGMENT_1 as segment1,
                        supplier.VENDOR_ID AS vendor_id557,
                        supplier.PARTY_PARTY_ID     AS party_id824,
                        supplier.PARTY_PARTY_NAME   AS party_name1
                    FROM
                        CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO supplier
                        --poz_suppliers supplier, --FscmTopModelAM.PrcPozPublicViewAM.SupplierPVO
                        --hz_parties    party --FscmTopModelAM.PrcPozPublicViewAM.SupplierPVO
--                    WHERE
--                        ( supplier.party_id = party.party_id )  AND ( ( 1 = 1 ) )
                ) v448314251,
--                (
--                    SELECT
--                        datafoxscoringcriteriapeo.criterion_id,
--                        datafoxscoringcriteriapeo.vendor_id
--                    FROM
--                        poz_df_scoring_criteria datafoxscoringcriteriapeo
--                ) v327536137,
                (
                    SELECT /*+ qb_name(LedgerPVO) */
                        ledger.LEDGER_ID ledger_id,
                        ledger.LEDGER_NAME                       AS name496,
                        ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_NAME AS name1050,
                        ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_STRUCTURE_INSTANCE_ID as structure_instance_id
                    FROM
                        CES_ERP.FSCM_PROD_FINGLLEDGERDEFN.LEDGER_PVO ledger
                    WHERE
                        ( 
                        --ledger.chart_of_accounts_id = keyflexfieldstructureinstanc.structure_instance_number  AND 
                         ( 101 ) = ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_APPLICATION_ID
                          AND ( 'GL#' ) = ledger.KEY_FLEXFIELD_STRUCTURE_INSTANC_KEY_FLEXFIELD_CODE )
                        AND ( ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) ) )
                ) v498378102,
                (
                    SELECT
                        customerpartypeo.PARTY_PARTY_ID as party_id
                    FROM
                        CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO customerpartypeo --FscmTopModelAM.PrcPozPublicViewAM.SupplierPVO, CustomerPVO??
                ) v405133203,
                (
                    SELECT /*+ qb_name(OrganizationPVO) */
                        organizationpeo.EFFECTIVE_END_DATE as effective_end_date,
                        organizationpeo.EFFECTIVE_START_DATE as effective_start_date,
                        organizationpeo.ORGANIZATION_NAME as organization_name,
                        organizationpeo.ORGANIZATION_PROFILE_ID as organization_profile_id,
                        organizationpeo.PARTY_ID as party_id
                    FROM
                        CES_ERP.FSCM_PROD_PARTIESANALYTICS.ORGANIZATION organizationpeo --FscmTopModelAM.PartiesAnalyticsAM.Organization

                    WHERE
                        ( SYSDATE() BETWEEN organizationpeo.EFFECTIVE_START_DATE AND organizationpeo.EFFECTIVE_END_DATE )
                        AND ( organizationpeo.EFFECTIVE_LATEST_CHANGE = 'Y' )
                ) v74173908,
                (
                    SELECT
                        jrnlsrc.JOURNAL_SOURCE_JE_SOURCE_NAME as je_source_name,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_DESCRIPTION         AS lang_description,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_JE_SOURCE_NAME      AS lang_je_source_name1,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_LANGUAGE            AS lang_language,
                        jrnlsrc.JRNL_SRC_TRANS_LANG_USER_JE_SRC_NAME AS lang_user_je_source_name
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_SOURCE_EXTRACT_PVO  jrnlsrc --, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalSourceExtractPVO
                        --gl_je_sources_tl jrnlsrctranslang --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalSourceExtractPVO
                    WHERE
                       --     jrnlsrc.je_source_name = jrnlsrctranslang.je_source_name  AND 
                             'US' = jrnlsrc.JRNL_SRC_TRANS_LANG_LANGUAGE
                ) v399845456,
                (
                    SELECT
                        currenciesbpeo.CURRENCY_CODE as currency_code--,
                        --currenciesbpeo."DigitalCurrencyCode" as digital_currency_code
                    FROM
                        CES_ERP.FSCM_PROD_ANALYTICSSERVICE.CURRENCIES_TLPVO currenciesbpeo --might need CURRENCIES_PVO
                        --fnd_currencies_b currenciesbpeo --FscmTopModelAM.AnalyticsServiceAM.CurrenciesPVO
                ) v220341495,
                (
                    select 
                        organizationunit.ORGANIZATION_ID      AS organization_id1,
                        organizationunit.EFFECTIVE_START_DATE AS effective_start_date651,
                        organizationunit.EFFECTIVE_END_DATE   AS effective_end_date8,
                        organizationunit.ORG_INFO_BUPEOORG_INFORMATION_ID as org_information_id,
                        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEONAME as name,
                        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOORGANIZATION_ID as organization_id,
                        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_START_DATE as effective_start_date,
                        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_END_DATE as effective_end_date,
                        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOLANGUAGE as language
                    from 
                       CES_ERP.FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_PVO organizationunit
                    where ( DATE_TRUNC('day', SYSDATE()) BETWEEN organizationunit.EFFECTIVE_START_DATE AND organizationunit.EFFECTIVE_END_DATE )
                    AND ( DATE_TRUNC('day', SYSDATE()) BETWEEN organizationunit.ORG_INFO_BUPEOEFFECTIVE_START_DATE AND organizationunit.ORG_INFO_BUPEOEFFECTIVE_END_DATE )
                    AND ( DATE_TRUNC('day', SYSDATE()) BETWEEN organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_START_DATE AND organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_END_DATE )  
                ) v86023658,
                (
                    SELECT
                        transactionheader.RA_CUSTOMER_TRX_COMPLETE_FLAG as complete_flag,
                        transactionheader.RA_CUSTOMER_TRX_CUST_TRX_TYPE_SEQ_ID as cust_trx_type_seq_id,
                        transactionheader.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID as customer_trx_id,
                        transactionheader.RA_CUSTOMER_TRX_TRX_NUMBER as trx_number,
                        NVL(transactionheader.RA_CUSTOMER_TRX_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) as TRX_LAST_UPDATE_DATE
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_HEADER_EXTRACT_PVO transactionheader

                ) v291284714,
                (
                    SELECT
                        transactiontype.CUST_TRX_TYPE_SEQ_ID as cust_trx_type_seq_id,
                        transactiontype.TRANSACTION_TYPE_NAME as name,
                        transactiontype.TRANSACTION_TYPE_TYPE as type,
                        transactiontype.AR_LOOKUP_LOOKUP_CODE as lookup_code,
                        transactiontype.AR_LOOKUP_LOOKUP_TYPE as lookup_type,
                        transactiontype.AR_LOOKUP_MEANING as meaning
                    FROM
                        CES_ERP.FSCM_PROD_FINARTOPPUBLICMODEL.TRANSACTION_TYPE_PVO transactiontype
                    WHERE
                            ( 'INV/CM' ) = transactiontype.AR_LOOKUP_LOOKUP_TYPE
                ) v211460917,
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
                    FROM CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO biflexfieldeo
                ) v208318186,
                (
                    SELECT /*+ qb_name(JournalImportReferencePVO) */
                        glimportreferences.GL_IMPORT_REFERENCES_JE_HEADER_ID as je_header_id,
                        glimportreferences.GL_IMPORT_REFERENCES_JE_LINE_NUM as je_line_num,
                        glimportreferences.GL_IMPORT_REFERENCES_GL_SL_LINK_ID as gl_sl_link_id,
                        glimportreferences.GL_IMPORT_REFERENCES_GL_SL_LINK_TABLE as gl_sl_link_table,
                        jrnlhdr.GL_JE_HEADERS_LEDGER_ID as ledger_id
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_IMPORT_REFERENCE_EXTRACT_PVO glimportreferences, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalImportReferenceExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_HEADER_EXTRACT_PVO        jrnlhdr --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO
                    WHERE
                        ( glimportreferences.GL_IMPORT_REFERENCES_JE_HEADER_ID = jrnlhdr.JE_HEADER_ID )
                        AND ( ( ( glimportreferences.GL_IMPORT_REFERENCES_GL_SL_LINK_ID IS NOT NULL ) ) )
                ) v367001394,
                (
                    SELECT 
                        jrnlline.JE_HEADER_ID as je_header_id,
                        jrnlline.JE_LINE_NUM as je_line_num,
                        jrnlhdr.JE_HEADER_ID                                     AS je_header_id1,
                        jrnlhdr.GL_JE_HEADERS_NAME as name,
                        jrnlbatch.JOURNAL_BATCH_JE_BATCH_ID                                    AS je_batch_id1,
                        jrnlbatch.JOURNAL_BATCH_POSTED_DATE                                    AS posted_date1,
                        jrnlbatch.JOURNAL_BATCH_STATUS                                         AS status2,
                        ( decode(jrnlbatch.JOURNAL_BATCH_STATUS, 'u', 'U', jrnlbatch.JOURNAL_BATCH_STATUS) ) AS posting_status_code
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_LINE_EXTRACT_PVO   jrnlline, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalLineExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_HEADER_EXTRACT_PVO jrnlhdr, --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalHeaderExtractPVO
                        CES_ERP.FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.JOURNAL_BATCH_EXTRACT_PVO jrnlbatch --FscmTopModelAM.FinExtractAM.GlBiccExtractAM.JournalBatchExtractPVO
                    WHERE
                        ( jrnlline.JE_HEADER_ID = jrnlhdr.JE_HEADER_ID
                          AND jrnlhdr.GL_JE_HEADERS_JE_BATCH_ID = jrnlbatch.JOURNAL_BATCH_JE_BATCH_ID )
                  --      AND ( ( jrnlline."GlJeLinesLedgerId" IN ( 300000026463625, 300000035198785, 300000126421633, 300000026463631, 300000001398016, 300000001407023,300000001410020, 300000001410050, 300000094032925 ) ) )
                ) v169022212,
                (
                    SELECT
                        invoiceheader.AP_INVOICES_DESCRIPTION as description,
                        invoiceheader.AP_INVOICES_INVOICE_DATE as invoice_date,
                        invoiceheader.AP_INVOICES_INVOICE_ID as invoice_id,
                        invoiceheader.AP_INVOICES_INVOICE_NUM as invoice_num,
                        invoiceheader.AP_INVOICES_VOUCHER_NUM as voucher_num,
                        purchaseorder.PURCHASING_DOCUMENT_HEADER_PO_HEADER_ID AS po_header_id1,
                        purchaseorder.PURCHASING_DOCUMENT_HEADER_SEGMENT_1 as segment1,
                        party.SUPPLIER_PARTY_ID            AS party_party_id,
                        party.PARTY_PARTY_NAME           AS party_party_name,
                        GREATEST(NVL(invoiceheader.AP_INVOICES_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')), NVL(party.SUPPLIER_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) AS INVOICES_LAST_UPDATE_DATE
                    FROM
                        CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.INVOICE_HEADER_EXTRACT_PVO invoiceheader, --FscmTopModelAM.FinExtractAM.ApBiccExtractAM.InvoiceHeaderExtractPVO
                        
                        (SELECT * FROM (
                            SELECT *,
                                   ROW_NUMBER() OVER (PARTITION BY PURCHASING_DOCUMENT_HEADER_PO_HEADER_ID ORDER BY SEQUENCE_NUM DESC) AS rn
                            FROM CES_ERP.FSCM_PROD_PRCPOPUBLICVIEW.PURCHASE_ORDER_HISTORY_PVO
                        ) WHERE rn = 1)  purchaseorder, --FscmTopModelAM.PrcPoPublicViewAM.PurchaseOrderHistoryPVO -- checks for dupes due to history - refer AP
                        CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO      party --FscmTopModelAM.PrcPozPublicViewAM.SupplierPVO
                    WHERE
                        ( invoiceheader.AP_INVOICES_PO_HEADER_ID = purchaseorder.PURCHASING_DOCUMENT_HEADER_PO_HEADER_ID (+)
                          AND invoiceheader.AP_INVOICES_PARTY_ID = party.SUPPLIER_PARTY_ID (+) )
                        AND ( ( 1 = 1 ) )
                ) v440969301
            WHERE
                ( v97717822.party_id = v448314251.vendor_id557 (+)
                --  AND v448314251.vendor_id557 = v327536137.vendor_id (+)
                  AND v97717822.ledger_id = v498378102.ledger_id
                  AND v97717822.party_id1 = v405133203.party_id (+)
                  AND v405133203.party_id = v74173908.party_id (+)
                  AND v97717822.je_source_name = v399845456.je_source_name
                  AND v97717822.currency_code = v220341495.currency_code
                  AND v97717822.security_id_int_1 = v86023658.organization_id1 (+)
                  AND v97717822.ar_customer_trx_id = v291284714.customer_trx_id (+)
                  AND v291284714.cust_trx_type_seq_id = v211460917.cust_trx_type_seq_id (+)
                  AND v97717822.code_combination_id = v208318186.s_g_0
                  AND v97717822.chart_of_accounts_id = v208318186.s_g_1                  
                  AND v97717822.gl_sl_link_id = v367001394.gl_sl_link_id (+)
                  AND v97717822.gl_sl_link_table = v367001394.gl_sl_link_table (+)
                  AND v97717822.ledger_id = v367001394.ledger_id (+)
                  AND v367001394.je_header_id = v169022212.je_header_id (+)
                  AND v367001394.je_line_num = v169022212.je_line_num (+)
                  AND v97717822.ap_invoice_id = v440969301.invoice_id (+) )
                AND ( ( ( NOT ( ( v211460917.type = 'DEP' ) ) )
                        OR ( ( v211460917.type IS NULL ) ) )
                      AND ( ( NOT ( ( v211460917.type = 'GUAR' ) ) )
                            OR ( ( v211460917.type IS NULL ) ) )
                      AND ( ( NOT ( ( v211460917.type = 'PMT' ) ) )
                            OR ( ( v211460917.type IS NULL ) ) )
                      --AND ( NOT ( ( v97717822.last_update_date_389 < to_timestamp(:P_EXTRACT_DATE_TS, 'yyyy-mm-dd hh24:mi:ss.ff') - interval '6' hour ) ) ) 
                      )
      
        ) t6080001
  {% if is_incremental() %}
where  t6080001.INCREMENTAL_LATEST_DATE >= (SELECT DATE(dateadd(days, {{var('erp_delta_load_days')}}, LAST_UPDATED_TIMESTAMP)) FROM  COMMON_{{target.name.upper()}}.ERP.CONTROL_TIME_INCREMENTAL_LOAD WHERE TABLE_NAME = '{{trimmed_source_name}}' )
         {% endif %}      
), sawith1 AS (
    SELECT
        SUM(d1.c88)                                                          AS c1,
        SUM(d1.c92)                                                          AS c2,
        SUM(d1.c93)                                                          AS c3,
        SUM(d1.c94)                                                          AS c4,
        SUM(d1.c95)                                                          AS c5,
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
        d1.c16                                                               AS c16,
        d1.c17                                                               AS c17,
        d1.c18                                                               AS c18,
        d1.c19                                                               AS c19,
        d1.c20                                                               AS c20,
        d1.c21                                                               AS c21,
        d1.c25                                                               AS c25,
        d1.c26                                                               AS c26,
        d1.c27                                                               AS c27,
        d1.c28                                                               AS c28,
        d1.c29                                                               AS c29,
        d1.c30                                                               AS c30,
        d1.c31                                                               AS c31,
        d1.c32                                                               AS c32,
        d1.c33                                                               AS c33,
        d1.c34                                                               AS c34,
        d1.c36                                                               AS c36,
        d1.c37                                                               AS c37,
        d1.c38                                                               AS c38,
        d1.c39                                                               AS c39,
        d1.c40                                                               AS c40,
        d1.c41                                                               AS c41,
        CAST(d1.c91 AS VARCHAR(100))                                         AS c42,
        d1.c43                                                               AS c43,
        d1.c44                                                               AS c44,
        d1.c45                                                               AS c45,
        d1.c46                                                               AS c46,
        d1.c47                                                               AS c47,
        CASE
            WHEN d1.c89 IS NULL THEN
                d1.c90
            ELSE
                d1.c89
        END                                                                  AS c48,
        d1.c49                                                               AS c49,
        d1.c50                                                               AS c50,
        d1.c51                                                               AS c51,
        d1.c52                                                               AS c52,
        d1.c53                                                               AS c53,
        d1.c55                                                               AS c55,
        d1.c56                                                               AS c56,
        d1.c57                                                               AS c57,
        d1.c58                                                               AS c58,
        d1.c59                                                               AS c59,
        d1.c60                                                               AS c60,
        d1.c61                                                               AS c61,
        d1.c62                                                               AS c62,
        d1.c64                                                               AS c64,
        d1.c65                                                               AS c65,
        d1.c67                                                               AS c67,
        d1.c68                                                               AS c68,
        d1.c69                                                               AS c69,
        concat(CAST(d1.c60 AS CHARACTER(30)), CAST(d1.c61 AS CHARACTER(30))) AS c70,
        d1.c71                                                               AS c71,
        d1.c72                                                               AS c72,
        d1.c73                                                               AS c73,
        d1.c74                                                               AS c74,
        d1.c76                                                               AS c76,
        d1.c78                                                               AS c78,
        d1.c80                                                               AS c80,
        d1.c82                                                               AS c82,
        d1.c85                                                               AS c85,
        d1.c87                                                               AS c87,
        d1.ID,
        d1.INCREMENTAL_LATEST_DATE
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
        d1.c16,
        d1.c17,
        d1.c18,
        d1.c19,
        d1.c20,
        d1.c21,
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
        d1.c36,
        d1.c37,
        d1.c38,
        d1.c39,
        d1.c40,
        d1.c41,
        d1.c43,
        d1.c44,
        d1.c45,
        d1.c46,
        d1.c47,
        d1.c49,
        d1.c50,
        d1.c51,
        d1.c52,
        d1.c53,
        d1.c55,
        d1.c56,
        d1.c57,
        d1.c58,
        d1.c59,
        d1.c60,
        d1.c61,
        d1.c62,
        d1.c64,
        d1.c65,
        d1.c67,
        d1.c68,
        d1.c69,
        d1.c71,
        d1.c72,
        d1.c73,
        d1.c74,
        d1.c76,
        d1.c78,
        d1.c80,
        d1.c82,
        d1.c85,
        d1.c87,
        d1.ID,
        d1.INCREMENTAL_LATEST_DATE,
        CASE
            WHEN d1.c89 IS NULL THEN
                    d1.c90
            ELSE
                d1.c89
        END,
        CAST(d1.c91 AS VARCHAR(100))
), sawith2 AS (
    SELECT
        t6080002.c389230568 AS c1,
        t6080002.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'XLA_ACCOUNTING_ENTRY_TYPE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 602 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t6080002
), sawith3 AS (
    SELECT
        t6080003.c389230568 AS c1,
        t6080003.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'XLA_BALANCE_TYPE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 602 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t6080003
), sawith4 AS (
    SELECT
        t6080004.c389230568 AS c1,
        t6080004.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'XLA_BUSINESS_FLOW_CLASS' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 602 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t6080004
), sawith5 AS (
    SELECT
        t6080005.c389230568 AS c1,
        t6080005.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'XCC_BC_FUNDS_STATUSES' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 0 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) t6080005
), sawith6 AS (
    SELECT
        t6080006.c185454959 AS c1,
        t6080006.c103205687 AS c2
    FROM
        (
            SELECT
                v248136131.NAME          AS c185454959,
                v248136131.CURRENCY_CODE AS c103205687,
                v248136131.LANGUAGE      AS c315124100
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.CURRENCIES_TLPVO v248136131 --FscmTopModelAM.AnalyticsServiceAM.CurrenciesTLPVO
            WHERE
                ( ( ( v248136131.LANGUAGE = 'US' ) ) )
        ) t6080006
), sawith7 AS (
    SELECT
        t6080007.c389230568 AS c1,
        t6080007.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'MJE_BATCH_STATUS' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE  = 'US' ) ) )
        ) t6080007
), sawith8 AS (
    SELECT
        t6080008.c389230568 AS c1,
        t6080008.c164164071 AS c2
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
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585 --FscmTopModelAM.AnalyticsServiceAM.LookupValuesTLPVO
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE  = 'YES/NO' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 222 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )

        ) t6080008
), sawith9 AS (
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
        d2.c1  AS c22,
        d1.c76 AS c23,
        d3.c1  AS c24,
        d1.c78 AS c25,
        d4.c1  AS c26,
        d1.c80 AS c27,
        d1.c25 AS c28,
        d1.c26 AS c29,
        d1.c27 AS c30,
        d1.c28 AS c31,
        d1.c29 AS c32,
        d1.c30 AS c33,
        d1.c31 AS c34,
        d1.c32 AS c35,
        d1.c33 AS c36,
        d1.c34 AS c37,
        d5.c1  AS c38,
        d1.c82 AS c39,
        d1.c36 AS c40,
        d1.c37 AS c41,
        d1.c38 AS c42,
        d1.c39 AS c43,
        d1.c40 AS c44,
        d1.c41 AS c45,
        d1.c42 AS c46,
        d1.c43 AS c47,
        d1.c44 AS c48,
        d1.c45 AS c49,
        d1.c46 AS c50,
        d1.c47 AS c51,
        d1.c48 AS c52,
        d1.c49 AS c53,
        d1.c50 AS c54,
        d1.c51 AS c55,
        d1.c52 AS c56,
        d1.c53 AS c57,
        d6.c1  AS c58,
        d1.c73 AS c59,
        d1.c55 AS c60,
        d1.c56 AS c61,
        d1.c57 AS c62,
        d1.c58 AS c63,
        d1.c59 AS c64,
        d1.c60 AS c65,
        d1.c61 AS c66,
        d1.c62 AS c67,
        d7.c1  AS c68,
        d1.c85 AS c69,
        d1.c64 AS c70,
        d1.c65 AS c71,
        d8.c1  AS c72,
        d1.c87 AS c73,
        d1.c67 AS c74,
        d1.c68 AS c75,
        d1.c69 AS c76,
        d1.c70 AS c77,
        d1.c71 AS c78,
        d1.c72 AS c79,
        d1.c74 AS c80,
        d1.ID,
        d1.INCREMENTAL_LATEST_DATE
    FROM
        (
            (
                (
                    (
                        (
                            (
                                sawith1 d1
                                LEFT OUTER JOIN sawith2 d2 ON d1.c76 = d2.c2
                            )
                            LEFT OUTER JOIN sawith3 d3 ON d1.c78 = d3.c2
                        )
                        LEFT OUTER JOIN sawith4 d4 ON d1.c80 = d4.c2
                    )
                    LEFT OUTER JOIN sawith5 d5 ON d1.c82 = d5.c2
                )
                LEFT OUTER JOIN sawith6 d6 ON d1.c73 = d6.c2
            )
            LEFT OUTER JOIN sawith7 d7 ON d1.c85 = d7.c2
        )
        LEFT OUTER JOIN sawith8 d8 ON d1.c87 = d8.c2
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
        d1.c58 AS c58,
        d1.c59 AS c59,
        d1.c60 AS c60,
        d1.c61 AS c61,
        d1.c62 AS c62,
        d1.c63 AS c63,
        d1.c64 AS c64,
        d1.c65 AS c65,
        d1.c66 AS c66,
        d1.c67 AS c67,
        d1.c68 AS c68,
        d1.c70 AS c70,
        d1.c71 AS c71,
        d1.c72 AS c72,
        d1.c73 AS c73,
        d1.c74 AS c74,
        d1.c75 AS c75,
        d1.c76 AS c76,
        d1.c77 AS c77,
        d1.c78 AS c78,
        d1.c79 AS c79,
        d1.c80 AS c80,
        d1.c81 AS c81,
        d1.ID,
        d1.INCREMENTAL_LATEST_DATE
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
                d901.c16                AS c11,
                d901.c17                AS c12,
                d901.c18                AS c13,
                d901.c19                AS c14,
                d901.c20                AS c15,
                d901.c21                AS c16,
                nvl(d901.c22, d901.c23) AS c17,
                nvl(d901.c24, d901.c25) AS c18,
                nvl(d901.c26, d901.c27) AS c19,
                d901.c28                AS c20,
                d901.c29                AS c21,
                d901.c30                AS c22,
                d901.c31                AS c23,
                d901.c32                AS c24,
                d901.c33                AS c25,
                d901.c34                AS c26,
                d901.c35                AS c27,
                d901.c36                AS c28,
                d901.c37                AS c29,
                nvl(d901.c38, d901.c39) AS c30,
                d901.c40                AS c31,
                d901.c41                AS c32,
                d901.c42                AS c33,
                d901.c43                AS c34,
                d901.c44                AS c35,
                d901.c45                AS c36,
                d901.c46                AS c37,
                d901.c47                AS c38,
                d901.c48                AS c39,
                d901.c49                AS c40,
                d901.c50                AS c41,
                d901.c51                AS c42,
                d901.c52                AS c43,
                d901.c53                AS c44,
                d901.c54                AS c45,
                d901.c55                AS c46,
                d901.c56                AS c47,
                d901.c57                AS c48,
                nvl(d901.c58, d901.c59) AS c49,
                d901.c60                AS c50,
                d901.c61                AS c51,
                d901.c5                 AS c52,
                d901.c4                 AS c53,
                d901.c3                 AS c54,
                d901.c2                 AS c55,
                d901.c1                 AS c56,
                d901.c62                AS c57,
                d901.c63                AS c58,
                d901.c64                AS c59,
                d901.c65                AS c60,
                d901.c66                AS c61,
                d901.c67                AS c62,
                nvl(d901.c68, d901.c69) AS c63,
                d901.c70                AS c64,
                d901.c71                AS c65,
                nvl(d901.c72, d901.c73) AS c66,
                d901.c74                AS c67,
                d901.c75                AS c68,
                d901.c76                AS c70,
                d901.c77                AS c71,
                d901.c78                AS c72,
                d901.c79                AS c73,
                d901.c59                AS c74,
                d901.c80                AS c75,
                d901.c23                AS c76,
                d901.c25                AS c77,
                d901.c27                AS c78,
                d901.c39                AS c79,
                d901.c69                AS c80,
                d901.c73                AS c81,
                d901.ID,
                d901.INCREMENTAL_LATEST_DATE,
                ROW_NUMBER()
                OVER(PARTITION BY d901.c6, d901.c7, d901.c8, d901.c9, d901.c10,
                                  d901.c11, d901.c12, d901.c13, d901.c14, d901.c15,
                                  d901.c16, d901.c17, d901.c18, d901.c19, d901.c20,
                                  d901.c21, d901.c23, d901.c25, d901.c27, d901.c28,
                                  d901.c29, d901.c30, d901.c31, d901.c32, d901.c33,
                                  d901.c34, d901.c35, d901.c36, d901.c37, d901.c39,
                                  d901.c40, d901.c41, d901.c42, d901.c43, d901.c44,
                                  d901.c45, d901.c46, d901.c47, d901.c48, d901.c49,
                                  d901.c50, d901.c51, d901.c52, d901.c53, d901.c54,
                                  d901.c55, d901.c56, d901.c57, d901.c59, d901.c60,
                                  d901.c61, d901.c62, d901.c63, d901.c64, d901.c65,
                                  d901.c66, d901.c67, d901.c69, d901.c70, d901.c71,
                                  d901.c73, d901.c74, d901.c75, d901.c76, d901.c77,
                                  d901.c78, d901.c79, d901.c80,d901.ID,d901.INCREMENTAL_LATEST_DATE
                     ORDER BY
                         d901.c6 ASC, d901.c7 ASC, d901.c8 ASC,
                         d901.c9 ASC, d901.c10 ASC, d901.c11 ASC, d901.c12 ASC, d901.c13 ASC,
                         d901.c14 ASC, d901.c15 ASC, d901.c16 ASC, d901.c17 ASC, d901.c18 ASC,
                         d901.c19 ASC, d901.c20 ASC, d901.c21 ASC, d901.c23 ASC, d901.c25 ASC,
                         d901.c27 ASC, d901.c28 ASC, d901.c29 ASC, d901.c30 ASC, d901.c31 ASC,
                         d901.c32 ASC, d901.c33 ASC, d901.c34 ASC, d901.c35 ASC, d901.c36 ASC,
                         d901.c37 ASC, d901.c39 ASC, d901.c40 ASC, d901.c41 ASC, d901.c42 ASC,
                         d901.c43 ASC, d901.c44 ASC, d901.c45 ASC, d901.c46 ASC, d901.c47 ASC,
                         d901.c48 ASC, d901.c49 ASC, d901.c50 ASC, d901.c51 ASC, d901.c52 ASC,
                         d901.c53 ASC, d901.c54 ASC, d901.c55 ASC, d901.c56 ASC, d901.c57 ASC,
                         d901.c59 ASC, d901.c60 ASC, d901.c61 ASC, d901.c62 ASC, d901.c63 ASC,
                         d901.c64 ASC, d901.c65 ASC, d901.c66 ASC, d901.c67 ASC, d901.c69 ASC,
                         d901.c70 ASC, d901.c71 ASC, d901.c73 ASC, d901.c74 ASC, d901.c75 ASC,
                         d901.c76 ASC, d901.c77 ASC, d901.c78 ASC, d901.c79 ASC, d901.c80 ASC,d901.ID asc,d901.INCREMENTAL_LATEST_DATE asc
                )                       AS c82
            FROM
                sawith9 d901
        ) d1
    WHERE
        ( d1.c82 = 1 )
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
    d1.c8 AS IDENTIFYING_PO,
    d1.c9 AS INVOICE_DATE,
    d1.c10 AS INVOICE_DESC,
    d1.c11 AS INVOICE_NUM,
    d1.c12 AS PARTY_NAME,
    d1.c13 AS VOUCHER_NUM,
    d1.c14 AS CONCATENATED_SEGMENTS,
    d1.c15 AS POSTING_DATE,
    d1.c16 AS ACCOUNTING_DATE,
    d1.c17 AS ACCOUNTING_ENTRY_TYPE,
    d1.c18 AS BALANCE_TYPE,
    d1.c19 AS BUSINESS_CLASS_NAME,
    d1.c20 AS BUSINESS_UNIT_NAME,
    d1.c21 AS CURRENCY_CONVERSION_DATE,
    d1.c22 AS CURRENCY_CONVERSION_DESC,
    d1.c23 AS CURRENCY_CONVERSION_RATE,
    d1.c24 AS CURRENCY_CONVERSION_TYPE,
    d1.c25 AS DATE_COMPLETED,
    d1.c26 AS DATE_TRANSFERRED_TO_GL,
    d1.c27 AS ENCUMBRANCE_TYPE,
    d1.c28 AS EVENT_TYPE_CODE,
    d1.c29 AS EVENT_TYPE_NAME,
    d1.c30 AS FUNDS_STATUS,
    d1.c31 AS HEADER_PERIOD_NAME,
    d1.c32 AS JOURNAL_CATEGORY_NAME,
    d1.c33 AS JOURNAL_CREATED_BY_USERNAME,
    d1.c34 AS JOURNAL_CREATED_BY,
    d1.c35 AS JOURNAL_CREATED_DATE,
    d1.c36 AS JOURNAL_DESC,
    d1.c37 AS JOURNAL_HEADER_ID,
    d1.c38 AS LAST_UPDATED_BY_USERNAME,
    d1.c39 AS LAST_UPDATED_BY,
    to_char(d1.c40,'YYYY-MM-DD HH24:MI:SS') AS LAST_UPDATE_DATE,
    d1.c41 AS JOURNAL_LINE_DESC,
    d1.c42 AS JOURNAL_LINE_NUM,
    d1.c43 AS LEGAL_ENTITY_NAME,
    d1.c44 AS OVERRIDE_REASON,
    d1.c45 AS TRANSACTION_NUMBER,
    d1.c46 AS SOURCE_JOURNAL_SOURCE_DESC,
    d1.c47 AS SOURCE_JOURNAL_SOURCE_NAME,
    d1.c48 AS USER_JOURNAL_SOURCE_NAME,
    d1.c49 AS DOCUMENT_CURRENCY_NAME,
    d1.c50 AS CHART_OF_ACCOUNT,
    d1.c51 AS LEDGER_NAME,
    d1.c52 AS AMOUNT_CR,
    d1.c53 AS AMOUNT_DR,
    d1.c54 AS AMOUNT_ENTERED_CR,
    d1.c55 AS AMOUNT_ENTERED_DR,
    d1.c56 AS STATISTICAL_AMOUNT,
    d1.c57 AS AR_TRANSACTION_NUMBER,
    d1.c58 AS SUPPLIER_NUMBER,
    d1.c59 AS SUPPLIER_NAME,
    d1.c60 AS GL_JOURNAL_HEADER_ID,
    d1.c61 AS GL_JOURNAL_LINE_NUM,
    d1.c62 AS JOURNAL_NAME,
    d1.c63 AS JOURNAL_STATUS,
    d1.c64 AS TRANSACTION_CLASS,
    d1.c65 AS TRANSACTION_TYPE,
    d1.c66 AS TRANSACTION_COMPLETE,
    d1.c67 AS BILL_TO_CUSTOMER,
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
            d1.c58 AS c58,
            d1.c59 AS c59,
            d1.c60 AS c60,
            d1.c61 AS c61,
            d1.c62 AS c62,
            d1.c63 AS c63,
            d1.c64 AS c64,
            d1.c65 AS c65,
            d1.c66 AS c66,
            d1.c67 AS c67,
            d1.ID,
            d1.INCREMENTAL_LATEST_DATE
        FROM
            sawith10 d1
    ) d1
