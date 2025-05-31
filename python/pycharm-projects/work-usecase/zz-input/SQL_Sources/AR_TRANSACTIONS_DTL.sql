
WITH 
SAWITH0 AS (select  /*+ inline */  
     T6235986.ID,
	 T6235986.C406671630 as c9,
     T6235986.C233929166 as c10,
     T6235986.C31199523 as c11,
     T6235986.C7207427 as c12,
     T6235986.C433723576 as c13,
     T6235986.C417910867 as c14,
     T6235986.C276323548 as c15,
     T6235986.C420124855 as c16,
     T6235986.C33224554 as c17,
     T6235986.C187863249 as c18,
     T6235986.C301161417 as c19,
     T6235986.C203685335 as c20,
     T6235986.C527751494 as c21,
     T6235986.C369898840 as c22,
     T6235986.C157895813 as c23,
     T6235986.C308010007 as c24,
     T6235986.C184077456 as c25,
     T6235986.C153253872 as c26,
     T6235986.C465216818 as c27,
     T6235986.C161366140 as c28,
     T6235986.C441856931 as c29,
     T6235986.C196380727 as c30,
     T6235986.C196342575 as c31,
     T6235986.C163592185 as c32,
     T6235986.C519818457 as c33,
     T6235986.C422183274 as c34,
     T6235986.C167968178 as c35,
     T6235986.C238687615 as c37,
     T6235986.C3973386 as c39,
     T6235986.C517362176 as c40,
     T6235986.C126400656 as c41,
     T6235986.C214686596 as c42,
     T6235986.C364048903 as c43,
     T6235986.C533631721 as c44,
     T6235986.C129307616 as c45,
     T6235986.C38618484 as c46,
     T6235986.C146557964 as c47,
     T6235986.C330636722 as c48,
     T6235986.C372857905 as c49,
     T6235986.C51067786 as c50,
     T6235986.C57234163 as c51,
     T6235986.C105329137 as c52,
     T6235986.C64190816 as c53,
     T6235986.C197594221 as c54,
     T6235986.C10223035 as c55,
     T6235986.C311905569 as c56,
     T6235986.C5576208 as c59,
     T6235986.C342081810 as c60,
     T6235986.C64203304 as c62,
     T6235986.C215348972 as c64,
     T6235986.C359046800 as c65,
     T6235986.C461354760 as c67,
     T6235986.C230180794 as c68,
     T6235986.C307898839 as c69,
     T6235986.C113420401 as c70,
     T6235986.C299168409 as c72,
     T6235986.C331908290 as c73,
     T6235986.C356728513 as c74,
     T6235986.C379200345 as c75,
     T6235986.C109800147 as c77,
     T6235986.C15439104 as c78,
     T6235986.C168826660 as c83,
     T6235986.C478163046 as c86,
     T6235986.C148776435 as c89,
     T6235986.C9590699 as c90,
     T6235986.C488845938 as c91,
     T6235986.C302941269 as c92,
     T6235986.C501538406 as c93,
     T6235986.C281146953 as c94,
     T6235986.C100662310 as c95,
     T6235986.C75721789 as c96,
     T6235986.C130160118 as c97,
     T6235986.C183425870 as c98,
     T6235986.C22888611 as c99,
     T6235986.C420216650 as c100,
     T6235986.C402304897 as c101,
     T6235986.C525092876 as c102,
     T6235986.C465528056 as c103,
     T6235986.C377811519 as c104,
     T6235986.C298986491 as c105,
     T6235986.C485080380 as c106,
     T6235986.trx_line_id as c107,
     T6235986.INCREMENTAL_LATEST_DATE
from 
     ( 
     
 SELECT
    V208318186.concat_seg              AS c406671630,
    V208318186.GL_BALANCING_           AS c233929166, --entity
    V208318186.FA_COST_CTR_            AS c31199523, --cc
    V208318186.GL_ACCOUNT_             AS c7207427, --account
    V208318186.GL_INTERCOMPANY_        AS c433723576, --inter
    V208318186.GL_MAT_TYPE_            AS c417910867, --mat type
    V208318186.GL_SCHEME_              AS c276323548, --scheme
    V208318186.GL_SPARE_               AS c420124855, --project
    v400621628.concat_values           AS c33224554,
    V400621628.GL_BALANCING_           AS c187863249,
    V400621628.FA_COST_CTR_            AS c301161417,
    V400621628.gl_account_             AS c203685335, --account
    v405133203.party_name              AS c527751494,
    v405133203.party_number            AS c369898840,
    v405133203.party_type              AS c157895813,
    v74173908.organization_name        AS c308010007,
    v503360181.person_first_name       AS c184077456,
    v503360181.person_last_name        AS c153253872,
    v132021520.report_date             AS c465216818,
    v35416688.term_due_date            AS c161366140,
    v382875220.meaning                 AS c441856931,
    v17715999.je_category_name         AS c196380727,
    v17715999.description1             AS c196342575,
    v17715999.displayed_line_number    AS c163592185,
    v157698628.customer_class_code     AS c519818457,
    v157698628.account_name            AS c422183274,
    v157698628.account_number          AS c167968178,
    v157698628.customer_type           AS c238687615,
    v102674681.city                    AS c3973386,
    v102674681.country                 AS c517362176,
    v102674681.description             AS c126400656,
    v102674681.party_site_name         AS c214686596,
    v102674681.party_site_number       AS c364048903,
    v102674681.postal_code             AS c533631721,
    v102674681.state                   AS c129307616,
    v102674681.status                  AS c38618484,
    v102674681.address1                AS c146557964,
    v102674681.address2                AS c330636722,
    v102674681.location                AS c372857905,
    v220364535.name                    AS c51067786,
    v220364535.name2                   AS c57234163,
    v518783208.fiscal_period_number    AS c105329137,
    v518783208.fiscal_period_name      AS c64190816,
    v518783208.fiscal_year_number      AS c197594221,
    v498378102.name496                 AS c10223035,
    ''           AS c311905569, --v220341495.currency_code
    v62756817.description              AS c5576208,
    v62756817.name                     AS c342081810,
	TRIM(REPLACE(v35416688.trx_number , CHR(9), '')) AS c64203304,
    v35416688.name3                    AS c215348972,
    ''          AS c359046800, --v292640615.description128
    v35416688.trx_date                 AS c461354760,
    v144270398.user_period_set_name    AS c230180794,
    v498378102.last_update_date        AS c307898839,
    v35416688.description281           AS c113420401,
    v35416688.line_type                AS c299168409,
    v102674681.site_use_id             AS c331908290,
    v62756817.cust_trx_type_seq_id     AS c356728513,
    v220364535.organization_id1        AS c379200345,
    ''     AS c109800147, --v292640615.code_combination_id
    v498378102.ledger_id               AS c15439104,
    v157698628.status                  AS c168826660,
    v404380990.term_id                 AS c478163046,
    v498378102.ledger_category_code    AS c148776435,
    v35416688.amount                   AS c9590699,
    v35416688.line_number              AS c488845938,
    v35416688.line_number3             AS c302941269,
    v35416688.link_to_cust_trx_line_id AS c501538406,
    v132021520.parent_month            AS c281146953,
    v132021520.day_of_year             AS c100662310,
    v518783208.calendar_id             AS c75721789,
    v518783208.fiscal_quarter_number   AS c130160118,
    v405133203.party_id                AS c183425870,
    v35416688.customer_trx_id          AS c22888611,
    v35416688.last_update_date3        AS c420216650,
    v35416688.acctd_amount             AS c402304897,
    v35416688.account_class            AS c525092876,
    v17715999.unrounded_entered_dr     AS c465528056,
    v17715999.unrounded_entered_cr     AS c377811519,
    v17715999.unrounded_accounted_dr   AS c298986491,
    v17715999.unrounded_accounted_cr   AS c485080380,
    v35416688.latest_rec_flag          AS c149936038,
    v208318186.s_g_0                   AS c210356818,
    v62756817.type                     AS c41833683,
    v35416688.cust_trx_line_gl_dist_id AS c64596933,
    v35416688.trx_line_id              AS trx_line_id,
    v17715999.ae_header_id             AS pka_aeheaderid0,
    v17715999.ref_ae_header_id         AS pka_refaeheaderid0,
    v17715999.temp_line_num            AS pka_templinenum0,
    v17715999.ae_header_id1            AS pka_xlalinesaeheaderid0,
    v17715999.ae_line_num1             AS pka_xlalinesaelinenum0,
    v17715999.ae_header_id2            AS pka_xlahdraeheaderid0,
    v382875220.lookup_type             AS pka_lookuptype0,
    v382875220.lookup_code             AS pka_lookupcode0,
    v144270398.calendar_id             AS pka_glcalendarscalendarid0,
    v144270398.period_set_id           AS pka_glcalendarsperiodsetid0,
    v144270398.period_type_id          AS pka_glcalendarsperiodtypeid0,
    v35416688.customer_trx_line_id1    AS pka_transactionlinecustomertr0,
    v35416688.customer_trx_id2         AS pka_transactionheaderdistcust0,
    v35416688.batch_source_seq_id3     AS pka_transactionbatchsourcebat0,
    v35416688.customer_trx_line_id5    AS pka_transactionlinelinkcustom0,
    v102674681.party_site_id           AS pka_partysiteid0,
    v102674681.location_id1            AS pka_locationid0,
    v518783208.report_date             AS pka_reportdate0,
    v518783208.fiscal_period_set_name  AS pka_fiscalperiodsetname0,
    v518783208.fiscal_period_type      AS pka_fiscalperiodtype0,
    v518783208.period_set_id           AS pka_glcalendarsperiodsetid1,
    v518783208.period_type_id          AS pka_glcalendarsperiodtypeid1,
    v220364535.effective_start_date651 AS pka_businessunitdatefrom0,
    v220364535.effective_end_date8     AS pka_businessunitdateto0,
    v220364535.legal_entity_id1        AS pka_legalentitylegalentityid0,
    v220364535.organization_id         AS pka_organizationunittranslati0,
    v220364535.effective_start_date    AS pka_organizationunittranslati1,
    v220364535.effective_end_date      AS pka_organizationunittranslati2,
    v220364535.language                AS pka_organizationunittranslati3,
    v503360181.person_profile_id       AS pka_personprofileid0,
    v503360181.effective_start_date    AS pka_effectivestartdate0,
    v503360181.effective_end_date      AS pka_effectiveenddate0,
    v74173908.organization_profile_id  AS pka_organizationprofileid0,
    v74173908.effective_start_date     AS pka_effectivestartdate1,
    v74173908.effective_end_date       AS pka_effectiveenddate1,
    v157698628.cust_account_id         AS pka_custaccountid0,
    v400621628.s_g_0                   AS pka_s_g_00,
 CONCAT(
    COALESCE(v400621628.concat_values, ''),
    '-',
    COALESCE(v17715999.displayed_line_number, ''),
    '-',
    COALESCE(TRIM(REPLACE(v35416688.trx_number, CHR(9), '')), ''),
    '-',
    COALESCE(v144270398.user_period_set_name, ''),
    '-',
    COALESCE(CASE 
        WHEN v35416688.line_type = 'LINE' THEN TO_VARCHAR(v35416688.line_number) 
        WHEN v35416688.line_type = 'TAX' THEN TO_VARCHAR(v35416688.line_number3) 
        WHEN v35416688.line_type = 'FREIGHT' THEN 
            CASE  
                WHEN v35416688.link_to_cust_trx_line_id IS NULL THEN TO_VARCHAR(v35416688.line_number) 
                ELSE TO_VARCHAR(v35416688.line_number3) 
            END  
        ELSE '' 
    END, ''),
    '-',
    COALESCE(v35416688.line_type, ''),
    '-',
    COALESCE(v35416688.trx_line_id, '')
) AS ID ,
     GREATEST(NVL(v498378102.last_update_date,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.last_update_date1,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.last_update_date2,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DDHH24:MI:SS')),NVL(v35416688.last_update_date3,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')))  as INCREMENTAL_LATEST_DATE

FROM
    (
SELECT 
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_AE_HEADER_ID as ae_header_id,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_REF_AE_HEADER_ID as ref_ae_header_id,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_SOURCE_DISTRIBUTION_ID_NUM_1 as source_distribution_id_num_1,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_SOURCE_DISTRIBUTION_TYPE as source_distribution_type,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_TEMP_LINE_NUM as temp_line_num,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_UNROUNDED_ACCOUNTED_CR as unrounded_accounted_cr,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_UNROUNDED_ACCOUNTED_DR as unrounded_accounted_dr,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_UNROUNDED_ENTERED_CR as unrounded_entered_cr,
            xladistlink.JOURNAL_ENTRY_DISTRIBUTION_UNROUNDED_ENTERED_DR as unrounded_entered_dr,
            xlalines.JOURNAL_ENTRY_LINE_ACCOUNTING_CLASS_CODE as accounting_class_code,
            xlalines.JOURNAL_ENTRY_LINE_ACCOUNTING_DATE as accounting_date,
            xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID AS ae_header_id1,
            xlalines.JOURNAL_ENTRY_LINE_AE_LINE_NUM  AS ae_line_num1,
            xlalines.JOURNAL_ENTRY_LINE_CODE_COMBINATION_ID as code_combination_id,
            xlalines.JOURNAL_ENTRY_LINE_DISPLAYED_LINE_NUMBER as displayed_line_number,
            xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID as ledger_id,
            xlahdr.JOURNAL_ENTRY_HEADER_AE_HEADER_ID   AS ae_header_id2,
            xlahdr.JOURNAL_ENTRY_HEADER_DESCRIPTION    AS description1,
            xlahdr.JOURNAL_ENTRY_HEADER_JE_CATEGORY_NAME as je_category_name,
            ( 'N' )               AS adjustment_period_flag,
            ledgers.LEDGER_CHART_OF_ACCOUNTS_ID as chart_of_accounts_id
        FROM
            {{source('CES_ERP_FSCM_FINEXTRACT_XLABICCEXTRACT','SUBLEDGER_JOURNAL_DISTRIBUTION_EXTRACT_PVO')}} xladistlink, --xla_distribution_links
            {{source('CES_ERP_FSCM_FINEXTRACT_XLABICCEXTRACT','SUBLEDGER_JOURNAL_LINE_EXTRACT_PVO')}}           xlalines, -- xla_ae_lines
            {{source('CES_ERP_FSCM_FINEXTRACT_XLABICCEXTRACT','SUBLEDGER_JOURNAL_HEADER_EXTRACT_PVO')}}         xlahdr,  -- xla_ae_headers
            {{source('CES_ERP_FSCM_FINEXTRACT_GLBICCEXTRACT','LEDGER_EXTRACT_PVO')}}             ledgers   --gl_ledgers
        WHERE
            ( xladistlink.JOURNAL_ENTRY_DISTRIBUTION_AE_HEADER_ID = xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID
              AND xladistlink.JOURNAL_ENTRY_DISTRIBUTION_AE_LINE_NUM = xlalines.JOURNAL_ENTRY_LINE_AE_LINE_NUM
              AND xladistlink.JOURNAL_ENTRY_DISTRIBUTION_APPLICATION_ID = xlalines.JOURNAL_ENTRY_LINE_APPLICATION_ID
              AND xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID = xlahdr.JOURNAL_ENTRY_HEADER_AE_HEADER_ID
              AND xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID = ledgers.LEDGER_LEDGER_ID )
              AND ( ( ( xlalines.JOURNAL_ENTRY_LINE_CODE_COMBINATION_ID <> - 1 ) ) )

    ) v17715999,
    (
        SELECT
            lookuppeo.LOOKUP_CODE as lookup_code,
            lookuppeo.LOOKUP_TYPE as lookup_type,
            lookuppeo.MEANING as meaning
        FROM
            {{source('CES_ERP_FSCM_FINXLASHAREDOBJ','ACCOUNTING_CLASS_CODE_LOOKUP_PVO')}} lookuppeo  --xla_lookups
        WHERE
            ( ( lookuppeo.LOOKUP_TYPE = 'XLA_ACCOUNTING_CLASS' ) )

    ) v382875220,
    (
         SELECT 
            fiscalday.FISCAL_PERIOD_ADJUSTMENT_PERIOD_FLAG as adjustment_period_flag,
            fiscalday.FISCAL_PERIOD_NUMBER as fiscal_period_number,
            fiscalday.FISCAL_PERIOD_SET_NAME as fiscal_period_set_name,
            fiscalday.FISCAL_PERIOD_TYPE as fiscal_period_type,
            fiscalday.FISCAL_YEAR_NUMBER as fiscal_year_number,
            fiscalday.REPORT_DATE as report_date,
            fiscalday.LEDGER_ID as ledger_id,
            fiscalday.GL_CALENDARS_USER_PERIOD_SET_NAME as user_period_set_name,
            fiscalday.GL_CALENDARS_CALENDAR_ID as calendar_id,
            fiscalday.GL_CALENDARS_PERIOD_SET_ID as period_set_id,
            fiscalday.GL_CALENDARS_PERIOD_TYPE_ID as period_type_id
        FROM
            {{source('CES_ERP_FSCM_FINGLCALACC','FISCAL_DAY_PVO')}} fiscalday
            
    ) v144270398,
    (
    
        SELECT
            ( 'N' )                                    AS adjustmentperiodflag,
            ( 'RA_CUST_TRX_LINE_GL_DIST_ALL' )         AS sourcedistributiontype,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_ACCOUNT_CLASS as account_class,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_ACCTD_AMOUNT as acctd_amount,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_AMOUNT as amount,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CODE_COMBINATION_ID as code_combination_id,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CUST_TRX_LINE_GL_DIST_ID as cust_trx_line_gl_dist_id,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CUSTOMER_TRX_ID as customer_trx_id,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_GL_DATE as gl_date,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_LATEST_REC_FLAG as latest_rec_flag,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_ORG_ID         AS org_id240,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_SET_OF_BOOKS_ID as set_of_books_id,
            transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_LAST_UPDATE_DATE AS last_update_date1,
            transactionline.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID       AS customer_trx_line_id1,
            transactionline.RA_CUSTOMER_TRX_LINE_DESCRIPTION                AS description281,
            transactionline.RA_CUSTOMER_TRX_LINE_LINE_NUMBER AS line_number,
            transactionline.RA_CUSTOMER_TRX_LINE_LINE_TYPE AS line_type,
            transactionline.RA_CUSTOMER_TRX_LINE_LINK_TO_CUST_TRX_LINE_ID AS link_to_cust_trx_line_id,
            transactionline.RA_CUSTOMER_TRX_LINE_LAST_UPDATE_DATE    AS last_update_date2,
            transactionheaderdist.RA_CUSTOMER_TRX_BILL_TO_CUSTOMER_ID as bill_to_customer_id,
            transactionheaderdist.RA_CUSTOMER_TRX_BILL_TO_SITE_USE_ID as bill_to_site_use_id,
            transactionheaderdist.RA_CUSTOMER_TRX_CUST_TRX_TYPE_SEQ_ID as cust_trx_type_seq_id,
            transactionheaderdist.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID      AS customer_trx_id2,
            transactionheaderdist.RA_CUSTOMER_TRX_INVOICE_CURRENCY_CODE as invoice_currency_code,
            transactionheaderdist.RA_CUSTOMER_TRX_LAST_UPDATE_DATE     AS last_update_date3,
            transactionheaderdist.RA_CUSTOMER_TRX_SET_OF_BOOKS_ID      AS set_of_books_id2,
            transactionheaderdist.RA_CUSTOMER_TRX_TERM_DUE_DATE as term_due_date,
            transactionheaderdist.RA_CUSTOMER_TRX_TERM_ID as term_id,
            transactionheaderdist.RA_CUSTOMER_TRX_TRX_DATE as trx_date,
            transactionheaderdist.RA_CUSTOMER_TRX_TRX_NUMBER as trx_number,
            transactionbatchsource.RA_BATCH_SOURCE_BATCH_SOURCE_SEQ_ID AS batch_source_seq_id3,
            transactionbatchsource.RA_BATCH_SOURCE_NAME                AS name3,
            custacct.PARTY_ID as party_id,
            ( DATE(transactionheaderdist.RA_CUSTOMER_TRX_TRX_DATE) )  AS trx_date_only,
            ledgers.LEDGER_CHART_OF_ACCOUNTS_ID as chart_of_accounts_id,
            transactionlinelink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID   AS customer_trx_line_id5,
            transactionlinelink.RA_CUSTOMER_TRX_LINE_LINE_NUMBER            AS line_number3,
            (case  
                when transactionline.RA_CUSTOMER_TRX_LINE_LINE_TYPE = 'LINE' then transactionline.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID
                when transactionline.RA_CUSTOMER_TRX_LINE_LINE_TYPE = 'TAX' then transactionlinelink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID 
                when transactionline.RA_CUSTOMER_TRX_LINE_LINE_TYPE = 'FREIGHT' then (case when transactionline.RA_CUSTOMER_TRX_LINE_LINK_TO_CUST_TRX_LINE_ID is null then transactionline.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID else transactionlinelink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID end)  
             else 0 end) as trx_line_id
             
        FROM
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_DISTRIBUTION_EXTRACT_PVO')}} transactionlinedistribution, --ra_cust_trx_line_gl_dist_all
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_LINE_EXTRACT_PVO')}}    transactionline,
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_HEADER_EXTRACT_PVO')}}          transactionheaderdist,   --ra_customer_trx_all
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_BATCH_SOURCE_EXTRACT_PVO')}}         transactionbatchsource,  --ra_batch_sources_all
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_LINE_EXTRACT_PVO')}}     transactionlinelink,
            {{source('CES_ERP_FSCM_PARTIESANALYTICS','CUSTOMER_ACCOUNT')}}             custacct,   --hz_cust_accounts
            {{source('CES_ERP_FSCM_FINEXTRACT_GLBICCEXTRACT','LEDGER_EXTRACT_PVO')}}                   ledgers,   --gl_ledgers
            {{source('CES_ERP_FSCM_FINEXTRACT_FUNBICCEXTRACT','BUSINESS_UNIT_EXTRACT_PVO')}}                businessunit   --fun_bu_perf_v  
        WHERE
             ( 
                transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CUSTOMER_TRX_LINE_ID = transactionline.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID (+)
                AND transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CUSTOMER_TRX_ID = transactionheaderdist.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID
                AND transactionheaderdist.RA_CUSTOMER_TRX_BATCH_SOURCE_SEQ_ID = transactionbatchsource.RA_BATCH_SOURCE_BATCH_SOURCE_SEQ_ID (+)
                AND transactionline.RA_CUSTOMER_TRX_LINE_LINK_TO_CUST_TRX_LINE_ID = transactionlinelink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID (+)
                AND transactionheaderdist.RA_CUSTOMER_TRX_BILL_TO_CUSTOMER_ID = custacct.CUST_ACCOUNT_ID (+)
                AND transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_SET_OF_BOOKS_ID = ledgers.LEDGER_LEDGER_ID
                AND transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_ORG_ID = businessunit.FUN_BU_PERF_PEOBUSINESS_UNIT_ID 
                AND upper(transactionheaderdist.RA_CUSTOMER_TRX_TRX_CLASS) <> upper('BR')
                )
                --and transactionheaderdist.RA_CUSTOMER_TRX_TRX_NUMBER='866025'

    ) v35416688,
    (
        SELECT
            paymentterm.RA_TERM_BTERM_ID as term_id
        FROM
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','PAYMENT_TERM_EXTRACT_PVO')}} paymentterm  --ra_terms_b

    ) v404380990,
    (
 SELECT
            customeraccountsiteusepeo.LOCATION as location,
            customeraccountsiteusepeo.SITE_USE_ID as site_use_id,
            customeraccountsiteusepeo.STATUS as status,
            customeraccountsiteusepeo.PARTY_SITE_ID as party_site_id,
            customeraccountsiteusepeo.PARTY_SITE_NAME as party_site_name,
            customeraccountsiteusepeo.PARTY_SITE_NUMBER as party_site_number,
            customeraccountsiteusepeo.ADDRESS_1 as address1,
            customeraccountsiteusepeo.ADDRESS_2 as address2,
            customeraccountsiteusepeo.LOCATION_ID AS location_id1,
            customeraccountsiteusepeo.CITY as city,
            customeraccountsiteusepeo.COUNTRY as country,
            customeraccountsiteusepeo.DESCRIPTION as description,
            customeraccountsiteusepeo.POSTAL_CODE as postal_code,
            customeraccountsiteusepeo.STATE as state
        FROM
            {{source('CES_ERP_FSCM_PARTIESANALYTICS','CUST_ACCOUNT_SITE_USE')}}  customeraccountsiteusepeo --hz_cust_site_uses_all

    ) v102674681,
    (
        SELECT 
            fiscalday.FISCAL_PERIOD_ADJUSTMENT_PERIOD_FLAG as adjustment_period_flag,
            fiscalday.FISCAL_PERIOD_NAME as fiscal_period_name,
            fiscalday.FISCAL_PERIOD_NUMBER as fiscal_period_number,
            fiscalday.FISCAL_PERIOD_SET_NAME as fiscal_period_set_name,
            fiscalday.FISCAL_PERIOD_TYPE as fiscal_period_type,
            fiscalday.FISCAL_QUARTER_NUMBER as fiscal_quarter_number,
            fiscalday.FISCAL_YEAR_NUMBER as fiscal_year_number,
            date(fiscalday.REPORT_DATE) as report_date,
            fiscalday.LEDGER_ID as ledger_id,
            fiscalday.GL_CALENDARS_CALENDAR_ID as calendar_id,
            fiscalday.GL_CALENDARS_PERIOD_SET_ID as period_set_id,
            fiscalday.GL_CALENDARS_PERIOD_TYPE_ID as period_type_id
        FROM
            {{source('CES_ERP_FSCM_FINGLCALACC','FISCAL_DAY_PVO')}} fiscalday

    ) v518783208,
    (
    
        SELECT
            organizationunit.ORGANIZATION_UNIT_PEOORGANIZATION_ID      AS organization_id1,
            organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE AS effective_start_date651,
            organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE   AS effective_end_date8,
            legalentity.LEGAL_ENTITY_LEGAL_ENTITY_ID           AS legal_entity_id1,
            legalentity.LEGAL_ENTITY_NAME                      AS name2,
            organizationunittranslation.ORGANIZATION_UNIT_TRANSLATION_PEONAME as name, -- ORGANIZATION_UNIT_TRANSLATION_PEOCREATION_DATE
            organizationunittranslation.ORGANIZATION_ID as organization_id,   -- organizationunit.ORGANIZATION_UNIT_PEOORGANIZATION_ID
            organizationunittranslation.EFFECTIVE_START_DATE as effective_start_date, --ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE
            organizationunittranslation.EFFECTIVE_END_DATE as effective_end_date, --ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE
            organizationunittranslation.LANGUAGE as language   -- ORGANIZATION_UNIT_TRANSLATION_PEOSOURCE_LANG
        FROM
            {{source('CES_ERP_FSCM_ORGANIZATION','ORGANIZATION_UNIT_TRANSLATION_PVO')}}   organizationunit,  --hr_all_organization_units_f
            {{source('CES_ERP_FSCM_FINFUNBUSINESSUNITS','BUSINESS_UNIT_PVO')}} organizationinformation,  --hr_organization_information_f
            {{source('CES_ERP_FSCM_FINEXTRACT_XLEBICCEXTRACT','LEGAL_ENTITY_EXTRACT_PVO')}}           legalentity,   --xle_entity_profiles
            {{source('CES_ERP_FSCM_ORGANIZATION','ORGANIZATION_UNIT_TRANSLATION_PVO')}}    organizationunittranslation  -- hr_organization_units_f_tl
        WHERE
              organizationunit.ORGANIZATION_UNIT_PEOORGANIZATION_ID = organizationinformation.BUSINESS_UNIT_ID AND 
            ( 'FUN_BUSINESS_UNIT' ) = organizationinformation.ORG_UNIT_CLASSIFICATION_CLASSIFICATION_CODE 
            AND organizationinformation.BUSINESS_UNIT_LEGAL_ENTITY_ID = legalentity.LEGAL_ENTITY_LEGAL_ENTITY_ID (+)
            AND organizationunit.ORGANIZATION_UNIT_PEOORGANIZATION_ID = organizationunittranslation.ORGANIZATION_ID
            AND organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE = organizationunittranslation.EFFECTIVE_START_DATE
            AND organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE = organizationunittranslation.EFFECTIVE_END_DATE
            AND ( DATE(SYSDATE()) BETWEEN organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE AND organizationunit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE )
            AND ( DATE(SYSDATE())BETWEEN organizationinformation.ORGANIZATION_INFORMATION_EFFECTIVE_START_DATE AND organizationinformation.ORGANIZATION_INFORMATION_EFFECTIVE_END_DATE )
            AND ( DATE(SYSDATE()) BETWEEN organizationunittranslation.EFFECTIVE_START_DATE AND organizationunittranslation.EFFECTIVE_END_DATE )
            
    ) v220364535,
    (
        SELECT 
            ledger.LEDGER_LAST_UPDATE_DATE as last_update_date,
            ledger.LEDGER_LEDGER_CATEGORY_CODE as ledger_category_code,
            ledger.LEDGER_LEDGER_ID as ledger_id,
            ledger.LEDGER_NAME AS name496
        FROM
            {{source('CES_ERP_FSCM_FINEXTRACT_GLBICCEXTRACT','LEDGER_EXTRACT_PVO')}}  ledger  --gl_ledgers
        WHERE
            ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) )

    ) v498378102,
    (
    
        SELECT 
            customerpartypeo.PARTY_ID,
            customerpartypeo.PARTY_NAME,
            customerpartypeo.PARTY_NUMBER,
            customerpartypeo.PARTY_TYPE,
            ( decode(customerpartypeo.PARTY_TYPE, 'PERSON', customerpartypeo.PARTY_ID, 0) ) AS per_party_id
        FROM
            CES_ERP.BIP_FSCM_PROD_HZ.HZ_PARTIES customerpartypeo
            
--            
--        SELECT 
--            customerpartypeo.PARTY_PARTY_ID as party_id,
--            customerpartypeo.PARTY_PARTY_NAME as party_name,
--            customerpartypeo.PARTY_PARTY_NUMBER as party_number,
--            customerpartypeo.PARTY_PARTY_TYPE as party_type,
--            ( decode(customerpartypeo.PARTY_PARTY_TYPE, 'PERSON', customerpartypeo.PARTY_PARTY_ID, 0) ) AS per_party_id
--        FROM
--            {{source('CES_ERP_FSCM_PRCPOZPUBLICVIEW','SUPPLIER_PVO')}} customerpartypeo  --hz_parties

    ) v405133203,
    (
        SELECT
            personpeo.EFFECTIVE_END_DATE as effective_end_date,
            personpeo.EFFECTIVE_START_DATE as effective_start_date,
            personpeo.PARTY_ID as party_id,
            personpeo.PERSON_FIRST_NAME as person_first_name,
            personpeo.PERSON_LAST_NAME as person_last_name,
            personpeo.PERSON_PROFILE_ID as person_profile_id
        FROM
            {{source('CES_ERP_FSCM_PARTIESANALYTICS','PERSON')}} personpeo --hz_person_profiles
        WHERE
            ( DATE(SYSDATE()) BETWEEN personpeo.EFFECTIVE_START_DATE AND personpeo.EFFECTIVE_END_DATE )
            AND ( personpeo.EFFECTIVE_LATEST_CHANGE = 'Y' ) 

    ) v503360181,
    (
        SELECT 
            organizationpeo.EFFECTIVE_END_DATE as effective_end_date,
            organizationpeo.EFFECTIVE_START_DATE as effective_start_date,
            organizationpeo.ORGANIZATION_NAME as organization_name,
            organizationpeo.ORGANIZATION_PROFILE_ID as organization_profile_id,
            organizationpeo.PARTY_ID as party_id
        FROM
            {{source('CES_ERP_FSCM_PARTIESANALYTICS','ORGANIZATION')}} organizationpeo --hz_organization_profiles
        WHERE
            ( DATE(SYSDATE()) BETWEEN organizationpeo.EFFECTIVE_START_DATE AND organizationpeo.EFFECTIVE_END_DATE )
            AND ( organizationpeo.EFFECTIVE_LATEST_CHANGE = 'Y' )

    ) v74173908,
    (
        SELECT
            transactiontype.RA_CUST_TRX_TYPE_CUST_TRX_TYPE_SEQ_ID as cust_trx_type_seq_id,
            transactiontype.RA_CUST_TRX_TYPE_DESCRIPTION as description,
            transactiontype.RA_CUST_TRX_TYPE_NAME as name,
            transactiontype.RA_CUST_TRX_TYPE_TYPE as type
        FROM
            {{source('CES_ERP_FSCM_FINEXTRACT_ARBICCEXTRACT','TRANSACTION_TYPE_EXTRACT_PVO')}} transactiontype  --ra_cust_trx_types_all
            
    ) v62756817,
    (
        SELECT DISTINCT
            fndcaldayeo.REPORT_DATE as report_date,     --column that flows into query, and left join here
            fndcaldayeo.LAST_UPDATE_DATE as last_update_date,
            fndcaldayeo.LAST_UPDATED_BY as last_updated_by,
            fndcaldayeo.CREATION_DATE as creation_date,
            fndcaldayeo.CREATED_BY as created_by,
            fndcaldayeo.LAST_UPDATE_LOGIN as last_update_login,
            fndcaldayeo.PARENT_MONTH as parent_month,  --column that flows into query
            fndcaldayeo.DAY_OF_YEAR as day_of_year       --column that flows into query   
        FROM
            {{source('CES_ERP_FSCM_FINGLCALACC','FISCAL_DAY_PVO')}} fndcaldayeo  --fnd_cal_day   -- NO MATCH, so using table GL_FISCAL_DAY_V            
    ) v132021520,
    (
        SELECT
            customeraccountpeo.CUST_ACCOUNT_ID,
            customeraccountpeo.ACCOUNT_NAME,
            customeraccountpeo.ACCOUNT_NUMBER,
            customeraccountpeo.CUSTOMER_CLASS_CODE,
            customeraccountpeo.CUSTOMER_TYPE,
            customeraccountpeo.STATUS
        FROM
            {{source('CES_ERP_FSCM_PARTIESANALYTICS','CUSTOMER_ACCOUNT')}} customeraccountpeo
        WHERE
            ( 1 = 1 )
    ) v157698628,
    (
        SELECT /*+ qb_name(AccountBIVO) */
            biflexfieldeo.CODE_COMBINATION_CODE_COMBINATION_ID                                                      AS s_g_0,
            biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID                                                     AS s_g_1,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 3001,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, NULL) )                          AS fa_cost_ctr_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Cost Center Scheme Ledger', 25063, 'Cost Center Scheme TAS Ledger',
                     18063, 'Cost Center Scheme VIC Ledger', 23063, 'Cost Center Scheme VIC NOP Ledger', 3001,
                     'Cost Center Service Co Ledger', 7037, 'Cost Center WARRRL WA Ledger', NULL) )         AS fa_cost_ctr_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 3001,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, NULL) )                          AS gl_account_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Account Scheme Ledger', 25063, 'Account Scheme TAS Ledger',
                     18063, 'Account Scheme VIC Ledger', 23063, 'Account Scheme VIC NOP Ledger', 3001,
                     'Account Service Co Ledger', 7037, 'Account WARRRL WA Ledger', NULL) )                 AS gl_account_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 3001,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, NULL) )                          AS gl_balancing_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 25063, 'Company Scheme TAS Ledger',
                     18063, 'Company Scheme VIC Ledger', 23063, 'Company Scheme VIC NOP Ledger', 3001,
                     'Company Service Co Ledger', 7037, 'Company WARRRL WA Ledger', NULL) )                 AS gl_balancing_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 3001,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_5, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_5, NULL) )                          AS gl_intercompany_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 25063, 'Intercompany Scheme TAS Ledger',
                     18063, 'Company Scheme VIC Ledger', 23063, 'Company Scheme VIC NOP Ledger', 3001,
                     'Company Service Co Ledger', 7037, 'Company WARRRL WA Ledger', NULL) )                 AS gl_intercompany_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, 7037,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL) )                                                        AS gl_mat_type_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Mat Type Scheme Ledger', 25063, 'Mat Type Scheme TAS Ledger',
                     18063, 'Mat Type Scheme VIC Ledger', 23063, 'Mat Type Scheme VIC NOP Ledger', 7037,
                     'Mat Type WARRRL WA Ledger', NULL) )                                                   AS gl_mat_type_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, biflexfieldeo.CODE_COMBINATION_SEGMENT_2, NULL) )     AS gl_scheme_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 3001, 'Scheme Service Co Ledger', NULL) ) AS gl_scheme_c,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6,
                     18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 3001,
                     biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL) )                          AS gl_spare_,
            ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Spare Scheme Ledger', 25063, 'Spare Scheme TAS Ledger',
                     18063, 'Spare Scheme VIC Ledger', 23063, 'Spare Scheme VIC NOP Ledger', 3001,
                     'Spare Service Co Ledger', 7037, 'Spare WARRRL WA Ledger', NULL) )                     AS gl_spare_c,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_2,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_5,
            biflexfieldeo.CODE_COMBINATION_SEGMENT_6,
            concat(biflexfieldeo.CODE_COMBINATION_SEGMENT_1,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_2,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_3,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_4,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_5,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_6) as concat_seg
        FROM
            {{source('CES_ERP_FREQ_FSCM_FINEXTRACT_GLBICCEXTRACT','CODE_COMBINATION_EXTRACT_PVO')}} biflexfieldeo
    ) v208318186,
( SELECT /*+ qb_name(AccountBIVO) */
    biflexfieldeo.CODE_COMBINATION_CODE_COMBINATION_ID                                                 AS s_g_0,
    biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID                                                AS s_g_1,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
             18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 3001,
             biflexfieldeo.CODE_COMBINATION_SEGMENT_3, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_3, NULL) )                  AS fa_cost_ctr_,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Cost Center Scheme Ledger', 25063, 'Cost Center Scheme TAS Ledger',
             18063, 'Cost Center Scheme VIC Ledger', 23063, 'Cost Center Scheme VIC NOP Ledger', 3001,
             'Cost Center Service Co Ledger', 7037, 'Cost Center WARRRL WA Ledger', NULL) ) AS fa_cost_ctr_c,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
             18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 3001,
             biflexfieldeo.CODE_COMBINATION_SEGMENT_4, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_4, NULL) )                  AS gl_account_,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Account Scheme Ledger', 25063, 'Account Scheme TAS Ledger',
             18063, 'Account Scheme VIC Ledger', 23063, 'Account Scheme VIC NOP Ledger', 3001,
             'Account Service Co Ledger', 7037, 'Account WARRRL WA Ledger', NULL) )         AS gl_account_c,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
             18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 3001,
             biflexfieldeo.CODE_COMBINATION_SEGMENT_1, 7037, biflexfieldeo.CODE_COMBINATION_SEGMENT_1, NULL) )                  AS gl_balancing_,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, 'Company Scheme Ledger', 25063, 'Company Scheme TAS Ledger',
             18063, 'Company Scheme VIC Ledger', 23063, 'Company Scheme VIC NOP Ledger', 3001,
             'Company Service Co Ledger', 7037, 'Company WARRRL WA Ledger', NULL) )         AS gl_balancing_c,
    ( decode(biflexfieldeo.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID, 2001, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                       || '-'
                                                       || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                       || '-'
                                                       || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                       || '-'
                                                       || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                       || '-'
                                                       || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                       || '-'
                                                       || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 25063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
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
             18063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                    || '-'
                    || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                    || '-'
                    || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                    || '-'
                    || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                    || '-'
                    || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                    || '-'
                    || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 23063, biflexfieldeo.CODE_COMBINATION_SEGMENT_1
                                                      || '-'
                                                      || biflexfieldeo.CODE_COMBINATION_SEGMENT_2
                                                      || '-'
                                                      || biflexfieldeo.CODE_COMBINATION_SEGMENT_3
                                                      || '-'
                                                      || biflexfieldeo.CODE_COMBINATION_SEGMENT_4
                                                      || '-'
                                                      || biflexfieldeo.CODE_COMBINATION_SEGMENT_5
                                                      || '-'
                                                      || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 3001,
             biflexfieldeo.CODE_COMBINATION_SEGMENT_1
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
                                              || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL) ) AS concat_values,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_1,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_2,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_3,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_4,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_5,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_6
FROM
    {{source('CES_ERP_FREQ_FSCM_FINEXTRACT_GLBICCEXTRACT','CODE_COMBINATION_EXTRACT_PVO')}} biflexfieldeo
    ) V400621628
WHERE 1=1 
AND V17715999.ACCOUNTING_CLASS_CODE = V382875220.LOOKUP_CODE(+) 
AND V17715999.ACCOUNTING_DATE = V144270398.REPORT_DATE(+) 
AND V17715999.ADJUSTMENT_PERIOD_FLAG = V144270398.ADJUSTMENT_PERIOD_FLAG(+) 
AND V17715999.LEDGER_ID = V144270398.LEDGER_ID(+) 
AND V17715999.SOURCE_DISTRIBUTION_ID_NUM_1(+) = V35416688.CUST_TRX_LINE_GL_DIST_ID 
AND V17715999.SOURCE_DISTRIBUTION_TYPE(+) = V35416688.SourceDistributionType 
AND V35416688.TERM_ID = V404380990.TERM_ID(+) 
AND V35416688.BILL_TO_SITE_USE_ID = V102674681.SITE_USE_ID(+) 
AND V35416688.TRX_DATE_ONLY = V518783208.REPORT_DATE 
AND V35416688.AdjustmentPeriodFlag = V518783208.ADJUSTMENT_PERIOD_FLAG 
AND V35416688.SET_OF_BOOKS_ID2 = V518783208.LEDGER_ID 
AND V35416688.ORG_ID240 = V220364535.ORGANIZATION_ID1 
AND V35416688.SET_OF_BOOKS_ID = V498378102.LEDGER_ID 
AND V35416688.PARTY_ID = V405133203.PARTY_ID(+)
AND V405133203.PER_PARTY_ID = V503360181.PARTY_ID(+) 
AND V405133203.PARTY_ID = V74173908.PARTY_ID(+) 
AND V35416688.CUST_TRX_TYPE_SEQ_ID = V62756817.CUST_TRX_TYPE_SEQ_ID 
AND V35416688.GL_DATE = V132021520.REPORT_DATE(+) 
AND V35416688.BILL_TO_CUSTOMER_ID = V157698628.CUST_ACCOUNT_ID(+) 
AND V17715999.CODE_COMBINATION_ID = V208318186.s_g_0(+) 
AND V17715999.CHART_OF_ACCOUNTS_ID = V208318186.s_g_1(+)  
AND V35416688.CODE_COMBINATION_ID = V400621628.s_g_0 
AND V35416688.CHART_OF_ACCOUNTS_ID = V400621628.s_g_1 
AND ( ( NOT ( (V62756817.TYPE = 'DEP' ) ) )  
AND ( NOT ( (V62756817.TYPE = 'GUAR' ) ) )  
AND ( NOT ( (V62756817.TYPE = 'PMT' ) ) )  
AND ( NOT ( (V35416688.CUST_TRX_LINE_GL_DIST_ID IS NULL ) ) )

 /*
AND 
(
( NOT ( ( 
 GREATEST(NVL(v498378102.last_update_date,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.last_update_date1,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.last_update_date2,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.last_update_date3,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) 
    nvl(v498378102.last_update_date, to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
)
*/    


AND NOT ( (V144270398.USER_PERIOD_SET_NAME IS NULL ) )
)

     ) T6235986
            
where  ( nvl(nullif(trim(T6235986.C149936038) ,'') , 'Y') = 'Y' ) ),
SAWITH1 AS (select  /*+ inline */  
     D1.ID,
	 D1.c9 as c1,
     D1.c10 as c2,
     D1.c11 as c3,
     D1.c12 as c4,
     D1.c13 as c5,
     D1.c14 as c6,
     D1.c15 as c7,
     D1.c16 as c8,
     D1.c17 as c9,
     D1.c18 as c10,
     D1.c19 as c11,
     D1.c20 as c12,
     D1.c21 as c13,
     D1.c22 as c14,
     D1.c23 as c15,
     D1.c24 as c16,
     D1.c25 as c17,
     D1.c26 as c18,
     D1.c27 as c19,
     D1.c28 as c20,
     D1.c29 as c21,
     D1.c30 as c22,
     D1.c31 as c23,
     D1.c32 as c24,
     D1.c33 as c25,
     D1.c34 as c26,
     D1.c35 as c27,
     D1.c37 as c28,
     D1.c39 as c29,
     D1.c40 as c30,
     D1.c41 as c31,
     D1.c42 as c32,
     D1.c43 as c33,
     D1.c44 as c34,
     D1.c45 as c35,
     D1.c46 as c36,
     D1.c47 as c37,
     D1.c48 as c38,
     D1.c49 as c39,
     D1.c50 as c40,
     D1.c51 as c41,
     D1.c52 as c42,
     D1.c53 as c43,
     D1.c54 as c44,
     D1.c55 as c45,
     D1.c56 as c46,
     D1.c59 as c47,
     D1.c60 as c48,
     D1.c62 as c49,
     D1.c64 as c50,
     D1.c65 as c51,
     D1.c67 as c52,
     D1.c68 as c53,
     D1.c69 as c54,
     D1.c70 as c55,
     D1.c72 as c56,
     D1.c73 as c57,
     D1.c74 as c58,
     D1.c75 as c59,
     D1.c77 as c60,
     D1.c78 as c61,
     D1.c83 as c62,
     D1.c86 as c63,
     D1.c89 as c64,
     D1.c91 as c65,
     D1.c92 as c66,
     D1.c93 as c67,
     D1.c94 as c68,
     D1.c95 as c69,
     D1.c96 as c70,
     D1.c97 as c71,
     D1.c98 as c72,
     D1.c99 as c73,
     D1.c100 as c74,
     D1.c102 as c75,
     D1.c107 as c107,
     sum(D1.c90) as c76,
     sum(D1.c101) as c77,
     sum(D1.c103) as c78,
     sum(D1.c104) as c79,
     sum(D1.c105) as c80,
     sum(D1.c106) as c81
     ,D1.INCREMENTAL_LATEST_DATE
from 
     SAWITH0 D1
group by D1.ID,D1.c9, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c15, D1.c16, D1.c17, D1.c18, D1.c19, D1.c20, D1.c21, D1.c22, D1.c23, D1.c24, D1.c25, D1.c26, D1.c27, 
D1.c28, D1.c29, D1.c30, D1.c31, D1.c32, D1.c33, D1.c34, D1.c35, D1.c37, D1.c39, D1.c40, D1.c41, D1.c42, D1.c43, D1.c44, D1.c45, D1.c46, D1.c47, D1.c48, D1.c49, 
D1.c50, D1.c51, D1.c52, D1.c53, D1.c54, D1.c55, D1.c56, D1.c59, D1.c60, D1.c62, D1.c64, D1.c65, D1.c67, D1.c68, D1.c69, D1.c70, D1.c72, D1.c73, D1.c74, D1.c75, 
D1.c77, D1.c78, D1.c83, D1.c86, D1.c89, D1.c91, D1.c92, D1.c93, D1.c94, D1.c95, D1.c96, D1.c97, D1.c98, D1.c99, D1.c100, D1.c102, D1.c107,D1.INCREMENTAL_LATEST_DATE),
SAWITH2 AS (select  /*+ inline */  
     sum(D1.c76) as c1,
     sum(D1.c77) as c2,
     sum(case  when D1.c75 = 'REC' then D1.c76 else 0 end ) as c3,
     sum(case  when D1.c75 = 'REC' then D1.c77 else 0 end ) as c4,
     sum(D1.c78) as c5,
     sum(D1.c79) as c6,
     sum(D1.c80) as c7,
     sum(D1.c81) as c8,
     D1.ID,
	 D1.c1 as c9,
     D1.c2 as c10,
     D1.c3 as c11,
     D1.c4 as c12,
     D1.c5 as c13,
     D1.c6 as c14,
     D1.c7 as c15,
     D1.c8 as c16,
     D1.c9 as c17,
     D1.c10 as c18,
     D1.c11 as c19,
     D1.c12 as c20,
     D1.c13 as c21,
     D1.c14 as c22,
     D1.c15 as c23,
     D1.c16 as c24,
     D1.c17 as c25,
     D1.c18 as c26,
     D1.c19 as c27,
     D1.c20 as c28,
     D1.c21 as c29,
     D1.c22 as c30,
     D1.c23 as c31,
     D1.c24 as c32,
     D1.c25 as c33,
     D1.c26 as c34,
     D1.c27 as c35,
     D1.c28 as c37,
     D1.c29 as c39,
     D1.c30 as c40,
     D1.c31 as c41,
     D1.c32 as c42,
     D1.c33 as c43,
     D1.c34 as c44,
     D1.c35 as c45,
     D1.c36 as c46,
     D1.c37 as c47,
     D1.c38 as c48,
     D1.c39 as c49,
     D1.c40 as c50,
     D1.c41 as c51,
     D1.c42 as c52,
     D1.c43 as c53,
     D1.c44 as c54,
     D1.c45 as c55,
     D1.c46 as c56,
     D1.c47 as c59,
     D1.c48 as c60,
     --cast(D1.c73 as  VARCHAR ( 100 ) ) as c61,
     to_varchar(D1.c73) as c61,
     D1.c49 as c62,
     case  when D1.c54 > D1.c74 then D1.c54 else D1.c74 end  as c63,
     D1.c50 as c64,
     D1.c51 as c65,
     D1.c52 as c67,
     D1.c53 as c68,
     D1.c54 as c69,
     D1.c55 as c70,
     case  when D1.c56 = 'LINE' then D1.c65 when D1.c56 = 'TAX' then D1.c66 when D1.c56 = 'FREIGHT' then case  when D1.c67 is null then D1.c65 else D1.c66 end  else NULL end  as c71,
     D1.c56 as c72,
     D1.c57 as c73,
     D1.c58 as c74,
     D1.c59 as c75,
     --cast(D1.c72 as  VARCHAR ( 10 ) ) as c76,
     to_varchar(D1.c72) as c76,
     D1.c60 as c77,
     D1.c61 as c78,
     D1.c68 * 1000 + D1.c69 as c79,
     D1.c44 * 10000 + D1.c70 * 100000000 + D1.c71 * 100 + D1.c42 as c80,
     D1.c70 * 10000 + D1.c44 as c81,
     D1.c62 as c83,
     D1.c63 as c86,
     D1.c64 as c89,
     D1.c107 as c107
     ,D1.INCREMENTAL_LATEST_DATE
from 
     SAWITH1 D1
group by D1.c44 * 10000 + D1.c70 * 100000000 + D1.c71 * 100 + D1.c42, D1.c68 * 1000 + D1.c69, D1.c70 * 10000 + D1.c44, D1.ID, D1.c1, D1.c2, D1.c3, D1.c4, D1.c5, D1.c6, D1.c7, 
D1.c8, D1.c9, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c15, D1.c16, D1.c17, D1.c18, D1.c19, D1.c20, D1.c21, D1.c22, D1.c23, D1.c24, D1.c25, D1.c26, D1.c27, D1.c28, 
D1.c29, D1.c30, D1.c31, D1.c32, D1.c33, D1.c34, D1.c35, D1.c36, D1.c37, D1.c38, D1.c39, D1.c40, D1.c41, D1.c42, D1.c43, D1.c44, D1.c45, D1.c46, D1.c47, D1.c48, D1.c49, 
D1.c50, D1.c51, D1.c52, D1.c53, D1.c54, D1.c55, D1.c56, D1.c57, D1.c58, D1.c59, D1.c60, D1.c61, D1.c62, D1.c63,D1.INCREMENTAL_LATEST_DATE, D1.c64, case  when D1.c54 > D1.c74 then D1.c54 else D1.c74 end , 
case  when D1.c56 = 'LINE' then D1.c65 when D1.c56 = 'TAX' then D1.c66 when D1.c56 = 'FREIGHT' then case  when D1.c67 is null then D1.c65 else D1.c66 end  else NULL end , 
to_varchar(D1.c72), 
to_varchar(D1.c73),
--cast(D1.c72 as  VARCHAR ( 10 ) ), 
--cast(D1.c73 as  VARCHAR ( 100 ) ),
D1.c107 ),
SAWITH3 AS (select  /*+ inline */  T6235987.C389230568 as c1,
     T6235987.C164164071 as c2
from 
     ( SELECT
    v72673585.MEANING             AS c389230568,
    v72673585.LOOKUP_CODE         AS c164164071,
    v72673585.LANGUAGE            AS c343259318,
    v72673585.LOOKUP_TYPE         AS c417433804,
    v72673585.VIEW_APPLICATION_ID AS c456636657,
    v72673585.SET_ID              AS c497682495
FROM
    {{source('CES_ERP_FREQ_FSCM_ANALYTICSSERVICE','LOOKUP_VALUES_TLPVO')}} v72673585
WHERE
    ( ( ( v72673585.LOOKUP_TYPE = 'HZ_STATUS' ) )
      AND ( ( v72673585.VIEW_APPLICATION_ID = 0 ) )
      AND ( ( v72673585.SET_ID = 0 ) )
      AND ( ( v72673585.LANGUAGE = 'US' ) ) )
) T6235987),
SAWITH4 AS (select  /*+ inline */  T6235988.C389230568 as c1,
     T6235988.C164164071 as c2
from 
     ( SELECT
    v72673585.MEANING             AS c389230568,
    v72673585.LOOKUP_CODE         AS c164164071,
    v72673585.LANGUAGE            AS c343259318,
    v72673585.LOOKUP_TYPE         AS c417433804,
    v72673585.VIEW_APPLICATION_ID AS c456636657,
    v72673585.SET_ID              AS c497682495
FROM
    {{source('CES_ERP_FREQ_FSCM_ANALYTICSSERVICE','LOOKUP_VALUES_TLPVO')}} v72673585
WHERE
    ( ( ( v72673585.LOOKUP_TYPE = 'CUSTOMER_TYPE' ) )
      AND ( ( v72673585.VIEW_APPLICATION_ID = 0 ) )
      AND ( ( v72673585.SET_ID = 0 ) )
      AND ( ( v72673585.LANGUAGE = 'US' ) ) )
) T6235988),
SAWITH5 AS (select  /*+ inline */  T6235989.C482842991 as c1,
     T6235989.C93940995 as c2,
     T6235989.C179792223 as c3
from 
     ( SELECT
    v153658366.RA_TERM_TLDESCRIPTION AS c482842991,
    v153658366.RA_TERM_TLTERM_ID     AS c93940995,
    v153658366.RA_TERM_TLNAME        AS c179792223,
    v153658366.RA_TERM_TLLANGUAGE    AS c392122403
FROM
    CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.PAYMENT_TERM_TLEXTRACT_PVO v153658366
WHERE
    ( ( ( v153658366.RA_TERM_TLLANGUAGE = 'US' ) ) )
) T6235989),
SAWITH6 AS (select  T6235990.C389230568 as c1,
     T6235990.C164164071 as c2
from 
     ( SELECT
    v72673585.MEANING             AS c389230568,
    v72673585.LOOKUP_CODE         AS c164164071,
    v72673585.LANGUAGE            AS c343259318,
    v72673585.LOOKUP_TYPE         AS c417433804,
    v72673585.VIEW_APPLICATION_ID AS c456636657,
    v72673585.SET_ID              AS c497682495
FROM
    {{source('CES_ERP_FREQ_FSCM_ANALYTICSSERVICE','LOOKUP_VALUES_TLPVO')}} v72673585
WHERE
    ( ( ( v72673585.LOOKUP_TYPE = 'GL_ASF_LEDGER_CATEGORY' ) )
      AND ( ( v72673585.VIEW_APPLICATION_ID = 101 ) )
      AND ( ( v72673585.SET_ID = 0 ) )
      AND ( ( v72673585.LANGUAGE = 'US' ) ) )
) T6235990),
SAWITH7 AS (select  /*+ inline */  
	D1.ID,
	D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c4 as c4,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     D1.c9 as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D1.c17 as c17,
     D1.c18 as c18,
     D1.c19 as c19,
     D1.c20 as c20,
     D1.c21 as c21,
     D1.c22 as c22,
     D1.c23 as c23,
     D1.c24 as c24,
     D1.c25 as c25,
     D1.c26 as c26,
     D1.c27 as c27,
     D1.c28 as c28,
     D1.c29 as c29,
     D1.c30 as c30,
     D1.c31 as c31,
     D1.c32 as c32,
     D1.c33 as c33,
     D1.c34 as c34,
     D1.c35 as c35,
     D2.c1 as c36,
     D1.c83 as c37,
     D1.c37 as c38,
     D3.c1 as c39,
     D1.c39 as c40,
     D1.c40 as c41,
     D1.c41 as c42,
     D1.c42 as c43,
     D1.c43 as c44,
     D1.c44 as c45,
     D1.c45 as c46,
     D1.c46 as c47,
     D1.c47 as c48,
     D1.c48 as c49,
     D1.c49 as c50,
     D1.c50 as c51,
     D1.c51 as c52,
     D1.c52 as c53,
     D1.c53 as c54,
     D1.c54 as c55,
     D1.c55 as c56,
     D1.c56 as c57,
     D4.c1 as c58,
     D1.c86 as c59,
     D4.c3 as c60,
     D1.c59 as c61,
     D1.c60 as c62,
     D1.c61 as c63,
     D1.c62 as c64,
     D1.c63 as c65,
     D1.c64 as c66,
     D1.c65 as c67,
     D5.c1 as c68,
     D1.c89 as c69,
     D1.c67 as c70,
     D1.c68 as c71,
     D1.c69 as c72,
     D1.c70 as c73,
     D1.c71 as c74,
     D1.c72 as c75,
     D1.c73 as c76,
     D1.c74 as c77,
     D1.c75 as c78,
     D1.c76 as c79,
     D1.c77 as c80,
     D1.c78 as c81,
     D1.c79 as c82,
     D1.c80 as c83,
     D1.c81 as c84,
     D1.c107 as c107
     ,D1.INCREMENTAL_LATEST_DATE
from 
     (
          (
               (
                    
                         SAWITH2 D1 left outer join SAWITH3 D2 On D1.c83 = D2.c2) left outer join SAWITH4 D3 On D1.c37 = D3.c2) left outer join 
                         SAWITH5 D4 On D1.c86 = D4.c2) left outer join SAWITH6 D5 On D1.c89 = D5.c2),
    sawith8 AS (
    SELECT
        D1.ID,
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
        d1.c69 AS c69,
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
        d1.c82 AS c82,
        d1.c83 AS c83,
        d1.c84 AS c84,
        d1.c107 AS c107
        ,D1.INCREMENTAL_LATEST_DATE
    FROM
        (
            SELECT
				d901.ID,
				d901.c9                 AS c1,
                d901.c10                AS c2,
                d901.c11                AS c3,
                d901.c12                AS c4,
                d901.c13                AS c5,
                d901.c14                AS c6,
                d901.c15                AS c7,
                d901.c16                AS c8,
                d901.c17                AS c9,
                d901.c18                AS c10,
                d901.c19                AS c11,
                d901.c20                AS c12,
                d901.c21                AS c13,
                d901.c22                AS c14,
                d901.c23                AS c15,
                d901.c24                AS c16,
                d901.c25                AS c17,
                d901.c26                AS c18,
                d901.c27                AS c19,
                d901.c28                AS c20,
                d901.c29                AS c21,
                d901.c30                AS c22,
                d901.c31                AS c23,
                d901.c32                AS c24,
                d901.c33                AS c25,
                d901.c34                AS c26,
                d901.c35                AS c27,
                nvl(d901.c36, d901.c37) AS c28,
                d901.c38                AS c29,
                nvl(d901.c39, d901.c38) AS c30,
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
                d901.c8                 AS c48,
                d901.c7                 AS c49,
                d901.c6                 AS c50,
                d901.c5                 AS c51,
                d901.c57                AS c52,
                d901.c4                 AS c53,
                d901.c3                 AS c54,
                d901.c2                 AS c55,
                d901.c1                 AS c56,
                nvl(d901.c58, to_char(d901.c59)) AS c57, --SF change to char
                nvl(d901.c60, to_char(d901.c59)) AS c58,  --SF change to char
                d901.c61                AS c59,
                d901.c62                AS c60,
                d901.c63                AS c61,
                d901.c64                AS c62,
                d901.c65                AS c63,
                d901.c66                AS c64,
                d901.c67                AS c65,
                nvl(d901.c68, d901.c69) AS c66,
                d901.c70                AS c67,
                d901.c71                AS c68,
                d901.c72                AS c69,
                d901.c73                AS c70,
                d901.c74                AS c71,
                d901.c75                AS c72,
                d901.c76                AS c73,
                d901.c77                AS c74,
                d901.c78                AS c75,
                d901.c79                AS c76,
                d901.c80                AS c77,
                d901.c81                AS c78,
                d901.c82                AS c79,
                d901.c83                AS c80,
                d901.c84                AS c81,
                d901.c37                AS c82,
                d901.c59                AS c83,
                d901.c69                AS c84,
                d901.c107               AS c107,
                d901.INCREMENTAL_LATEST_DATE,
                ROW_NUMBER()
                OVER(PARTITION BY d901.ID, d901.c9, d901.c10, d901.c11, d901.c12, d901.c13,
                                  d901.c14, d901.c15, d901.c16, d901.c17, d901.c18,
                                  d901.c19, d901.c20, d901.c21, d901.c22, d901.c23,
                                  d901.c24, d901.c25, d901.c26, d901.c27, d901.c28,
                                  d901.c29, d901.c30, d901.c31, d901.c32, d901.c33,
                                  d901.c34, d901.c35, d901.c37, d901.c38, d901.c40,
                                  d901.c41, d901.c42, d901.c43, d901.c44, d901.c45,
                                  d901.c46, d901.c47, d901.c48, d901.c49, d901.c50,
                                  d901.c51, d901.c52, d901.c53, d901.c54, d901.c55,
                                  d901.c56, d901.c57, d901.c59, d901.c61, d901.c62,
                                  d901.c63, d901.c64, d901.c65, d901.c66, d901.c67,
                                  d901.c69, d901.c70, d901.c71, d901.c72, d901.c73,
                                  d901.c74, d901.c75, d901.c76, d901.c77, d901.c78,
                                  d901.c79, d901.c80, d901.c81, d901.c82, d901.c83,
                                  d901.c84, d901.c107,d901.INCREMENTAL_LATEST_DATE
                     ORDER BY
                         d901.ID ASC, d901.INCREMENTAL_LATEST_DATE ASC, d901.c9 ASC, d901.c10 ASC, d901.c11 ASC, d901.c12 ASC, d901.c13 ASC,
                         d901.c14 ASC, d901.c15 ASC, d901.c16 ASC, d901.c17 ASC, d901.c18 ASC,
                         d901.c19 ASC, d901.c20 ASC, d901.c21 ASC, d901.c22 ASC, d901.c23 ASC,
                         d901.c24 ASC, d901.c25 ASC, d901.c26 ASC, d901.c27 ASC, d901.c28 ASC,
                         d901.c29 ASC, d901.c30 ASC, d901.c31 ASC, d901.c32 ASC, d901.c33 ASC,
                         d901.c34 ASC, d901.c35 ASC, d901.c37 ASC, d901.c38 ASC, d901.c40 ASC,
                         d901.c41 ASC, d901.c42 ASC, d901.c43 ASC, d901.c44 ASC, d901.c45 ASC,
                         d901.c46 ASC, d901.c47 ASC, d901.c48 ASC, d901.c49 ASC, d901.c50 ASC,
                         d901.c51 ASC, d901.c52 ASC, d901.c53 ASC, d901.c54 ASC, d901.c55 ASC,
                         d901.c56 ASC, d901.c57 ASC, d901.c59 ASC, d901.c61 ASC, d901.c62 ASC,
                         d901.c63 ASC, d901.c64 ASC, d901.c65 ASC, d901.c66 ASC, d901.c67 ASC,
                         d901.c69 ASC, d901.c70 ASC, d901.c71 ASC, d901.c72 ASC, d901.c73 ASC,
                         d901.c74 ASC, d901.c75 ASC, d901.c76 ASC, d901.c77 ASC, d901.c78 ASC,
                         d901.c79 ASC, d901.c80 ASC, d901.c81 ASC, d901.c82 ASC, d901.c83 ASC,
                         d901.c84 ASC, d901.c107 ASC
                )                       AS c85
            FROM
                sawith7 d901
        ) d1
    WHERE
        ( d1.c85 = 1 )
)
SELECT
  d1.ID,
  NVL(d1.c1,'NA') AS CONCATENATED_SEGMENTS,
  d1.c2 AS ENTITY_CODE,
  d1.c3 AS COST_CENTER_CODE,
  d1.c4 AS ACCOUNT_CODE,
  d1.c5 AS INTERCOMPANY_CODE,
  d1.c6 AS MATERIAL_TYPE_CODE,
  d1.c7 AS SCHEME_CODE,
  d1.c8 AS PROJECT_CODE,
  d1.c9 AS DIST_CONCATENATED_SEGMENTS,
  d1.c10 AS DIST_ENTITY_CODE,
  d1.c11 AS DIST_COST_CENTER_CODE,
  d1.c12 AS DIST_ACCOUNT_CODE,
  d1.c13 AS BILL_TO_CUST_NAME,
  d1.c14 AS BILL_TO_CUST_NUM,
  d1.c15 AS BILL_TO_CUST_TYPE,
  d1.c16 AS BILL_TO_CUST_ORG_NAME,
  d1.c17 AS BILL_TO_CUST_PER_FIRST_NAME,
  d1.c18 AS BILL_TO_CUST_PER_LAST_NAME,
  d1.c19 AS ACCOUNTING_DATE,
  d1.c20 AS PAYMENT_TERMS_DUE_DATE,
  d1.c21 AS ACCOUNTING_CLASS,
  d1.c22 AS JOURNAL_CATEGORY_NAME,
  d1.c23 AS JOURNAL_DESC,
  NVL(d1.c24,0) AS JOURNAL_LINE_NUM,
  d1.c25 AS BILL_TO_CUST_ACCOUNT_CLASS_CODE,
  d1.c26 AS BILL_TO_CUST_ACCOUNT_DESC,
  d1.c27 AS BILL_TO_CUST_ACCOUNT_NUM,
  d1.c28 AS BILL_TO_ACCOUNT_STATUS,
  d1.c29 AS BILL_TO_ACCOUNT_TYPE_CODE,
  d1.c30 AS BILL_TO_ACCOUNT_TYPE,
  d1.c31 AS BILL_TO_SITE_CITY,
  d1.c32 AS BILL_TO_SITE_COUNTRY,
  d1.c33 AS BILL_TO_SITE_DESC,
  d1.c34 AS BILL_TO_SITE_NAME,
  d1.c35 AS BILL_TO_SITE_NUM,
  d1.c36 AS BILL_TO_SITE_POSTAL_CODE,
  d1.c37 AS BILL_TO_SITE_STATE,
  d1.c38 AS BILL_TO_SITE_STATUS,
  d1.c39 AS BILL_TO_SITE_ADDR1,
  d1.c40 AS BILL_TO_SITE_ADDR2,
  d1.c41 AS BILL_TO_SITE,
  d1.c42 AS BU_NAME,
  d1.c43 AS LEGAL_ENTITY_NAME,
  d1.c44 AS FISCAL_PERIOD_NUM,
  d1.c45 AS FISCAL_PERIOD,
  d1.c46 AS FISCAL_YEAR_NUM,
  d1.c47 AS LEDGER_NAME,
  d1.c48 AS ACCOUNTED_AMT_CR,
  d1.c49 AS ACCOUNTED_AMT_DR,
  d1.c50 AS ENTERED_AMT_CR,
  d1.c51 AS ENTERED_AMT_DR,
  d1.c52 AS CURRENCY,
  d1.c53 AS TRANSACTION_ACCOUNTED_AMT,
  d1.c54 AS TRANSACTION_ENTERED_AMT,
  d1.c55 AS DIST_ACCOUNTED_AMT,
  d1.c56 AS DIST_ENTERED_AMT,
  d1.c57 AS PAYMENT_TERM_DESC,
  d1.c58 AS PAYMENT_TERM_NAME,
  d1.c59 AS TRANSACTION_TYPE_DESC,
  d1.c60 AS TRANSACTION_TYPE,
  d1.c61 AS TRANSACTION_ID,
  d1.c62 AS TRANSACTION_NUMBER,
  to_char(d1.c63,'YYYY-MM-DD HH24:MI:SS') AS LAST_UPDATE_DATE,
  d1.c64 AS TRANSACTION_SOURCE_NAME,
  d1.c65 AS ACCOUNT_CODE_COMBIN_DESC,
  d1.c66 AS LEDGER_CATEGORY_NAME,
  d1.c67 AS TRANSACTION_DATE,
  d1.c68 AS ACCOUNTING_CALENDAR_NAME,
  d1.c70 AS LINE_DESC,
  NVL(d1.c71,0) AS TXN_LINE_NUM,
  NVL(d1.c72,'NA') AS LINE_TYPE_CODE,
  NVL(d1.c107,0) AS LINE_TXN_ID,
  d1.INCREMENTAL_LATEST_DATE
from ( select 
	D1.ID,
	D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c4 as c4,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     D1.c9 as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D1.c17 as c17,
     D1.c18 as c18,
     D1.c19 as c19,
     D1.c20 as c20,
     D1.c21 as c21,
     D1.c22 as c22,
     D1.c23 as c23,
     D1.c24 as c24,
     D1.c25 as c25,
     D1.c26 as c26,
     D1.c27 as c27,
     D1.c28 as c28,
     D1.c29 as c29,
     D1.c30 as c30,
     D1.c31 as c31,
     D1.c32 as c32,
     D1.c33 as c33,
     D1.c34 as c34,
     D1.c35 as c35,
     D1.c36 as c36,
     D1.c37 as c37,
     D1.c38 as c38,
     D1.c39 as c39,
     D1.c40 as c40,
     D1.c41 as c41,
     D1.c42 as c42,
     D1.c43 as c43,
     D1.c44 as c44,
     D1.c45 as c45,
     D1.c46 as c46,
     D1.c47 as c47,
     D1.c48 as c48,
     D1.c49 as c49,
     D1.c50 as c50,
     D1.c51 as c51,
     D1.c52 as c52,
     D1.c53 as c53,
     D1.c54 as c54,
     D1.c55 as c55,
     D1.c56 as c56,
     D1.c57 as c57,
     D1.c58 as c58,
     D1.c59 as c59,
     D1.c60 as c60,
     D1.c61 as c61,
     D1.c62 as c62,
     GREATEST(D1.c69, D1.c63) as c63,
     D1.c64 as c64,
     D1.c65 as c65,
     D1.c66 as c66,
     D1.c67 as c67,
     D1.c68 as c68,
     D1.c69 as c69,
     D1.c70 as c70,
     D1.c71 as c71,
     D1.c72 as c72,
     D1.c107 as c107,
    D1.INCREMENTAL_LATEST_DATE
from 
     SAWITH8 D1 ) D1