

SELECT COUNT(*) FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS

;

SELECT 

CONCAT(DIST_CONCATENATED_SEGMENTS, JOURNAL_LINE_NUM, TRANSACTION_NUMBER, ACCOUNTING_CALENDAR_NAME) AS ID,
CONCATENATED_SEGMENTS,
ENTITY_CODE,
COST_CENTER_CODE,
ACCOUNT_CODE,
INTERCOMPANY_CODE,
MATERIAL_TYPE_CODE,
SCHEME_CODE,
PROJECT_CODE,
DIST_CONCATENATED_SEGMENTS,
DIST_ENTITY_CODE,
DIST_COST_CENTER_CODE,
DIST_ACCOUNT_CODE,
BILL_TO_CUST_NAME,
BILL_TO_CUST_NUM,
BILL_TO_CUST_TYPE,
BILL_TO_CUST_ORG_NAME,
BILL_TO_CUST_PER_FIRST_NAME,
BILL_TO_CUST_PER_LAST_NAME,
ACCOUNTING_DATE,
PAYMENT_TERMS_DUE_DATE,
ACCOUNTING_CLASS,
JOURNAL_CATEGORY_NAME,
JOURNAL_DESC,
JOURNAL_LINE_NUM,
BILL_TO_CUST_ACCOUNT_CLASS_CODE,
BILL_TO_CUST_ACCOUNT_DESC,
BILL_TO_CUST_ACCOUNT_NUM,
BILL_TO_ACCOUNT_STATUS,
BILL_TO_ACCOUNT_TYPE_CODE,
BILL_TO_ACCOUNT_TYPE,
BILL_TO_SITE_CITY,
BILL_TO_SITE_COUNTRY,
BILL_TO_SITE_DESC,
BILL_TO_SITE_NAME,
BILL_TO_SITE_NUM,
BILL_TO_SITE_POSTAL_CODE,
BILL_TO_SITE_STATE,
BILL_TO_SITE_STATUS,
BILL_TO_SITE_ADDR1,
BILL_TO_SITE_ADDR2,
BILL_TO_SITE,
BU_NAME,
LEGAL_ENTITY_NAME,
FISCAL_PERIOD_NUM,
FISCAL_PERIOD,
FISCAL_YEAR_NUM,
LEDGER_NAME,
ACCOUNTED_AMT_CR,
ACCOUNTED_AMT_DR,
ENTERED_AMT_CR,
ENTERED_AMT_DR,
CURRENCY,
TRANSACTION_ACCOUNTED_AMT,
TRANSACTION_ENTERED_AMT,
DIST_ACCOUNTED_AMT,
DIST_ENTERED_AMT,
PAYMENT_TERM_DESC,
PAYMENT_TERM_NAME,
TRANSACTION_TYPE_DESC,
TRANSACTION_TYPE,
TRANSACTION_ID,
TRANSACTION_NUMBER,
LAST_UPDATE_DATE,
TRANSACTION_SOURCE_NAME,
ACCOUNT_CODE_COMBIN_DESC,
LEDGER_CATEGORY_NAME,
TRANSACTION_DATE,
ACCOUNTING_CALENDAR_NAME,
LINE_DESC

FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS
WHERE DATE(TRANSACTION_DATE) = '2023-01-05'

;


;
SELECT 

CONCAT(DIST_CONCATENATED_SEGMENTS, JOURNAL_LINE_NUM, TRANSACTION_NUMBER, ACCOUNTING_CALENDAR_NAME) AS ID,
CONCATENATED_SEGMENTS,
ENTITY_CODE,
COST_CENTER_CODE,
ACCOUNT_CODE,
INTERCOMPANY_CODE,
MATERIAL_TYPE_CODE,
SCHEME_CODE,
PROJECT_CODE,
DIST_CONCATENATED_SEGMENTS,
DIST_ENTITY_CODE,
DIST_COST_CENTER_CODE,
DIST_ACCOUNT_CODE,
BILL_TO_CUST_NAME,
BILL_TO_CUST_NUM,
BILL_TO_CUST_TYPE,
BILL_TO_CUST_ORG_NAME,
BILL_TO_CUST_PER_FIRST_NAME,
BILL_TO_CUST_PER_LAST_NAME,
ACCOUNTING_DATE,
PAYMENT_TERMS_DUE_DATE,
ACCOUNTING_CLASS,
JOURNAL_CATEGORY_NAME,
JOURNAL_DESC,
JOURNAL_LINE_NUM,
BILL_TO_CUST_ACCOUNT_CLASS_CODE,
BILL_TO_CUST_ACCOUNT_DESC,
BILL_TO_CUST_ACCOUNT_NUM,
BILL_TO_ACCOUNT_STATUS,
BILL_TO_ACCOUNT_TYPE_CODE,
BILL_TO_ACCOUNT_TYPE,
BILL_TO_SITE_CITY,
BILL_TO_SITE_COUNTRY,
BILL_TO_SITE_DESC,
BILL_TO_SITE_NAME,
BILL_TO_SITE_NUM,
BILL_TO_SITE_POSTAL_CODE,
BILL_TO_SITE_STATE,
BILL_TO_SITE_STATUS,
BILL_TO_SITE_ADDR1,
BILL_TO_SITE_ADDR2,
BILL_TO_SITE,
BU_NAME,
LEGAL_ENTITY_NAME,
FISCAL_PERIOD_NUM,
FISCAL_PERIOD,
FISCAL_YEAR_NUM,
LEDGER_NAME,
ACCOUNTED_AMT_CR,
ACCOUNTED_AMT_DR,
ENTERED_AMT_CR,
ENTERED_AMT_DR,
CURRENCY,
TRANSACTION_ACCOUNTED_AMT,
TRANSACTION_ENTERED_AMT,
DIST_ACCOUNTED_AMT,
DIST_ENTERED_AMT,
PAYMENT_TERM_DESC,
PAYMENT_TERM_NAME,
TRANSACTION_TYPE_DESC,
TRANSACTION_TYPE,
TRANSACTION_ID,
TRANSACTION_NUMBER,
LAST_UPDATE_DATE,
TRANSACTION_SOURCE_NAME,
ACCOUNT_CODE_COMBIN_DESC,
LEDGER_CATEGORY_NAME,
TRANSACTION_DATE,
ACCOUNTING_CALENDAR_NAME
--LINE_DESC

FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST
WHERE DATE(TRANSACTION_DATE) = '2023-01-05' 
order by CONCAT(DIST_CONCATENATED_SEGMENTS, JOURNAL_LINE_NUM, TRANSACTION_NUMBER, ACCOUNTING_CALENDAR_NAME) asc
;

SELECT  count(*) FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST 
;
ALTER WAREHOUSE ANALYTICS_CES SET WAREHOUSE_SIZE = 'Medium'
ALTER WAREHOUSE ANALYTICS_CES SET WAREHOUSE_SIZE = 'Small'
ALTER WAREHOUSE ANALYTICS_CES SET WAREHOUSE_SIZE = 'X-Small'


;

SELECT 
FROM  RAW_DEV.TEST.AR_TRANSACTIONS_TEST
WHERE DATE(TRANSACTION_DATE) >= '2023-01-01' AND DATE(TRANSACTION_DATE) <= '2024-07-01'


RAW_PROD.CES_ERP.AP_TRANSACTIONS
;
     /* NEW VERSION */

--9508-98-1400-51005-0000-00000


CREATE OR REPLACE TEMPORARY TABLE RAW_DEV.TEST.AR_TRANSACTIONS_TEST AS 

     WITH 
SAWITH0 AS (select T5783149.C406671630 as c9,
     T5783149.C233929166 as c10,
     T5783149.C31199523 as c11,
     T5783149.C7207427 as c12,
     T5783149.C433723576 as c13,
     T5783149.C417910867 as c14,
     T5783149.C276323548 as c15,
     T5783149.C420124855 as c16,
     T5783149.C33224554 as c17,
     T5783149.C187863249 as c18,
     T5783149.C301161417 as c19,
     T5783149.C203685335 as c20,
     T5783149.C527751494 as c21,
     T5783149.C369898840 as c22,
     T5783149.C157895813 as c23,
     T5783149.C308010007 as c24,
     T5783149.C184077456 as c25,
     T5783149.C153253872 as c26,
     T5783149.C465216818 as c27,
     T5783149.C161366140 as c28,
     T5783149.C441856931 as c29,
     T5783149.C196380727 as c30,
     T5783149.C196342575 as c31,
     T5783149.C163592185 as c32,
     T5783149.C519818457 as c33,
     T5783149.C422183274 as c34,
     T5783149.C167968178 as c35,
     T5783149.C238687615 as c37,
     T5783149.C3973386 as c39,
     T5783149.C517362176 as c40,
     T5783149.C126400656 as c41,
     T5783149.C214686596 as c42,
     T5783149.C364048903 as c43,
     T5783149.C533631721 as c44,
     T5783149.C129307616 as c45,
     T5783149.C38618484 as c46,
     T5783149.C146557964 as c47,
     T5783149.C330636722 as c48,
     T5783149.C372857905 as c49,
     T5783149.C51067786 as c50,
     T5783149.C57234163 as c51,
     T5783149.C105329137 as c52,
     T5783149.C64190816 as c53,
     T5783149.C197594221 as c54,
     T5783149.C10223035 as c55,
     T5783149.C311905569 as c56,
     T5783149.C5576208 as c59,
     T5783149.C342081810 as c60,
     T5783149.C64203304 as c62,
     T5783149.C420216650 as c63,
     T5783149.C215348972 as c64,
     T5783149.C359046800 as c65,
     T5783149.C461354760 as c67,
     T5783149.C230180794 as c68,
     T5783149.C307898839 as c69,
     T5783149.C331908290 as c70,
     T5783149.C356728513 as c71,
     T5783149.C379200345 as c72,
     T5783149.C109800147 as c74,
     T5783149.C15439104 as c75,
     T5783149.C168826660 as c80,
     T5783149.C478163046 as c83,
     T5783149.C148776435 as c86,
     T5783149.C9590699 as c87,
     T5783149.C281146953 as c88,
     T5783149.C100662310 as c89,
     T5783149.C75721789 as c90,
     T5783149.C130160118 as c91,
     T5783149.C183425870 as c92,
     T5783149.C22888611 as c93,
     T5783149.C402304897 as c94,
     T5783149.C525092876 as c95,
     T5783149.C465528056 as c96,
     T5783149.C377811519 as c97,
     T5783149.C298986491 as c98,
     T5783149.C485080380 as c99
from 
     ( SELECT
    v208318186.concat_seg       AS c406671630,  -- v292640615.concatenated_seg
    v208318186.gl_balancing_           AS c233929166, --v470773127.value 
    v208318186.fa_cost_ctr_            AS c31199523,  --v343808451.value  
    v208318186.gl_account_             AS c7207427,  --v435856572.value 
    v208318186.gl_intercompany_        AS c433723576, --v34792864.value 
    v208318186.gl_mat_type_            AS c417910867,  --v412550833.value
    v208318186.gl_scheme_              AS c276323548,  --v278434713.value 
    v208318186.gl_spare_               AS c420124855,  -- v344303243.value
    v400621628.concat_values           AS c33224554,
    v400621628.gl_balancing_           AS c187863249,  --v392743570.value 
    v400621628.fa_cost_ctr_            AS c301161417,  --v155578885.value   
    v400621628.gl_account_             AS c203685335,  --v241607465.value    
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
    v35416688.invoice_currency_code    AS c311905569,
    v62756817.description              AS c5576208,
    v62756817.name                     AS c342081810,
    v35416688.trx_number               AS c64203304,
    v35416688.last_update_date3        AS c420216650,
    v35416688.name3                    AS c215348972,
    null                               AS c359046800,  --  v292640615.description128 d1.c65 AS ACCOUNT_CODE_COMBIN_DESC,
    v35416688.trx_date                 AS c461354760,
    v144270398.user_period_set_name    AS c230180794,
    v498378102.last_update_date        AS c307898839,
    v102674681.site_use_id             AS c331908290,
    v62756817.cust_trx_type_seq_id     AS c356728513,
    v220364535.organization_id1        AS c379200345,
    v17715999.code_combination_id    AS c109800147, 
    v498378102.ledger_id               AS c15439104,
    v157698628.status                  AS c168826660,
    v404380990.term_id                 AS c478163046,
    v498378102.ledger_category_code    AS c148776435,
    v35416688.amount                   AS c9590699,
    v132021520.parent_month            AS c281146953,
    v132021520.day_of_year             AS c100662310,
    v518783208.calendar_id             AS c75721789,
    v518783208.fiscal_quarter_number   AS c130160118,
    v405133203.party_id                AS c183425870,
    v35416688.customer_trx_id          AS c22888611,
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
    v35416688.customer_trx_id2         AS pka_transactionheaderdistcust0,
    v35416688.batch_source_seq_id3     AS pka_transactionbatchsourcebat0,
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
    v157698628.cust_account_id         AS pka_custaccountid0
FROM
       ( SELECT 
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
            FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_DISTRIBUTION_EXTRACT_PVO xladistlink, --xla_distribution_links
            FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_LINE_EXTRACT_PVO           xlalines, -- xla_ae_lines
            FSCM_PROD_FINEXTRACT_XLABICCEXTRACT.SUBLEDGER_JOURNAL_HEADER_EXTRACT_PVO         xlahdr,  -- xla_ae_headers
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO             ledgers   --gl_ledgers
        WHERE
            ( xladistlink.JOURNAL_ENTRY_DISTRIBUTION_AE_HEADER_ID = xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID
              AND xladistlink.JOURNAL_ENTRY_DISTRIBUTION_AE_LINE_NUM = xlalines.JOURNAL_ENTRY_LINE_AE_LINE_NUM
              AND xladistlink.JOURNAL_ENTRY_DISTRIBUTION_APPLICATION_ID = xlalines.JOURNAL_ENTRY_LINE_APPLICATION_ID
              AND xlalines.JOURNAL_ENTRY_LINE_AE_HEADER_ID = xlahdr.JOURNAL_ENTRY_HEADER_AE_HEADER_ID
              AND xlalines.JOURNAL_ENTRY_LINE_LEDGER_ID = ledgers.LEDGER_LEDGER_ID )
              AND ( ( ( xlalines.JOURNAL_ENTRY_LINE_CODE_COMBINATION_ID <> - 1 ) ) )
    ) v17715999,
    -- (
    --     SELECT 
    --         codecombination.CODE_COMBINATION_CHART_OF_ACCOUNTS_ID as chart_of_accounts_id,
    --         codecombination.CODE_COMBINATION_CODE_COMBINATION_ID as code_combination_id,
    --         ( fnd_flex_xml_publisher_apis.process_kff_combination_1('FLEXFIELD', 'GL', 'GL#', codecombination.chart_of_accounts_id, NULL,
    --                                                                 codecombination.code_combination_id, 'ALL', 'Y', 'VALUE') )            AS
    --                                                                 concatenated_seg,
    --         ( fnd_flex_xml_publisher_apis.process_kff_combination_1('FLEXFIELD', 'GL', 'GL#', codecombination.chart_of_accounts_id, NULL,
    --                                                                 codecombination.code_combination_id, 'ALL', 'Y', 'FULL_DESCRIPTION') ) AS
    --                                                                 description128
    --     FROM
    --         FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO codecombination  --gl_code_combinations
    -- ) v292640615,
    (
        SELECT
            lookuppeo.LOOKUP_CODE as lookup_code,
            lookuppeo.LOOKUP_TYPE as lookup_type,
            lookuppeo.MEANING as meaning
        FROM
            FSCM_PROD_FINXLASHAREDOBJ.ACCOUNTING_CLASS_CODE_LOOKUP_PVO lookuppeo  --xla_lookups
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
            ledger.LEDGER_LEDGER_ID as ledger_id,
            glcalendars.GL_CALENDARS_USER_PERIOD_SET_NAME as user_period_set_name,
            glcalendars.GL_CALENDARS_CALENDAR_ID as calendar_id,
            glcalendars.GL_CALENDARS_PERIOD_SET_ID as period_set_id,
            glcalendars.GL_CALENDARS_PERIOD_TYPE_ID as period_type_id
        FROM
            FSCM_PROD_FINGLCALACC.FISCAL_DAY_PVO fiscalday, --gl_fiscal_day_v
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO      ledger,     -- gl_ledgers
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.FISCAL_CALENDAR_EXTRACT_PVO    glcalendars   -- gl_calendars
        WHERE
                fiscalday.FISCAL_PERIOD_SET_NAME = ledger.LEDGER_PERIOD_SET_NAME
            AND fiscalday.FISCAL_PERIOD_TYPE = ledger.LEDGER_ACCOUNTED_PERIOD_TYPE
            AND fiscalday.FISCAL_PERIOD_SET_NAME = glcalendars.GL_CALENDARS_PERIOD_SET_NAME
            AND fiscalday.FISCAL_PERIOD_TYPE = glcalendars.GL_CALENDARS_PERIOD_TYPE
    ) v144270398,
    (

        SELECT
             CONCAT(( 'N' ),( DATE(transactionheaderdist.RA_CUSTOMER_TRX_TRX_DATE) ),transactionheaderdist.RA_CUSTOMER_TRX_SET_OF_BOOKS_ID ) ID,
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
            ledgers.LEDGER_CHART_OF_ACCOUNTS_ID as chart_of_accounts_id
        FROM
            FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_DISTRIBUTION_EXTRACT_PVO transactionlinedistribution, --ra_cust_trx_line_gl_dist_all
            FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_HEADER_EXTRACT_PVO          transactionheaderdist,   --ra_customer_trx_all
            FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_BATCH_SOURCE_EXTRACT_PVO         transactionbatchsource,  --ra_batch_sources_all
            FSCM_PROD_PARTIESANALYTICS.CUSTOMER_ACCOUNT             custacct,   --hz_cust_accounts
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO                   ledgers,   --gl_ledgers
           FSCM_PROD_FINEXTRACT_FUNBICCEXTRACT.BUSINESS_UNIT_EXTRACT_PVO                businessunit   --fun_bu_perf_v  
        WHERE
             ( transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_CUSTOMER_TRX_ID = transactionheaderdist.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID
                AND transactionheaderdist.RA_CUSTOMER_TRX_BATCH_SOURCE_SEQ_ID = transactionbatchsource.RA_BATCH_SOURCE_BATCH_SOURCE_SEQ_ID (+)
                AND transactionheaderdist.RA_CUSTOMER_TRX_BILL_TO_CUSTOMER_ID = custacct.CUST_ACCOUNT_ID (+)
                AND transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_SET_OF_BOOKS_ID = ledgers.LEDGER_LEDGER_ID
                AND transactionlinedistribution.RA_CUST_TRX_LINE_GL_DIST_ORG_ID = businessunit.FUN_BU_PERF_PEOBUSINESS_UNIT_ID 
                )
                
    ) v35416688,
    (
        SELECT
            paymentterm.RA_TERM_BTERM_ID as term_id
        FROM
            FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.PAYMENT_TERM_EXTRACT_PVO paymentterm  --ra_terms_b
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
            FSCM_PROD_PARTIESANALYTICS.CUST_ACCOUNT_SITE_USE  customeraccountsiteusepeo --hz_cust_site_uses_all

    ) v102674681,
    (
        

        
        SELECT 
          CONCAT(fiscalday.FISCAL_PERIOD_ADJUSTMENT_PERIOD_FLAG,date(fiscalday.REPORT_DATE),ledger.LEDGER_LEDGER_ID ) as ID,
            fiscalday.FISCAL_PERIOD_ADJUSTMENT_PERIOD_FLAG as adjustment_period_flag,
            fiscalday.FISCAL_PERIOD_NAME as fiscal_period_name,
            fiscalday.FISCAL_PERIOD_NUMBER as fiscal_period_number,
            fiscalday.FISCAL_PERIOD_SET_NAME as fiscal_period_set_name,
            fiscalday.FISCAL_PERIOD_TYPE as fiscal_period_type,
            fiscalday.FISCAL_QUARTER_NUMBER as fiscal_quarter_number,
            fiscalday.FISCAL_YEAR_NUMBER as fiscal_year_number,
            date(fiscalday.REPORT_DATE) as report_date,
            ledger.LEDGER_LEDGER_ID as ledger_id,
            glcalendars.GL_CALENDARS_CALENDAR_ID as calendar_id,
            glcalendars.GL_CALENDARS_PERIOD_SET_ID as period_set_id,
            glcalendars.GL_CALENDARS_PERIOD_TYPE_ID as period_type_id
        FROM
            FSCM_PROD_FINGLCALACC.FISCAL_DAY_PVO fiscalday, --gl_fiscal_day_v   --not in mapping list
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO      ledger,  -- gl_ledgers
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.FISCAL_CALENDAR_EXTRACT_PVO    glcalendars   -- gl_calendars
        WHERE
                fiscalday.FISCAL_PERIOD_SET_NAME = ledger.LEDGER_PERIOD_SET_NAME
            AND fiscalday.FISCAL_PERIOD_TYPE = ledger.LEDGER_ACCOUNTED_PERIOD_TYPE
            AND fiscalday.FISCAL_PERIOD_SET_NAME = glcalendars.GL_CALENDARS_PERIOD_SET_NAME
            AND fiscalday.FISCAL_PERIOD_TYPE = glcalendars.GL_CALENDARS_PERIOD_TYPE

    
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
            FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_TRANSLATION_PVO   organizationunit,  --hr_all_organization_units_f
            FSCM_PROD_FINFUNBUSINESSUNITS.BUSINESS_UNIT_PVO organizationinformation,  --hr_organization_information_f
            FSCM_PROD_FINEXTRACT_XLEBICCEXTRACT.LEGAL_ENTITY_EXTRACT_PVO           legalentity,   --xle_entity_profiles
            FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_TRANSLATION_PVO    organizationunittranslation  -- hr_organization_units_f_tl
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
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.LEDGER_EXTRACT_PVO  ledger  --gl_ledgers
        WHERE
            ( ( ledger.LEDGER_OBJECT_TYPE_CODE = 'L' ) )
    ) v498378102,
    (   /* in this query option with ParentPartyId is available but not specified */
        SELECT 
            customerpartypeo.PARTY_PARTY_ID as party_id,
            customerpartypeo.PARTY_PARTY_NAME as party_name,
            customerpartypeo.PARTY_PARTY_NUMBER as party_number,
            customerpartypeo.PARTY_PARTY_TYPE as party_type,
            ( decode(customerpartypeo.PARTY_PARTY_TYPE, 'PERSON', customerpartypeo.PARTY_PARTY_ID, 0) ) AS per_party_id
        FROM
            FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO customerpartypeo  --hz_parties
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
            FSCM_PROD_PARTIESANALYTICS.PERSON personpeo --hz_person_profiles
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
            FSCM_PROD_PARTIESANALYTICS.ORGANIZATION organizationpeo --hz_organization_profiles
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
            FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_TYPE_EXTRACT_PVO transactiontype  --ra_cust_trx_types_all
    ) v62756817,
    -- (
    --     SELECT
    --         currenciesbpeo.CURRENCY_CODE as currency_code,
    --         currenciesbpeo.DIGITAL_CURRENCY_CODE as digital_currency_code
    --     FROM
    --         FSCM_PROD_ANALYTICSSERVICE.CURRENCIES_PVO currenciesbpeo  --fnd_currencies_b
    -- ) v220341495,
    (   /*dummy pvo with matching columns to be able to run query without, to check if data given here is good */ 
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
            FSCM_PROD_FINGLCALACC.FISCAL_DAY_PVO fndcaldayeo  --fnd_cal_day   -- NO MATCH, so using table GL_FISCAL_DAY_V
    ) v132021520,
    (
        SELECT
            customeraccountpeo.CUST_ACCOUNT_ID as cust_account_id,
            customeraccountpeo.ACCOUNT_NAME as account_name,
            customeraccountpeo.ACCOUNT_NUMBER as account_number,
            customeraccountpeo.CUSTOMER_CLASS_CODE as customer_class_code,
            customeraccountpeo.CUSTOMER_TYPE as customer_type,
            customeraccountpeo.STATUS as status
        FROM
            FSCM_PROD_PARTIESANALYTICS.CUSTOMER_ACCOUNT customeraccountpeo  -- hz_cust_accounts

    ) v157698628,
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
    biflexfieldeo.CODE_COMBINATION_SEGMENT_1 AS segment1,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_2 AS segment2,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_3 AS segment3,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_4 AS segment4,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_5 AS segment5,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_6 AS segment6,
 concat(biflexfieldeo.CODE_COMBINATION_SEGMENT_1,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_2,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_3,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_4,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_5,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_6) as concat_seg
        FROM
            FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO biflexfieldeo  --gl_code_combinations
    ) v208318186,
    
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
                                                     || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, 7037,
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
            || biflexfieldeo.CODE_COMBINATION_SEGMENT_6, NULL)) AS concat_values,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_1 AS segment1,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_2 AS segment2,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_3 AS segment3,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_4 AS segment4,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_5 AS segment5,
    biflexfieldeo.CODE_COMBINATION_SEGMENT_6 AS segment6,
 concat(biflexfieldeo.CODE_COMBINATION_SEGMENT_1,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_2,'-', biflexfieldeo.CODE_COMBINATION_SEGMENT_3,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_4,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_5,'-',biflexfieldeo.CODE_COMBINATION_SEGMENT_6) as concat_seg
FROM
    FSCM_PROD_FINEXTRACT_GLBICCEXTRACT.CODE_COMBINATION_EXTRACT_PVO biflexfieldeo  -- gl_code_combinations  
) V400621628
WHERE
( 
v17715999.accounting_class_code = v382875220.lookup_code (+)
AND v17715999.accounting_date = v144270398.report_date (+)
AND v17715999.adjustment_period_flag = v144270398.adjustment_period_flag (+)
AND v17715999.ledger_id = v144270398.ledger_id (+)
AND v17715999.source_distribution_id_num_1 (+) = v35416688.cust_trx_line_gl_dist_id
AND v17715999.source_distribution_type (+) = v35416688.sourcedistributiontype
AND v35416688.term_id = v404380990.term_id (+)
AND v35416688.bill_to_site_use_id = v102674681.site_use_id (+)
AND v35416688.trx_date_only = v518783208.report_date
AND v35416688.adjustmentperiodflag = v518783208.adjustment_period_flag
AND v35416688.set_of_books_id2 = v518783208.ledger_id
AND v35416688.org_id240 = v220364535.organization_id1
AND v35416688.set_of_books_id = v498378102.ledger_id
AND v35416688.party_id = v405133203.party_id (+)
AND v405133203.per_party_id = v503360181.party_id (+)
AND v405133203.party_id = v74173908.party_id (+)
AND v35416688.cust_trx_type_seq_id = v62756817.cust_trx_type_seq_id 
AND v35416688.gl_date = v132021520.report_date (+)
AND v35416688.bill_to_customer_id = v157698628.cust_account_id (+)
AND v17715999.code_combination_id = v208318186.s_g_0 (+)
AND v17715999.chart_of_accounts_id = v208318186.s_g_1 (+)
--AND v17715999.code_combination_id = v292640615.code_combination_id (+)
AND v35416688.code_combination_id = v400621628.s_g_0
AND v35416688.chart_of_accounts_id = v400621628.s_g_1 )

AND ( ( NOT ( ( v62756817.type = 'DEP' ) ) )
AND ( NOT ( ( v62756817.type = 'GUAR' ) ) )
AND ( NOT ( ( v62756817.type = 'PMT' ) ) )
AND ( NOT ( ( v35416688.cust_trx_line_gl_dist_id IS NULL ) ) )
--AND (NOT ( ( GREATEST(v498378102.last_update_date, v35416688.last_update_date3) < DATEADD(day, -3, TO_TIMESTAMP_NTZ(SYSDATE())) ) ) )
--- TEST FILTERS AND v208318186.concat_seg = '9602-00-0000-21020-0000-00000' AND DATE(v35416688.trx_date) = '2023-05-05'

)


--AND ( NOT ( ( v498378102.last_update_date < to_timestamp('2023-04-20', 'yyyy-mm-dd hh24:mi:ss.ff') ) ) ) 
--OR ( NOT ( ( v35416688.last_update_date3 < to_timestamp('2023-04-20', 'yyyy-mm-dd hh24:mi:ss.ff') ) ) ) 


AND NOT ( (V144270398.USER_PERIOD_SET_NAME IS NULL ) )

) T5783149

--where  

----( nvl(T5783149.C149936038 , 'Y') = 'Y' ) and  --latest rec flag v35416688.latest_rec_flag in transaction line table , when applied we don't get all the results    

--T5783149.c109800147  = '300000124664708'--AND T5783149.c276323548 = '20'
),
SAWITH1 AS (select D1.c9 as c1,
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
     D1.c63 as c50,
     D1.c64 as c51,
     D1.c65 as c52,
     D1.c67 as c53,
     D1.c68 as c54,
     D1.c69 as c55,
     D1.c70 as c56,
     D1.c71 as c57,
     D1.c72 as c58,
     D1.c74 as c59,
     D1.c75 as c60,
     D1.c80 as c61,
     D1.c83 as c62,
     D1.c86 as c63,
     D1.c88 as c64,
     D1.c89 as c65,
     D1.c90 as c66,
     D1.c91 as c67,
     D1.c92 as c68,
     D1.c93 as c69,
     D1.c95 as c70,
     sum(D1.c87) as c71,
     sum(D1.c94) as c72,
     sum(D1.c96) as c73,
     sum(D1.c97) as c74,
     sum(D1.c98) as c75,
     sum(D1.c99) as c76
from 
     SAWITH0 D1
group by D1.c9, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c15, D1.c16, D1.c17, D1.c18, D1.c19, D1.c20, D1.c21, D1.c22, D1.c23, 
D1.c24, D1.c25, D1.c26, D1.c27, D1.c28, D1.c29, D1.c30, D1.c31, D1.c32, D1.c33, D1.c34, D1.c35, D1.c37, D1.c39, D1.c40, D1.c41, D1.c42, 
D1.c43, D1.c44, D1.c45, D1.c46, D1.c47, D1.c48, D1.c49, D1.c50, D1.c51, D1.c52, D1.c53, D1.c54, D1.c55, D1.c56, D1.c59, D1.c60, D1.c62, 
D1.c63, D1.c64, D1.c65, D1.c67, D1.c68, D1.c69, D1.c70, D1.c71, D1.c72, D1.c74, D1.c75, D1.c80, D1.c83, D1.c86, D1.c88, D1.c89, D1.c90, 
D1.c91, D1.c92, D1.c93, D1.c95),
SAWITH2 AS (select sum(D1.c71) as c1,
     sum(D1.c72) as c2,
     sum(case  when D1.c70 = 'REC' then D1.c71 else 0 end ) as c3,
     sum(case  when D1.c70 = 'REC' then D1.c72 else 0 end ) as c4,
     sum(D1.c73) as c5,
     sum(D1.c74) as c6,
     sum(D1.c75) as c7,
     sum(D1.c76) as c8,
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
     cast(D1.c69 as  VARCHAR ( 100 ) ) as c61,
     D1.c49 as c62,
     D1.c50 as c63,
     D1.c51 as c64,
     D1.c52 as c65,
     D1.c53 as c67,
     D1.c54 as c68,
     D1.c55 as c69,
     D1.c56 as c70,
     D1.c57 as c71,
     D1.c58 as c72,
     cast(D1.c68 as  VARCHAR ( 100 ) ) as c73,
     D1.c59 as c74,
     D1.c60 as c75,
     D1.c64 * 1000 + D1.c65 as c76,
     D1.c44 * 10000 + D1.c66 * 100000000 + D1.c67 * 100 + D1.c42 as c77,
     D1.c66 * 10000 + D1.c44 as c78,
     D1.c61 as c80,
     D1.c62 as c83,
     D1.c63 as c86
from 
     SAWITH1 D1
group by D1.c44 * 10000 + D1.c66 * 100000000 + D1.c67 * 100 + D1.c42, D1.c64 * 1000 + D1.c65, D1.c66 * 10000 + D1.c44, D1.c1, D1.c2, D1.c3, D1.c4, D1.c5, D1.c6, D1.c7, D1.c8, 
D1.c9, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c15, D1.c16, D1.c17, D1.c18, D1.c19, D1.c20, D1.c21, D1.c22, D1.c23, D1.c24, D1.c25, D1.c26, D1.c27, D1.c28, D1.c29, D1.c30, 
D1.c31, D1.c32, D1.c33, D1.c34, D1.c35, D1.c36, D1.c37, D1.c38, D1.c39, D1.c40, D1.c41, D1.c42, D1.c43, D1.c44, D1.c45, D1.c46, D1.c47, D1.c48, D1.c49, D1.c50, D1.c51, D1.c52, 
D1.c53, D1.c54, D1.c55, D1.c56, D1.c57, D1.c58, D1.c59, D1.c60, D1.c61, D1.c62, D1.c63, cast(D1.c68 as  VARCHAR ( 100 ) ), cast(D1.c69 as  VARCHAR ( 100 ) )
),SAWITH3 AS 
(select T5783150.C389230568 as c1,
     T5783150.C164164071 as c2
from 
     (SELECT V72673585.MEANING AS C389230568,         
     V72673585.LOOKUP_CODE AS C164164071,         
     V72673585.LANGUAGE AS C343259318,         
     V72673585.LOOKUP_TYPE AS C417433804,         
     V72673585.VIEW_APPLICATION_ID AS C456636657,         
     V72673585.SET_ID AS C497682495 
     FROM CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO V72673585 
     WHERE ( ( (V72673585.LOOKUP_TYPE = 'HZ_STATUS' ) )  
     AND ( (V72673585.VIEW_APPLICATION_ID = 0 ) )  
     AND ( (V72673585.SET_ID = 0 ) )  
     AND ( (V72673585.LANGUAGE = 'US' ) ) )) T5783150
     ),SAWITH4 AS 
     (select T5783151.C389230568 as c1,
     T5783151.C164164071 as c2
    from 
     (SELECT V72673585.MEANING AS C389230568,         
     V72673585.LOOKUP_CODE AS C164164071,         
     V72673585.LANGUAGE AS C343259318,         
     V72673585.LOOKUP_TYPE AS C417433804,         
     V72673585.VIEW_APPLICATION_ID AS C456636657,         
     V72673585.SET_ID AS C497682495 
     FROM CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO V72673585 
     WHERE ( ( (V72673585.LOOKUP_TYPE = 'CUSTOMER_TYPE' ) )  
     AND ( (V72673585.VIEW_APPLICATION_ID = 0 ) )  
     AND ( (V72673585.SET_ID = 0 ) )  
     AND ( (V72673585.LANGUAGE = 'US' ) ) )) T5783151),
SAWITH5 AS (select T5783152.C482842991 as c1, -- Payment of the transaction balance is due within 5 Business days of the Invoice issue date.
     T5783152.C93940995 as c2,
     T5783152.C179792223 as c3
from 
     (SELECT 
     V153658366.RA_TERM_TLDESCRIPTION AS C482842991,         
     V153658366.RA_TERM_TLTERM_ID AS C93940995,         
     V153658366.RA_TERM_TLNAME AS C179792223,         
     V153658366.RA_TERM_TLLANGUAGE AS C392122403 
     FROM CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.PAYMENT_TERM_EXTRACT_PVO V153658366 
     WHERE ( ( (V153658366.RA_TERM_TLLANGUAGE = 'US' ) ) )) T5783152
     ),SAWITH6 AS 
     (select T5783153.C389230568 as c1,
     T5783153.C164164071 as c2
from 
     (SELECT V72673585.MEANING AS C389230568,         
     V72673585.LOOKUP_CODE AS C164164071,         
     V72673585.LANGUAGE AS C343259318,        
     V72673585.LOOKUP_TYPE AS C417433804,         
     V72673585.VIEW_APPLICATION_ID AS C456636657,         
     V72673585.SET_ID AS C497682495 
     FROM CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO V72673585 
     WHERE ( ( (V72673585.LOOKUP_TYPE = 'GL_ASF_LEDGER_CATEGORY' ) )  
     AND ( (V72673585.VIEW_APPLICATION_ID = 101 ) )  
     AND ( (V72673585.SET_ID = 0 ) )  
     AND ( (V72673585.LANGUAGE = 'US' ) ) )) T5783153
     ),SAWITH7 AS 
(select D1.c1 as c1,
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
     D1.c80 as c37,
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
     D1.c83 as c59,
     D4.c3 as c60,
     D1.c59 as c61,
     D1.c60 as c62,
     D1.c61 as c63,
     D1.c62 as c64,
     D1.c63 as c65,
     D1.c64 as c66,
     D1.c65 as c67,
     D5.c1 as c68,
     D1.c86 as c69,
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
     D1.c78 as c81
from 
     (
          (
               (
                    
                         SAWITH2 D1 left outer join SAWITH3 D2 On D1.c80 = D2.c2) left outer join SAWITH4 D3 On D1.c37 = D3.c2) left outer join SAWITH5 D4 On D1.c83 = D4.c2) 
                         left outer join SAWITH6 D5 On D1.c86 = D5.c2),
    sawith8 AS (
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
        d1.c81 AS c81
    FROM
        (
            SELECT
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
                NVL(d901.c36, d901.c37) AS c28,
                d901.c38                AS c29,
                NVL(d901.c39, d901.c38) AS c30,
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
                NVL(d901.c58, cast(d901.c59 as string)) AS c57, -- cast required as c59 = term id (number)  and c58 comment (string), conflicts. 
                NVL(d901.c60, cast(d901.c59 as string)) AS c58,  -- cast required as c59 = term id (number)  and c58 comment (string), conflicts. 
                d901.c61                AS c59,
                d901.c62                AS c60,
                d901.c63                AS c61,
                d901.c64                AS c62,
                d901.c65                AS c63,
                d901.c66                AS c64,
                d901.c67                AS c65,
                NVL(d901.c68, d901.c69) AS c66,
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
                d901.c37                AS c79,
                d901.c59                AS c80,
                d901.c69                AS c81,
                ROW_NUMBER()
                OVER(PARTITION BY d901.c9, d901.c10, d901.c11, d901.c12, d901.c13,
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
                                  d901.c79, d901.c80, d901.c81
                     ORDER BY
                         d901.c9 ASC, d901.c10 ASC, d901.c11 ASC,
                         d901.c12 ASC, d901.c13 ASC, d901.c14 ASC, d901.c15 ASC, d901.c16 ASC,
                         d901.c17 ASC, d901.c18 ASC, d901.c19 ASC, d901.c20 ASC, d901.c21 ASC,
                         d901.c22 ASC, d901.c23 ASC, d901.c24 ASC, d901.c25 ASC, d901.c26 ASC,
                         d901.c27 ASC, d901.c28 ASC, d901.c29 ASC, d901.c30 ASC, d901.c31 ASC,
                         d901.c32 ASC, d901.c33 ASC, d901.c34 ASC, d901.c35 ASC, d901.c37 ASC,
                         d901.c38 ASC, d901.c40 ASC, d901.c41 ASC, d901.c42 ASC, d901.c43 ASC,
                         d901.c44 ASC, d901.c45 ASC, d901.c46 ASC, d901.c47 ASC, d901.c48 ASC,
                         d901.c49 ASC, d901.c50 ASC, d901.c51 ASC, d901.c52 ASC, d901.c53 ASC,
                         d901.c54 ASC, d901.c55 ASC, d901.c56 ASC, d901.c57 ASC, d901.c59 ASC,
                         d901.c61 ASC, d901.c62 ASC, d901.c63 ASC, d901.c64 ASC, d901.c65 ASC,
                         d901.c66 ASC, d901.c67 ASC, d901.c69 ASC, d901.c70 ASC, d901.c71 ASC,
                         d901.c72 ASC, d901.c73 ASC, d901.c74 ASC, d901.c75 ASC, d901.c76 ASC,
                         d901.c77 ASC, d901.c78 ASC, d901.c79 ASC, d901.c80 ASC, d901.c81 ASC
                )                       AS c82
            FROM
                sawith7 d901
        ) d1
    WHERE
        ( d1.c82 = 1 )
)
SELECT
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
  d1.c68 AS ACCOUNTING_CALENDAR_NAME--,
  --to_char(d1.c69,'YYYY-MM-DD HH24:MI:SS') AS LEDGER_LAST_UPDATE_DATE
from 
( select D1.c1 as c1,
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
     D1.c69 as c69
from 
     SAWITH8 D1 ) D1 
;
 GREATEST(NVL(v17715999.JOURNAL_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v35416688.TRANSACTION_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')),NVL(v405133203.SUPPLIER_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) AS  INCREMENTAL_LATEST_DATE, 
    CONCAT(v400621628.concat_values,'-',v17715999.displayed_line_number,'-',v35416688.trx_number,'-',v144270398.user_period_set_name) AS ID

/* COMPARE PROD VS NEW QUERY */

-- 31.5K ROWS THAT ARE DIFFERENT

-- 31 K ROWS ONLY PRESENT IN DEV
-- 109 ROWS ONLY PRESENT IN PROD
--  ID  = DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER,'-', ACCOUNTING_CALENDAR_NAME 

WITH combined AS (
    SELECT 
        COALESCE(t1.ID, t2.ID) AS ID,
        t1.ID AS t1_ID,
        t2.ID AS t2_ID,
        t1.CONCATENATED_SEGMENTS AS t1_CONCATENATED_SEGMENTS,
        t2.CONCATENATED_SEGMENTS AS t2_CONCATENATED_SEGMENTS,
        t1.ENTITY_CODE AS t1_ENTITY_CODE,
        t2.ENTITY_CODE AS t2_ENTITY_CODE,
        t1.COST_CENTER_CODE AS t1_COST_CENTER_CODE,
        t2.COST_CENTER_CODE AS t2_COST_CENTER_CODE,
        t1.ACCOUNT_CODE AS t1_ACCOUNT_CODE,
        t2.ACCOUNT_CODE AS t2_ACCOUNT_CODE,
        t1.INTERCOMPANY_CODE AS t1_INTERCOMPANY_CODE,
        t2.INTERCOMPANY_CODE AS t2_INTERCOMPANY_CODE,
        t1.MATERIAL_TYPE_CODE AS t1_MATERIAL_TYPE_CODE,
        t2.MATERIAL_TYPE_CODE AS t2_MATERIAL_TYPE_CODE,
        t1.SCHEME_CODE AS t1_SCHEME_CODE,
        t2.SCHEME_CODE AS t2_SCHEME_CODE,
        t1.PROJECT_CODE AS t1_PROJECT_CODE,
        t2.PROJECT_CODE AS t2_PROJECT_CODE,
        t1.DIST_CONCATENATED_SEGMENTS AS t1_DIST_CONCATENATED_SEGMENTS,
        t2.DIST_CONCATENATED_SEGMENTS AS t2_DIST_CONCATENATED_SEGMENTS,
        t1.DIST_ENTITY_CODE AS t1_DIST_ENTITY_CODE,
        t2.DIST_ENTITY_CODE AS t2_DIST_ENTITY_CODE,
        t1.DIST_COST_CENTER_CODE AS t1_DIST_COST_CENTER_CODE,
        t2.DIST_COST_CENTER_CODE AS t2_DIST_COST_CENTER_CODE,
        t1.DIST_ACCOUNT_CODE AS t1_DIST_ACCOUNT_CODE,
        t2.DIST_ACCOUNT_CODE AS t2_DIST_ACCOUNT_CODE,
        t1.BILL_TO_CUST_NAME AS t1_BILL_TO_CUST_NAME,
        t2.BILL_TO_CUST_NAME AS t2_BILL_TO_CUST_NAME,
        t1.BILL_TO_CUST_NUM AS t1_BILL_TO_CUST_NUM,
        t2.BILL_TO_CUST_NUM AS t2_BILL_TO_CUST_NUM,
        t1.BILL_TO_CUST_TYPE AS t1_BILL_TO_CUST_TYPE,
        t2.BILL_TO_CUST_TYPE AS t2_BILL_TO_CUST_TYPE,
        t1.BILL_TO_CUST_ORG_NAME AS t1_BILL_TO_CUST_ORG_NAME,
        t2.BILL_TO_CUST_ORG_NAME AS t2_BILL_TO_CUST_ORG_NAME,
        t1.BILL_TO_CUST_PER_FIRST_NAME AS t1_BILL_TO_CUST_PER_FIRST_NAME,
        t2.BILL_TO_CUST_PER_FIRST_NAME AS t2_BILL_TO_CUST_PER_FIRST_NAME,
        t1.BILL_TO_CUST_PER_LAST_NAME AS t1_BILL_TO_CUST_PER_LAST_NAME,
        t2.BILL_TO_CUST_PER_LAST_NAME AS t2_BILL_TO_CUST_PER_LAST_NAME,
        t1.ACCOUNTING_DATE AS t1_ACCOUNTING_DATE,
        t2.ACCOUNTING_DATE AS t2_ACCOUNTING_DATE,
        t1.PAYMENT_TERMS_DUE_DATE AS t1_PAYMENT_TERMS_DUE_DATE,
        t2.PAYMENT_TERMS_DUE_DATE AS t2_PAYMENT_TERMS_DUE_DATE,
        t1.ACCOUNTING_CLASS AS t1_ACCOUNTING_CLASS,
        t2.ACCOUNTING_CLASS AS t2_ACCOUNTING_CLASS,
        t1.JOURNAL_CATEGORY_NAME AS t1_JOURNAL_CATEGORY_NAME,
        t2.JOURNAL_CATEGORY_NAME AS t2_JOURNAL_CATEGORY_NAME,
        t1.JOURNAL_DESC AS t1_JOURNAL_DESC,
        t2.JOURNAL_DESC AS t2_JOURNAL_DESC,
        t1.JOURNAL_LINE_NUM AS t1_JOURNAL_LINE_NUM,
        t2.JOURNAL_LINE_NUM AS t2_JOURNAL_LINE_NUM,
        t1.BILL_TO_CUST_ACCOUNT_CLASS_CODE AS t1_BILL_TO_CUST_ACCOUNT_CLASS_CODE,
        t2.BILL_TO_CUST_ACCOUNT_CLASS_CODE AS t2_BILL_TO_CUST_ACCOUNT_CLASS_CODE,
        t1.BILL_TO_CUST_ACCOUNT_DESC AS t1_BILL_TO_CUST_ACCOUNT_DESC,
        t2.BILL_TO_CUST_ACCOUNT_DESC AS t2_BILL_TO_CUST_ACCOUNT_DESC,
        t1.BILL_TO_CUST_ACCOUNT_NUM AS t1_BILL_TO_CUST_ACCOUNT_NUM,
        t2.BILL_TO_CUST_ACCOUNT_NUM AS t2_BILL_TO_CUST_ACCOUNT_NUM,
        t1.BILL_TO_ACCOUNT_STATUS AS t1_BILL_TO_ACCOUNT_STATUS,
        t2.BILL_TO_ACCOUNT_STATUS AS t2_BILL_TO_ACCOUNT_STATUS,
        t1.BILL_TO_ACCOUNT_TYPE_CODE AS t1_BILL_TO_ACCOUNT_TYPE_CODE,
        t2.BILL_TO_ACCOUNT_TYPE_CODE AS t2_BILL_TO_ACCOUNT_TYPE_CODE,
        t1.BILL_TO_ACCOUNT_TYPE AS t1_BILL_TO_ACCOUNT_TYPE,
        t2.BILL_TO_ACCOUNT_TYPE AS t2_BILL_TO_ACCOUNT_TYPE,
        t1.BILL_TO_SITE_CITY AS t1_BILL_TO_SITE_CITY,
        t2.BILL_TO_SITE_CITY AS t2_BILL_TO_SITE_CITY,
        t1.BILL_TO_SITE_COUNTRY AS t1_BILL_TO_SITE_COUNTRY,
        t2.BILL_TO_SITE_COUNTRY AS t2_BILL_TO_SITE_COUNTRY,
        t1.BILL_TO_SITE_DESC AS t1_BILL_TO_SITE_DESC,
        t2.BILL_TO_SITE_DESC AS t2_BILL_TO_SITE_DESC,
        t1.BILL_TO_SITE_NAME AS t1_BILL_TO_SITE_NAME,
        t2.BILL_TO_SITE_NAME AS t2_BILL_TO_SITE_NAME,
        t1.BILL_TO_SITE_NUM AS t1_BILL_TO_SITE_NUM,
        t2.BILL_TO_SITE_NUM AS t2_BILL_TO_SITE_NUM,
        t1.BILL_TO_SITE_POSTAL_CODE AS t1_BILL_TO_SITE_POSTAL_CODE,
        t2.BILL_TO_SITE_POSTAL_CODE AS t2_BILL_TO_SITE_POSTAL_CODE,
        t1.BILL_TO_SITE_STATE AS t1_BILL_TO_SITE_STATE,
        t2.BILL_TO_SITE_STATE AS t2_BILL_TO_SITE_STATE,
        t1.BILL_TO_SITE_STATUS AS t1_BILL_TO_SITE_STATUS,
        t2.BILL_TO_SITE_STATUS AS t2_BILL_TO_SITE_STATUS,
        t1.BILL_TO_SITE_ADDR1 AS t1_BILL_TO_SITE_ADDR1,
        t2.BILL_TO_SITE_ADDR1 AS t2_BILL_TO_SITE_ADDR1,
        t1.BILL_TO_SITE_ADDR2 AS t1_BILL_TO_SITE_ADDR2,
        t2.BILL_TO_SITE_ADDR2 AS t2_BILL_TO_SITE_ADDR2,
        t1.BILL_TO_SITE AS t1_BILL_TO_SITE,
        t2.BILL_TO_SITE AS t2_BILL_TO_SITE,
        t1.BU_NAME AS t1_BU_NAME,
        t2.BU_NAME AS t2_BU_NAME,
        t1.LEGAL_ENTITY_NAME AS t1_LEGAL_ENTITY_NAME,
        t2.LEGAL_ENTITY_NAME AS t2_LEGAL_ENTITY_NAME,
        t1.FISCAL_PERIOD_NUM AS t1_FISCAL_PERIOD_NUM,
        t2.FISCAL_PERIOD_NUM AS t2_FISCAL_PERIOD_NUM,
        t1.FISCAL_PERIOD AS t1_FISCAL_PERIOD,
        t2.FISCAL_PERIOD AS t2_FISCAL_PERIOD,
        t1.FISCAL_YEAR_NUM AS t1_FISCAL_YEAR_NUM,
        t2.FISCAL_YEAR_NUM AS t2_FISCAL_YEAR_NUM,
        t1.LEDGER_NAME AS t1_LEDGER_NAME,
        t2.LEDGER_NAME AS t2_LEDGER_NAME,
        t1.ACCOUNTED_AMT_CR AS t1_ACCOUNTED_AMT_CR,
        t2.ACCOUNTED_AMT_CR AS t2_ACCOUNTED_AMT_CR,
        t1.ACCOUNTED_AMT_DR AS t1_ACCOUNTED_AMT_DR,
        t2.ACCOUNTED_AMT_DR AS t2_ACCOUNTED_AMT_DR,
        t1.ENTERED_AMT_CR AS t1_ENTERED_AMT_CR,
        t2.ENTERED_AMT_CR AS t2_ENTERED_AMT_CR,
        t1.ENTERED_AMT_DR AS t1_ENTERED_AMT_DR,
        t2.ENTERED_AMT_DR AS t2_ENTERED_AMT_DR,
        t1.CURRENCY AS t1_CURRENCY,
        t2.CURRENCY AS t2_CURRENCY,
        t1.TRANSACTION_ACCOUNTED_AMT AS t1_TRANSACTION_ACCOUNTED_AMT,
        t2.TRANSACTION_ACCOUNTED_AMT AS t2_TRANSACTION_ACCOUNTED_AMT,
        t1.TRANSACTION_ENTERED_AMT AS t1_TRANSACTION_ENTERED_AMT,
        t2.TRANSACTION_ENTERED_AMT AS t2_TRANSACTION_ENTERED_AMT,
        t1.DIST_ACCOUNTED_AMT AS t1_DIST_ACCOUNTED_AMT,
        t2.DIST_ACCOUNTED_AMT AS t2_DIST_ACCOUNTED_AMT,
        t1.DIST_ENTERED_AMT AS t1_DIST_ENTERED_AMT,
        t2.DIST_ENTERED_AMT AS t2_DIST_ENTERED_AMT,
        t1.PAYMENT_TERM_DESC AS t1_PAYMENT_TERM_DESC,
        t2.PAYMENT_TERM_DESC AS t2_PAYMENT_TERM_DESC,
        t1.PAYMENT_TERM_NAME AS t1_PAYMENT_TERM_NAME,
        t2.PAYMENT_TERM_NAME AS t2_PAYMENT_TERM_NAME,
        t1.TRANSACTION_TYPE_DESC AS t1_TRANSACTION_TYPE_DESC,
        t2.TRANSACTION_TYPE_DESC AS t2_TRANSACTION_TYPE_DESC,
        t1.TRANSACTION_TYPE AS t1_TRANSACTION_TYPE,
        t2.TRANSACTION_TYPE AS t2_TRANSACTION_TYPE,
        t1.TRANSACTION_ID AS t1_TRANSACTION_ID,
        t2.TRANSACTION_ID AS t2_TRANSACTION_ID,
        t1.TRANSACTION_NUMBER AS t1_TRANSACTION_NUMBER,
        t2.TRANSACTION_NUMBER AS t2_TRANSACTION_NUMBER,
        t1.LAST_UPDATE_DATE AS t1_LAST_UPDATE_DATE,
        t2.LAST_UPDATE_DATE AS t2_LAST_UPDATE_DATE,
        t1.TRANSACTION_SOURCE_NAME AS t1_TRANSACTION_SOURCE_NAME,
        t2.TRANSACTION_SOURCE_NAME AS t2_TRANSACTION_SOURCE_NAME,
        t1.ACCOUNT_CODE_COMBIN_DESC AS t1_ACCOUNT_CODE_COMBIN_DESC,
        t2.ACCOUNT_CODE_COMBIN_DESC AS t2_ACCOUNT_CODE_COMBIN_DESC,
        t1.LEDGER_CATEGORY_NAME AS t1_LEDGER_CATEGORY_NAME,
        t2.LEDGER_CATEGORY_NAME AS t2_LEDGER_CATEGORY_NAME,
        t1.TRANSACTION_DATE AS t1_TRANSACTION_DATE,
        t2.TRANSACTION_DATE AS t2_TRANSACTION_DATE,
        t1.ACCOUNTING_CALENDAR_NAME AS t1_ACCOUNTING_CALENDAR_NAME,
        t2.ACCOUNTING_CALENDAR_NAME AS t2_ACCOUNTING_CALENDAR_NAME
    FROM (
        SELECT 
            CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER,'-', ACCOUNTING_CALENDAR_NAME) AS ID,
            CONCATENATED_SEGMENTS,
            ENTITY_CODE,
            COST_CENTER_CODE,
            ACCOUNT_CODE,
            INTERCOMPANY_CODE,
            MATERIAL_TYPE_CODE,
            SCHEME_CODE,
            PROJECT_CODE,
            DIST_CONCATENATED_SEGMENTS,
            DIST_ENTITY_CODE,
            DIST_COST_CENTER_CODE,
            DIST_ACCOUNT_CODE,
            BILL_TO_CUST_NAME,
            BILL_TO_CUST_NUM,
            BILL_TO_CUST_TYPE,
            BILL_TO_CUST_ORG_NAME,
            BILL_TO_CUST_PER_FIRST_NAME,
            BILL_TO_CUST_PER_LAST_NAME,
            ACCOUNTING_DATE,
            PAYMENT_TERMS_DUE_DATE,
            ACCOUNTING_CLASS,
            JOURNAL_CATEGORY_NAME,
            JOURNAL_DESC,
            JOURNAL_LINE_NUM,
            BILL_TO_CUST_ACCOUNT_CLASS_CODE,
            BILL_TO_CUST_ACCOUNT_DESC,
            BILL_TO_CUST_ACCOUNT_NUM,
            BILL_TO_ACCOUNT_STATUS,
            BILL_TO_ACCOUNT_TYPE_CODE,
            BILL_TO_ACCOUNT_TYPE,
            BILL_TO_SITE_CITY,
            BILL_TO_SITE_COUNTRY,
            BILL_TO_SITE_DESC,
            BILL_TO_SITE_NAME,
            BILL_TO_SITE_NUM,
            BILL_TO_SITE_POSTAL_CODE,
            BILL_TO_SITE_STATE,
            BILL_TO_SITE_STATUS,
            BILL_TO_SITE_ADDR1,
            BILL_TO_SITE_ADDR2,
            BILL_TO_SITE,
            BU_NAME,
            LEGAL_ENTITY_NAME,
            FISCAL_PERIOD_NUM,
            FISCAL_PERIOD,
            FISCAL_YEAR_NUM,
            LEDGER_NAME,
            ACCOUNTED_AMT_CR,
            ACCOUNTED_AMT_DR,
            ENTERED_AMT_CR,
            ENTERED_AMT_DR,
            CURRENCY,
            TRANSACTION_ACCOUNTED_AMT,
            TRANSACTION_ENTERED_AMT,
            DIST_ACCOUNTED_AMT,
            DIST_ENTERED_AMT,
            PAYMENT_TERM_DESC,
            PAYMENT_TERM_NAME,
            TRANSACTION_TYPE_DESC,
            TRANSACTION_TYPE,
            TRANSACTION_ID,
            TRANSACTION_NUMBER,
            LAST_UPDATE_DATE,
            TRANSACTION_SOURCE_NAME,
            ACCOUNT_CODE_COMBIN_DESC,
            LEDGER_CATEGORY_NAME,
            TRANSACTION_DATE,
            ACCOUNTING_CALENDAR_NAME
        FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS
    ) t1
    FULL OUTER JOIN (
        SELECT 
            CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER,'-', ACCOUNTING_CALENDAR_NAME) AS ID,
            CONCATENATED_SEGMENTS,
            ENTITY_CODE,
            COST_CENTER_CODE,
            ACCOUNT_CODE,
            INTERCOMPANY_CODE,
            MATERIAL_TYPE_CODE,
            SCHEME_CODE,
            PROJECT_CODE,
            DIST_CONCATENATED_SEGMENTS,
            DIST_ENTITY_CODE,
            DIST_COST_CENTER_CODE,
            DIST_ACCOUNT_CODE,
            BILL_TO_CUST_NAME,
            BILL_TO_CUST_NUM,
            BILL_TO_CUST_TYPE,
            BILL_TO_CUST_ORG_NAME,
            BILL_TO_CUST_PER_FIRST_NAME,
            BILL_TO_CUST_PER_LAST_NAME,
            ACCOUNTING_DATE,
            PAYMENT_TERMS_DUE_DATE,
            ACCOUNTING_CLASS,
            JOURNAL_CATEGORY_NAME,
            JOURNAL_DESC,
            JOURNAL_LINE_NUM,
            BILL_TO_CUST_ACCOUNT_CLASS_CODE,
            BILL_TO_CUST_ACCOUNT_DESC,
            BILL_TO_CUST_ACCOUNT_NUM,
            BILL_TO_ACCOUNT_STATUS,
            BILL_TO_ACCOUNT_TYPE_CODE,
            BILL_TO_ACCOUNT_TYPE,
            BILL_TO_SITE_CITY,
            BILL_TO_SITE_COUNTRY,
            BILL_TO_SITE_DESC,
            BILL_TO_SITE_NAME,
            BILL_TO_SITE_NUM,
            BILL_TO_SITE_POSTAL_CODE,
            BILL_TO_SITE_STATE,
            BILL_TO_SITE_STATUS,
            BILL_TO_SITE_ADDR1,
            BILL_TO_SITE_ADDR2,
            BILL_TO_SITE,
            BU_NAME,
            LEGAL_ENTITY_NAME,
            FISCAL_PERIOD_NUM,
            FISCAL_PERIOD,
            FISCAL_YEAR_NUM,
            LEDGER_NAME,
            ACCOUNTED_AMT_CR,
            ACCOUNTED_AMT_DR,
            ENTERED_AMT_CR,
            ENTERED_AMT_DR,
            CURRENCY,
            TRANSACTION_ACCOUNTED_AMT,
            TRANSACTION_ENTERED_AMT,
            DIST_ACCOUNTED_AMT,
            DIST_ENTERED_AMT,
            PAYMENT_TERM_DESC,
            PAYMENT_TERM_NAME,
            TRANSACTION_TYPE_DESC,
            TRANSACTION_TYPE,
            TRANSACTION_ID,
            TRANSACTION_NUMBER,
            LAST_UPDATE_DATE,
            TRANSACTION_SOURCE_NAME,
            ACCOUNT_CODE_COMBIN_DESC,
            LEDGER_CATEGORY_NAME,
            TRANSACTION_DATE,
            ACCOUNTING_CALENDAR_NAME
        FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST
    ) t2
    ON t1.ID = t2.ID
)
SELECT 
    ID,
    CASE 
        WHEN t1_ID IS NOT NULL AND t2_ID IS NULL THEN 'Present in RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS only'
        WHEN t1_ID IS NULL AND t2_ID IS NOT NULL THEN 'Present in RAW_DEV.TEST.AR_TRANSACTIONS_TEST only'
        ELSE 'Present in both'
    END AS Presence_Flag,
    COALESCE(t1_CONCATENATED_SEGMENTS, t2_CONCATENATED_SEGMENTS) AS CONCATENATED_SEGMENTS,
    COALESCE(t1_ENTITY_CODE, t2_ENTITY_CODE) AS ENTITY_CODE,
    COALESCE(t1_COST_CENTER_CODE, t2_COST_CENTER_CODE) AS COST_CENTER_CODE,
    COALESCE(t1_ACCOUNT_CODE, t2_ACCOUNT_CODE) AS ACCOUNT_CODE,
    COALESCE(t1_INTERCOMPANY_CODE, t2_INTERCOMPANY_CODE) AS INTERCOMPANY_CODE,
    COALESCE(t1_MATERIAL_TYPE_CODE, t2_MATERIAL_TYPE_CODE) AS MATERIAL_TYPE_CODE,
    COALESCE(t1_SCHEME_CODE, t2_SCHEME_CODE) AS SCHEME_CODE,
    COALESCE(t1_PROJECT_CODE, t2_PROJECT_CODE) AS PROJECT_CODE,
    COALESCE(t1_DIST_CONCATENATED_SEGMENTS, t2_DIST_CONCATENATED_SEGMENTS) AS DIST_CONCATENATED_SEGMENTS,
    COALESCE(t1_DIST_ENTITY_CODE, t2_DIST_ENTITY_CODE) AS DIST_ENTITY_CODE,
    COALESCE(t1_DIST_COST_CENTER_CODE, t2_DIST_COST_CENTER_CODE) AS DIST_COST_CENTER_CODE,
    COALESCE(t1_DIST_ACCOUNT_CODE, t2_DIST_ACCOUNT_CODE) AS DIST_ACCOUNT_CODE,
    COALESCE(t1_BILL_TO_CUST_NAME, t2_BILL_TO_CUST_NAME) AS BILL_TO_CUST_NAME,
    COALESCE(t1_BILL_TO_CUST_NUM, t2_BILL_TO_CUST_NUM) AS BILL_TO_CUST_NUM,
    COALESCE(t1_BILL_TO_CUST_TYPE, t2_BILL_TO_CUST_TYPE) AS BILL_TO_CUST_TYPE,
    COALESCE(t1_BILL_TO_CUST_ORG_NAME, t2_BILL_TO_CUST_ORG_NAME) AS BILL_TO_CUST_ORG_NAME,
    COALESCE(t1_BILL_TO_CUST_PER_FIRST_NAME, t2_BILL_TO_CUST_PER_FIRST_NAME) AS BILL_TO_CUST_PER_FIRST_NAME,
    COALESCE(t1_BILL_TO_CUST_PER_LAST_NAME, t2_BILL_TO_CUST_PER_LAST_NAME) AS BILL_TO_CUST_PER_LAST_NAME,
    COALESCE(t1_ACCOUNTING_DATE, t2_ACCOUNTING_DATE) AS ACCOUNTING_DATE,
    COALESCE(t1_PAYMENT_TERMS_DUE_DATE, t2_PAYMENT_TERMS_DUE_DATE) AS PAYMENT_TERMS_DUE_DATE,
    COALESCE(t1_ACCOUNTING_CLASS, t2_ACCOUNTING_CLASS) AS ACCOUNTING_CLASS,
    COALESCE(t1_JOURNAL_CATEGORY_NAME, t2_JOURNAL_CATEGORY_NAME) AS JOURNAL_CATEGORY_NAME,
    COALESCE(t1_JOURNAL_DESC, t2_JOURNAL_DESC) AS JOURNAL_DESC,
    COALESCE(t1_JOURNAL_LINE_NUM, t2_JOURNAL_LINE_NUM) AS JOURNAL_LINE_NUM,
    COALESCE(t1_BILL_TO_CUST_ACCOUNT_CLASS_CODE, t2_BILL_TO_CUST_ACCOUNT_CLASS_CODE) AS BILL_TO_CUST_ACCOUNT_CLASS_CODE,
    COALESCE(t1_BILL_TO_CUST_ACCOUNT_DESC, t2_BILL_TO_CUST_ACCOUNT_DESC) AS BILL_TO_CUST_ACCOUNT_DESC,
    COALESCE(t1_BILL_TO_CUST_ACCOUNT_NUM, t2_BILL_TO_CUST_ACCOUNT_NUM) AS BILL_TO_CUST_ACCOUNT_NUM,
    COALESCE(t1_BILL_TO_ACCOUNT_STATUS, t2_BILL_TO_ACCOUNT_STATUS) AS BILL_TO_ACCOUNT_STATUS,
    COALESCE(t1_BILL_TO_ACCOUNT_TYPE_CODE, t2_BILL_TO_ACCOUNT_TYPE_CODE) AS BILL_TO_ACCOUNT_TYPE_CODE,
    COALESCE(t1_BILL_TO_ACCOUNT_TYPE, t2_BILL_TO_ACCOUNT_TYPE) AS BILL_TO_ACCOUNT_TYPE,
    COALESCE(t1_BILL_TO_SITE_CITY, t2_BILL_TO_SITE_CITY) AS BILL_TO_SITE_CITY,
    COALESCE(t1_BILL_TO_SITE_COUNTRY, t2_BILL_TO_SITE_COUNTRY) AS BILL_TO_SITE_COUNTRY,
    COALESCE(t1_BILL_TO_SITE_DESC, t2_BILL_TO_SITE_DESC) AS BILL_TO_SITE_DESC,
    COALESCE(t1_BILL_TO_SITE_NAME, t2_BILL_TO_SITE_NAME) AS BILL_TO_SITE_NAME,
    COALESCE(t1_BILL_TO_SITE_NUM, t2_BILL_TO_SITE_NUM) AS BILL_TO_SITE_NUM,
    COALESCE(t1_BILL_TO_SITE_POSTAL_CODE, t2_BILL_TO_SITE_POSTAL_CODE) AS BILL_TO_SITE_POSTAL_CODE,
    COALESCE(t1_BILL_TO_SITE_STATE, t2_BILL_TO_SITE_STATE) AS BILL_TO_SITE_STATE,
    COALESCE(t1_BILL_TO_SITE_STATUS, t2_BILL_TO_SITE_STATUS) AS BILL_TO_SITE_STATUS,
    COALESCE(t1_BILL_TO_SITE_ADDR1, t2_BILL_TO_SITE_ADDR1) AS BILL_TO_SITE_ADDR1,
    COALESCE(t1_BILL_TO_SITE_ADDR2, t2_BILL_TO_SITE_ADDR2) AS BILL_TO_SITE_ADDR2,
    COALESCE(t1_BILL_TO_SITE, t2_BILL_TO_SITE) AS BILL_TO_SITE,
    COALESCE(t1_BU_NAME, t2_BU_NAME) AS BU_NAME,
    COALESCE(t1_LEGAL_ENTITY_NAME, t2_LEGAL_ENTITY_NAME) AS LEGAL_ENTITY_NAME,
    COALESCE(t1_FISCAL_PERIOD_NUM, t2_FISCAL_PERIOD_NUM) AS FISCAL_PERIOD_NUM,
    COALESCE(t1_FISCAL_PERIOD, t2_FISCAL_PERIOD) AS FISCAL_PERIOD,
    COALESCE(t1_FISCAL_YEAR_NUM, t2_FISCAL_YEAR_NUM) AS FISCAL_YEAR_NUM,
    COALESCE(t1_LEDGER_NAME, t2_LEDGER_NAME) AS LEDGER_NAME,
    COALESCE(t1_ACCOUNTED_AMT_CR, t2_ACCOUNTED_AMT_CR) AS ACCOUNTED_AMT_CR,
    COALESCE(t1_ACCOUNTED_AMT_DR, t2_ACCOUNTED_AMT_DR) AS ACCOUNTED_AMT_DR,
    COALESCE(t1_ENTERED_AMT_CR, t2_ENTERED_AMT_CR) AS ENTERED_AMT_CR,
    COALESCE(t1_ENTERED_AMT_DR, t2_ENTERED_AMT_DR) AS ENTERED_AMT_DR,
    COALESCE(t1_CURRENCY, t2_CURRENCY) AS CURRENCY,
    COALESCE(t1_TRANSACTION_ACCOUNTED_AMT, t2_TRANSACTION_ACCOUNTED_AMT) AS TRANSACTION_ACCOUNTED_AMT,
    COALESCE(t1_TRANSACTION_ENTERED_AMT, t2_TRANSACTION_ENTERED_AMT) AS TRANSACTION_ENTERED_AMT,
    COALESCE(t1_DIST_ACCOUNTED_AMT, t2_DIST_ACCOUNTED_AMT) AS DIST_ACCOUNTED_AMT,
    COALESCE(t1_DIST_ENTERED_AMT, t2_DIST_ENTERED_AMT) AS DIST_ENTERED_AMT,
    COALESCE(t1_PAYMENT_TERM_DESC, t2_PAYMENT_TERM_DESC) AS PAYMENT_TERM_DESC,
    COALESCE(t1_PAYMENT_TERM_NAME, t2_PAYMENT_TERM_NAME) AS PAYMENT_TERM_NAME,
    COALESCE(t1_TRANSACTION_TYPE_DESC, t2_TRANSACTION_TYPE_DESC) AS TRANSACTION_TYPE_DESC,
    COALESCE(t1_TRANSACTION_TYPE, t2_TRANSACTION_TYPE) AS TRANSACTION_TYPE,
    COALESCE(t1_TRANSACTION_ID, t2_TRANSACTION_ID) AS TRANSACTION_ID,
    COALESCE(t1_TRANSACTION_NUMBER, t2_TRANSACTION_NUMBER) AS TRANSACTION_NUMBER,
    COALESCE(t1_LAST_UPDATE_DATE, t2_LAST_UPDATE_DATE) AS LAST_UPDATE_DATE,
    COALESCE(t1_TRANSACTION_SOURCE_NAME, t2_TRANSACTION_SOURCE_NAME) AS TRANSACTION_SOURCE_NAME,
    COALESCE(t1_ACCOUNT_CODE_COMBIN_DESC, t2_ACCOUNT_CODE_COMBIN_DESC) AS ACCOUNT_CODE_COMBIN_DESC,
    COALESCE(t1_LEDGER_CATEGORY_NAME, t2_LEDGER_CATEGORY_NAME) AS LEDGER_CATEGORY_NAME,
    COALESCE(t1_TRANSACTION_DATE, t2_TRANSACTION_DATE) AS TRANSACTION_DATE,
    COALESCE(t1_ACCOUNTING_CALENDAR_NAME, t2_ACCOUNTING_CALENDAR_NAME) AS ACCOUNTING_CALENDAR_NAME
FROM combined
WHERE t1_ID IS NULL OR t2_ID IS NULL


;

SELECT CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER,'-', ACCOUNTING_CALENDAR_NAME) AS ID, * 
FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS 
WHERE TRANSACTION_NUMBER = 'P101310138_CR' AND JOURNAL_LINE_NUM = '10'

--1410-21-1100-40005-0000-00000-10-P101310138-Scheme Sec Cal
;
--1410-21-1100-40005-0000-00000-10-P101310138_CR-Scheme Sec Cal
--1410-21-1100-40005-0000-00000-10-P101310138_CR-Scheme Ledger

SELECT CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER,'-', ACCOUNTING_CALENDAR_NAME) AS ID, * 
FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST 
WHERE TRANSACTION_NUMBER = 'P101317362' --AND JOURNAL_LINE_NUM = '10'

--1410-21-1100-40005-0000-00000-10-P101310138-Scheme Sec Cal

;

SELECT COUNT(*)
FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS 
WHERE DATE(TRANSACTION_DATE) >= '2023-01-01' AND DATE(TRANSACTION_DATE) <='2024-07-01'
-- 1,239,233
;
SELECT COUNT(*)
--CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER) AS ID,*
FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST 
--WHERE TRANSACTION_NUMBER = '692014'
WHERE DATE(TRANSACTION_DATE) >= '2023-01-01' AND DATE(TRANSACTION_DATE) <= '2024-07-01'
---1,270,478
646857.0
646853.0
--646,840
--1270482
;
70,834,281
/*DATA THAT DOESN'T EXIST IN THE NEW AR _TRANS TABLE */
/*

CR_P100991990;CR_P100986185;CR_P100986185;CR_P100991990;CR_P100742054;CR_P100742054;CR_P100986188;CR_P100991990;CR_P100986185;CR_P100986188;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100986188;CR_P100742053;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100986185;CR_P100991990;CR_P100991990;CR_P100742054;CR_P100991990;CR_P100991990;CR_P100986188;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100742054;CR_P100986188;CR_P100991990;CR_P100742053;CR_P100986188;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100742053;CR_P100742053;CR_P100991990;CR_P100986185;CR_P100991990;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100986185;CR_P100742054;CR_P100742054;CR_P100991990;CR_P100986188;CR_P100991990;CR_P100986188;CR_P100991990;CR_P100742054;CR_P100986185;CR_P100742054;CR_P100991990;CR_P100986185;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100742054;CR_P100986188;CR_P100742054;CR_P100991990;CR_P100986185;CR_P100991990;CR_P100991990;CR_P100991990;CR_P100742053;CR_P100991990;CR_P100986185;CR_P100742053;CR_P100991990;CR_P100986188;CR_P100742054;CR_P100991990;CR_P100742054;CR_P100986188;CR_P100986185;CR_P100986185;CR_P100986188;CR_P100742053;P101317362 */



SELECT COUNT(*)
FROM RAW_PROD.GG_CES_ERP.AR_TRANSACTIONS 
WHERE DATE(TRANSACTION_DATE) >= '2023-01-01' AND DATE(TRANSACTION_DATE) <='2024-07-01'
-- 1,239,233
;
SELECT COUNT(*)
--CONCAT(DIST_CONCATENATED_SEGMENTS,'-', JOURNAL_LINE_NUM,'-', TRANSACTION_NUMBER) AS ID,*
FROM RAW_DEV.TEST.AR_TRANSACTIONS_TEST 
--WHERE TRANSACTION_NUMBER = '692014'
WHERE DATE(TRANSACTION_DATE) >= '2023-01-01' AND DATE(TRANSACTION_DATE) <= '2024-07-01'
