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



WITH VIEW_A AS ( 
SELECT
SUM (AMOUNT) AS AMOUNT, 
SUM(AMOUNT_INV_CURR) AS AMOUNT_INV_CURR,
CHECK_NUMBER AS CHECK_NUMBER,                          
INVHEADER_INVOICE_DATE AS INVOICE_DATE,                           
CREATION_DATE1 AS CREATION_DATE,                            
CAST(INVOICE_ID AS VARCHAR(100)) AS INVOICE_ID,     
INVOICE_NUM AS INVOICE_NUMBER,                          
DUE_DATE AS DUE_DATE,                            
CHECK_DATE AS CHECK_DATE,                           
ORGANIZATIONUNIT_NAME,                          
LEGAL_ENTITY_NAME,                          
PARTY_NAME1,                         
SUPPLIER_SEGMENT1,
VENDOR_SITE_CODE,                          
LAST_UPDATE_DATE,                
INVHEADER_LAST_UPDATE_DATE,                        
INVHEADER_DESCRIPTION,               
CASE
    WHEN DATEDIFF(day, due_date::DATE, check_date::DATE) >= 0 THEN 'On-Time'
    WHEN DATEDIFF(day, due_date::DATE, check_date::DATE) < 0
         AND DATEDIFF(day, due_date::DATE, check_date::DATE) >= -30 THEN '1-30 Days Late'
    WHEN DATEDIFF(day, due_date::DATE, check_date::DATE) < -30
         AND DATEDIFF(day, due_date::DATE, check_date::DATE) >= -60 THEN '30-60 Days Late'
    WHEN DATEDIFF(day, due_date::DATE, check_date::DATE) < -60
         AND DATEDIFF(day, due_date::DATE, check_date::DATE) >= -90 THEN '60-90 Days Late'
    ELSE '> 1-90 Days Late'
END AS AGING_BUCKETS,
DATEDIFF(day, check_date::DATE, due_date::DATE) AS DAYS_LATE_PAID,
PAY_STATUS_DISPLAYED_FIELD,  
TERMS_ID,           
ORG_ID,     
LEGAL_ENTITY_ID,       
STATUS_LOOKUP_CODE,    
PAYMENT_STATUS_FLAG,    
PAYCODE_DESCRIPTION,
CHECKCODE_DESCRIPTION,
TERMS_DESCRIPTION,
INCREMENTAL_LATEST_DATE

FROM (
   SELECT   --10,423,140
    invoicepayment.AP_INVOICE_PAYMENTS_ALL_AMOUNT AS amount,
    invoicepayment.AP_INVOICE_PAYMENTS_ALL_INVOICE_ID AS invoice_id,
    invoicepayment.AP_INVOICE_PAYMENTS_ALL_INVOICE_PAYMENT_ID AS invoice_payment_id,
    checks.AP_CHECKS_ALL_CHECK_DATE AS check_date,
    checks.AP_CHECKS_ALL_CHECK_ID AS check_id,
    checks.AP_CHECKS_ALL_CHECK_NUMBER AS check_number,
    checks.AP_CHECKS_ALL_LAST_UPDATE_DATE AS last_update_date,
    checks.AP_CHECKS_ALL_LEGAL_ENTITY_ID AS legal_entity_id,
    checks.AP_CHECKS_ALL_ORG_ID AS org_id,
    checks.AP_CHECKS_ALL_PAYMENT_TYPE_FLAG AS payment_type_flag,
    checks.AP_CHECKS_ALL_STATUS_LOOKUP_CODE AS status_lookup_code,
    checks.AP_CHECKS_ALL_VENDOR_ID AS vendor_id,
    checks.AP_CHECKS_ALL_VENDOR_SITE_ID AS vendor_site_id,
    paymentschedule.AP_PAYMENT_SCHEDULES_ALL_DUE_DATE AS due_date,
    paymentschedule.AP_PAYMENT_SCHEDULES_ALL_INVOICE_ID AS invoice_id2,
    paymentschedule.AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM AS payment_num,
    invoicepayment.AP_INVOICE_PAYMENTS_ALL_AMOUNT_INV_CURR AS amount_inv_curr,
    supplier.SUPPLIER_SEGMENT_1 AS supplier_segment1,
    supplier.SUPP_INV_DIST_VENDOR_ID AS vendor_id557,
    supplier.PARTY_PARTY_ID     AS party_id824,
    supplier.PARTY_PARTY_NAME   AS party_name1,
    legalentity.LEGAL_ENTITY_NAME AS legal_entity_name,
    suppliersite.SUPPLIER_SITE_VENDOR_SITE_CODE AS vendor_site_code,
    organizationunit.effective_start_date651,
    organizationunit.effective_end_date8,
    organizationunit.org_information_id,
    organizationunit.name as organizationunit_name,
    organizationunit.organization_id,
    organizationunit.effective_start_date,
    organizationunit.effective_end_date,
    invoiceheader.AP_INVOICES_CREATION_DATE AS creation_date1,
    invoiceheader.AP_INVOICES_DESCRIPTION as invheader_description,
    invoiceheader.AP_INVOICES_INVOICE_DATE as invheader_invoice_date,
    --invoiceheader.AP_INVOICES_INVOICE_ID as invoice_id,
    invoiceheader.AP_INVOICES_INVOICE_NUM as invoice_num,
    invoiceheader.AP_INVOICES_LAST_UPDATE_DATE as invheader_last_update_date,
    invoiceheader.AP_INVOICES_PAYMENT_STATUS_FLAG as payment_status_flag,
    invoiceheader.AP_INVOICES_TERMS_ID as terms_id,
    payableslookup.DISPLAYED_FIELD as pay_status_displayed_field,
    nvl(payment_desc.MEANING, checks.AP_CHECKS_ALL_PAYMENT_TYPE_FLAG) paycode_description,
    nvl( check_desc.MEANING, checks.AP_CHECKS_ALL_STATUS_LOOKUP_CODE )  checkcode_description,
    nvl(terms_desc.PAYMENT_TERM_HEADER_TRANSLATION_NAME, to_char(invoiceheader.AP_INVOICES_TERMS_ID)) terms_description,
    GREATEST(NVL(checks.AP_CHECKS_ALL_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')), NVL(invoiceheader.AP_INVOICES_LAST_UPDATE_DATE,current_date)) as INCREMENTAL_LATEST_DATE    
    FROM CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.PAID_DISBURSEMENT_SCHEDULE_EXTRACT_PVO invoicepayment  -- v360131283
    JOIN CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.DISBURSEMENT_HEADER_EXTRACT_PVO checks  -- v360131283
    ON invoicepayment.AP_INVOICE_PAYMENTS_ALL_CHECK_ID = checks.AP_CHECKS_ALL_CHECK_ID
    JOIN CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.INVOICE_PAYMENT_SCHEDULE_EXTRACT_PVO paymentschedule  -- v360131283
    ON invoicepayment.AP_INVOICE_PAYMENTS_ALL_INVOICE_ID = paymentschedule.AP_PAYMENT_SCHEDULES_ALL_INVOICE_ID 
    AND invoicepayment.AP_INVOICE_PAYMENTS_ALL_PAYMENT_NUM = paymentschedule.AP_PAYMENT_SCHEDULES_ALL_PAYMENT_NUM 
    LEFT JOIN CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO supplier --v448314251
    ON checks.AP_CHECKS_ALL_VENDOR_ID = supplier.SUPP_INV_DIST_VENDOR_ID
    LEFT JOIN CES_ERP.FSCM_PROD_FINEXTRACT_XLEBICCEXTRACT.LEGAL_ENTITY_EXTRACT_PVO legalentity  -- v286822520
    ON checks.AP_CHECKS_ALL_LEGAL_ENTITY_ID = legalentity.LEGAL_ENTITY_LEGAL_ENTITY_ID 
    LEFT JOIN CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_SITE_PVO suppliersite  -- v250398895
    ON checks.AP_CHECKS_ALL_VENDOR_SITE_ID = suppliersite.VENDOR_SITE_ID
    JOIN ( SELECT 
        organizationunit.ORGANIZATION_ID      AS organization_id1,
        organizationunit.EFFECTIVE_START_DATE AS effective_start_date651,
        organizationunit.EFFECTIVE_END_DATE   AS effective_end_date8,
        organizationunit.ORG_INFO_BUPEOORG_INFORMATION_ID as org_information_id,
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEONAME as name,
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOORGANIZATION_ID as organization_id,
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_START_DATE as effective_start_date,
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_END_DATE as effective_end_date,
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOLANGUAGE as language
        FROM CES_ERP.FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_PVO organizationunit
        WHERE ( DATE_TRUNC('DAY', SYSDATE()) BETWEEN organizationunit.EFFECTIVE_START_DATE AND organizationunit.EFFECTIVE_END_DATE )
        AND ( DATE_TRUNC('DAY', SYSDATE()) BETWEEN organizationunit.ORG_INFO_BUPEOEFFECTIVE_START_DATE AND organizationunit.ORG_INFO_BUPEOEFFECTIVE_END_DATE )
        AND ( DATE_TRUNC('DAY', SYSDATE()) BETWEEN organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_START_DATE AND      
        organizationunit.ORGANIZATION_UNIT_TRANSLATION_PEOEFFECTIVE_END_DATE ) ) organizationunit  -- v220364535
    ON checks.AP_CHECKS_ALL_ORG_ID = organizationunit.organization_id1
    JOIN CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.INVOICE_HEADER_EXTRACT_PVO invoiceheader --v420737864
    ON invoicepayment.AP_INVOICE_PAYMENTS_ALL_INVOICE_ID = invoiceheader.AP_INVOICES_INVOICE_ID
    LEFT JOIN CES_ERP.FSCM_PROD_FINAPSHAREDCORE.INVOICE_PAYMENT_LOOKUP_PVO payableslookup  -- v480842854
    ON  invoiceheader.AP_INVOICES_PAYMENT_STATUS_FLAG = payableslookup.LOOKUP_CODE
    LEFT OUTER JOIN  (
            SELECT
                v106358286.PAYMENT_TERM_HEADER_TRANSLATION_NAME ,
                v106358286.PAYMENT_TERM_HEADER_TRANSLATION_TERM_ID,
                v106358286.PAYMENT_TERM_HEADER_TRANSLATION_LANGUAGE 
            FROM
              CES_ERP.FSCM_PROD_FINEXTRACT_APBICCEXTRACT.PAYMENT_TERM_HEADER_TRANSLATION_EXTRACT_PVO v106358286
            WHERE
                ( ( ( v106358286.PAYMENT_TERM_HEADER_TRANSLATION_LANGUAGE = 'US' ) ) )
        ) terms_desc   --v106358286 -- sawith2
    ON  invoiceheader.AP_INVOICES_TERMS_ID = terms_desc.PAYMENT_TERM_HEADER_TRANSLATION_TERM_ID
    LEFT OUTER JOIN ( 
            SELECT
                v72673585.MEANING  ,
                v72673585.LOOKUP_CODE 
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'CHECK STATE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 200 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) )
        ) check_desc --sawith3 -- v72673585
    ON  checks.AP_CHECKS_ALL_STATUS_LOOKUP_CODE  = check_desc.LOOKUP_CODE 
     LEFT OUTER JOIN ( SELECT
                v72673585.MEANING,
                v72673585.LOOKUP_CODE 
            FROM
                CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO v72673585
            WHERE
                ( ( ( v72673585.LOOKUP_TYPE = 'PAYMENT TYPE' ) )
                  AND ( ( v72673585.VIEW_APPLICATION_ID = 200 ) )
                  AND ( ( v72673585.SET_ID = 0 ) )
                  AND ( ( v72673585.LANGUAGE = 'US' ) ) 
                )) payment_desc  --  sawith4  -- v72673585
    ON  checks.AP_CHECKS_ALL_PAYMENT_TYPE_FLAG =  payment_desc.LOOKUP_CODE
    
    
    {% if is_incremental() %}

     WHERE 
     GREATEST(NVL(checks.AP_CHECKS_ALL_LAST_UPDATE_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')), NVL(invoiceheader.AP_INVOICES_LAST_UPDATE_DATE,current_date)) >= (SELECT DATE(dateadd(days, {{var('erp_delta_load_days')}}, LAST_UPDATED_TIMESTAMP)) FROM  COMMON_{{target.name.upper()}}.ERP.CONTROL_TIME_INCREMENTAL_LOAD WHERE TABLE_NAME = '{{trimmed_source_name}}' )
                  
    {% endif %}


    ) X
    GROUP BY
    CHECK_NUMBER,                          
    INVHEADER_INVOICE_DATE,                            
    CREATION_DATE1,
    CHECK_DATE,
    CAST(INVOICE_ID AS VARCHAR(100)),     
    INVOICE_NUM,                        
    DUE_DATE,                          
    ORGANIZATIONUNIT_NAME,                          
    LEGAL_ENTITY_NAME,                          
    PARTY_NAME1,                          
    SUPPLIER_SEGMENT1,
    VENDOR_SITE_CODE,                         
    LAST_UPDATE_DATE,                
    INVHEADER_LAST_UPDATE_DATE,                         
    INVHEADER_DESCRIPTION,                   
    PAY_STATUS_DISPLAYED_FIELD,  
    TERMS_ID,            
    ORG_ID,      
    LEGAL_ENTITY_ID,          
    STATUS_LOOKUP_CODE,    
    PAYMENT_STATUS_FLAG,     
    PAYCODE_DESCRIPTION,
    CHECKCODE_DESCRIPTION,
    TERMS_DESCRIPTION,
    INCREMENTAL_LATEST_DATE
    
    ), VIEW_B AS (SELECT 
    TO_CHAR(CHECK_NUMBER)||'-'||TO_CHAR(INVOICE_DATE,'YYYY-MM-DD')||'-'||INVOICE_NUMBER AS ID,
    CHECK_NUMBER  AS CHECK_NUMBER,
    INVOICE_DATE  AS INVOICE_DATE,
    CREATION_DATE  AS INVOICE_ENTERED_DATE,    
    INVOICE_ID AS INVOICE_ID,
    INVOICE_NUMBER  AS INVOICE_NUMBER,
    TERMS_DESCRIPTION AS PAYMENT_TERMS,
    DUE_DATE AS DUE_DATE,
    CHECK_DATE AS PAYMENT_DATE,    
    CHECKCODE_DESCRIPTION  AS PAYMENT_STATUS,
    ORGANIZATIONUNIT_NAME AS BU_NAME,
    LEGAL_ENTITY_NAME AS LEGAL_ENTITY_NAME,
    PARTY_NAME1 AS SUPPLIER_NAME,
    SUPPLIER_SEGMENT1 AS SUPPLIER_NUMBER,
    VENDOR_SITE_CODE AS SUPPLIER_SITE,
    AMOUNT AS PAYMENT_AMOUNT,
    INVHEADER_LAST_UPDATE_DATE  AS INVOICE_LAST_UPD_DATE,
    LAST_UPDATE_DATE AS PAYMENT_LAST_UPD_DATE,    
    INVHEADER_DESCRIPTION AS INVOICE_DESC,
    AMOUNT_INV_CURR AS PAID_AMOUNT_IN_INVOICE_CURRENCY,
    PAYCODE_DESCRIPTION AS PAYMENT_TYPE,
    CASE
        WHEN paycode_description <> 'Netting' THEN AMOUNT_INV_CURR
    END    AS  PMT_AMT_IN_INV_CURR_EX_NETTING,
    AGING_BUCKETS AS AGING_BUCKETS,
    DAYS_LATE_PAID AS DAYS_LATE_PAID,
    PAY_STATUS_DISPLAYED_FIELD AS PAID_STATUS,
    INCREMENTAL_LATEST_DATE, 
    row_number() OVER (PARTITION BY ID ORDER BY GREATEST(NVL(PAYMENT_LAST_UPD_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')), NVL(INVOICE_LAST_UPD_DATE,to_timestamp('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))) DESC) AS UNIQUE_ID
     -- ,ROW_NUMBER() OVER(PARTITION BY CHECK_NUMBER, INVOICE_DATE, CREATION_DATE, INVOICE_ID, INVOICE_NUMBER, TERMS_DESCRIPTION, DUE_DATE, CHECK_DATE, CHECKCODE_DESCRIPTION, ORGANIZATIONUNIT_NAME,LEGAL_ENTITY_NAME, PARTY_NAME1, SUPPLIER_SEGMENT1, VENDOR_SITE_CODE, INVHEADER_LAST_UPDATE_DATE, LAST_UPDATE_DATE, INVHEADER_DESCRIPTION, PAYCODE_DESCRIPTION, AGING_BUCKETS, DAYS_LATE_PAID, TERMS_ID   
     -- ORDER BY CHECK_NUMBER ASC, INVOICE_DATE ASC, CREATION_DATE ASC, INVOICE_ID ASC, INVOICE_NUMBER ASC, TERMS_DESCRIPTION ASC, DUE_DATE ASC, CHECK_DATE ASC, CHECKCODE_DESCRIPTION ASC, ORGANIZATIONUNIT_NAME ASC, LEGAL_ENTITY_NAME ASC, PARTY_NAME1 ASC, SUPPLIER_SEGMENT1 ASC, VENDOR_SITE_CODE ASC, INVHEADER_LAST_UPDATE_DATE ASC, LAST_UPDATE_DATE ASC, INVHEADER_DESCRIPTION ASC, PAYCODE_DESCRIPTION ASC, AGING_BUCKETS ASC, DAYS_LATE_PAID ASC, TERMS_ID ASC) AS DUPLICATES_CHECK
    FROM VIEW_A  )
    SELECT
    ID,
    CHECK_NUMBER,
    INVOICE_DATE,
    INVOICE_ENTERED_DATE,
    INVOICE_ID,
    INVOICE_NUMBER,
    PAYMENT_TERMS,
    DUE_DATE,
    PAYMENT_DATE,
    PAYMENT_STATUS,
    BU_NAME,
    LEGAL_ENTITY_NAME,
    SUPPLIER_NAME,
    SUPPLIER_NUMBER,
    SUPPLIER_SITE,
    PAYMENT_AMOUNT,
    INVOICE_LAST_UPD_DATE,
    PAYMENT_LAST_UPD_DATE,
    INVOICE_DESC,
    PAID_AMOUNT_IN_INVOICE_CURRENCY,
    PAYMENT_TYPE,
    PMT_AMT_IN_INV_CURR_EX_NETTING,
    AGING_BUCKETS,
    DAYS_LATE_PAID,
    PAID_STATUS,
    INCREMENTAL_LATEST_DATE

    FROM VIEW_B

    WHERE UNIQUE_ID = 1


