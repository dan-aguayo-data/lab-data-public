-- Query latest date from sales transactions
SELECT 
    MAX(DD.FULL_DATE)
FROM {{ ref('FT_TransactionAdjustments') }} F
LEFT JOIN {{ ref('D_Date') }} DD
    ON DD.DATE_KEY = F.DATE_KEY;

-- Query sales details with date filter
SELECT 
    F.COMPANY_ID,
    F.ORDER_CATEGORY_ID,
    F.ORDER_CODE,
    F.LINE_ITEM_NUM,
    F.LINE_CATEGORY_ID,
    F.BILLING_CODE,
    F.BILLING_TYPE_ID,
    F.BUYER_ACCOUNT_ID,
    F.DELIVERY_ACCOUNT_ID,
    F.RECORD_DATE_KEY,
    F.PRODUCT_ID,
    F.FACILITY_ID,
    F.QUANTITY_DELIVERED,
    F.QUANTITY_UOM_ID,
    F.QUANTITY_TOTAL,
    F.QUANTITY_UNITS,
    F.CURRENCY_ID,
    F.TOTAL_SALE_AMOUNT,
    F.BILLED_AMOUNT,
    F.TAX_AMOUNT,
    F.DUTY_AMOUNT,
    F.QA_SCORE,
    F.BONUS_QUANTITY,
    F.TAX_CALCULATION,
    F.COST_OF_SALES,
    F.DISTRIBUTION_COST,
    F.AREA_ID,
    F.REGION_ID,
    F.COMPLETED_FLAG,
    F.LOAD_TIMESTAMP,
    DD.FULL_DATE
FROM {{ ref('FT_SalesRecords') }} F
LEFT JOIN {{ ref('D_Date') }} DD
    ON DD.DATE_KEY = F.RECORD_DATE_KEY
WHERE DD.FULL_DATE >= '2023-01-01';

-- Query view dependencies
SELECT DISTINCT 
    SCHEMA_NAME(v.schema_id) AS schema_name,
    v.name AS view_name,
    SCHEMA_NAME(o.schema_id) AS ref_schema_name,
    o.name AS ref_entity_name,
    o.type_desc AS entity_type
FROM sys.views v
JOIN sys.sql_expression_dependencies d
    ON d.referencing_id = v.object_id
    AND d.referenced_id IS NOT NULL
JOIN sys.objects o
    ON o.object_id = d.referenced_id
WHERE o.name IN (
    'FT_SalesSummary_Monthly',
    'FT_SalesSummary_Weekly',
    'FT_SalesOther',
    'FT_SalesRecords',
    'FT_TransactionAdjustments'
)
ORDER BY schema_name, view_name;