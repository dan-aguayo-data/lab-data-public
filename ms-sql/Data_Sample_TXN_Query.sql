/* QUERY TO COMPARE UNIT DIFFERENCE BETWEEN DATAWAREHOUSE SOLUTION VS LEGACY ERP TABLE ON FULL_DATE */

DECLARE @FISCAL_YEAR AS VARCHAR(50) = '2024';
DECLARE @FISCAL_PERIOD AS INT = 4;

PRINT @FISCAL_YEAR;
PRINT @FISCAL_PERIOD;

SELECT 
    NEW.RECORD_DATE AS RECORD_DATE_DW,
    NEW.Quantity_Sum_DW,
    ERP.QUANTITY_ERP,
    (ERP.QUANTITY_ERP - NEW.Quantity_Sum_DW) AS "Variance ERP vs DW"
FROM (
    SELECT
        X.RECORD_DATE,
        SUM(X.quantity) AS Quantity_Sum_DW,
        SUM(X.value) AS Value_Sum_DW
    FROM (
        SELECT 
            f.RECORD_DATE,
            f.DOCUMENT_ID,
            d.FISCAL_YEAR,
            d.FISCAL_MONTH,
            o.ACCOUNT_CODE,
            b.LOCATION_KEY AS DIM_LOCATION_KEY,
            f.LOCATION_KEY AS FACT_LOCATION_KEY,
            b.LOCATION_CODE,
            p.PRODUCT_KEY AS DIM_PRODUCT_KEY,
            f.PRODUCT_KEY AS FACT_PRODUCT_KEY,
            p.PRODUCT_CODE,
            u.DEPARTMENT_CODE,
            d.FULL_DATE,
            f.QUANTITY,
            f.UNIT_TYPE,
            f.DESCRIPTION,
            p.ITEM_SIZE,
            f.AMOUNT,
            u.DEPARTMENT_OWNER_CODE,
            p.CATEGORY_CODE,
            u.DEPARTMENT_OWNER_NAME,
            f.AMOUNT_YTD AS value,
            f.QUANTITY AS quantity
        FROM {{ ref('FT_TransactionLedger_DW') }} f
        LEFT JOIN {{ ref('D_Date_DW') }} d ON f.DATE_KEY = d.DATE_KEY
        LEFT JOIN {{ ref('D_Account_DW') }} o ON f.ACCOUNT_KEY = o.ACCOUNT_KEY
        LEFT JOIN {{ ref('D_Product_DW') }} p ON f.PRODUCT_KEY = p.PRODUCT_KEY
        LEFT JOIN {{ ref('D_Location_DW') }} b ON f.LOCATION_KEY = b.LOCATION_KEY
        LEFT JOIN {{ ref('D_Department_DW') }} u ON f.DEPARTMENT_KEY = u.DEPARTMENT_KEY
        WHERE f.FISCAL_YEAR = @FISCAL_YEAR
            AND d.FISCAL_MONTH = @FISCAL_PERIOD
    ) X
    GROUP BY X.RECORD_DATE
) NEW
LEFT JOIN (
    SELECT
        X.FULL_DATE AS FULL_DATE_ERP,
        SUM(X.Quantity) AS QUANTITY_ERP
    FROM (
        SELECT 
            d1.FULL_DATE,
            d1.FISCAL_YEAR,
            d1.FISCAL_MONTH,
            c.COMPANY_KEY AS DOC_COMPANY_KEY,
            ISNULL(t.ORDER_TYPE_KEY, '-1') AS DOC_TYPE_KEY,
            e.DOC_TYPE_CODE,
            CONVERT(INT, CAST(e.DOC_NUM AS DECIMAL)) AS DOC_NUM,
            ISNULL(d1.DATE_KEY, '-1') AS RECORD_DATE_KEY,
            CAST(e.LINE_NUM AS DECIMAL(18,2)) AS LINE_NUM,
            e.LINE_EXTENSION_CODE,
            ISNULL(l.LEDGER_TYPE_KEY, '-1') AS LEDGER_TYPE_KEY,
            CAST(CONVERT(DOUBLE PRECISION, e.QUANTITY)/100 AS NUMERIC(16,2)) AS Quantity,
            CAST(CONVERT(DOUBLE PRECISION, e.AMOUNT)/100 AS NUMERIC(16,2)) AS Amount
        FROM {{ source('LEGACY_ERP', 'TRANSACTION_TABLE') }} e
        LEFT JOIN {{ ref('D_Company_DW') }} c ON e.COMPANY_CODE = c.COMPANY_CODE
        LEFT JOIN {{ ref('D_OrderType_DW') }} t ON t.ORDER_TYPE_CODE = e.DOC_TYPE_CODE
        LEFT JOIN {{ ref('D_Date_DW') }} d1 ON CAST({{ dbt_utils.date_trunc('day', 'e.RECORD_DATE') }} AS DATETIME) = d1.FULL_DATE
        LEFT JOIN {{ ref('D_Date_DW') }} d2 ON CAST({{ dbt_utils.date_trunc('day', 'e.SERVICE_DATE') }} AS DATETIME) = d2.FULL_DATE
        LEFT JOIN {{ ref('D_LedgerType_DW') }} l ON l.LEDGER_TYPE_CODE = e.LEDGER_CODE
        LEFT JOIN {{ ref('D_Department_DW') }} u ON u.DEPARTMENT_CODE = e.DEPARTMENT_CODE
        LEFT JOIN {{ ref('D_Account_DW') }} o ON o.ACCOUNT_CODE = CONCAT(e.ACCOUNT_NUM, e.SUB_ACCOUNT)
        LEFT JOIN {{ ref('D_Product_DW') }} p ON p.PRODUCT_CODE = e.ITEM_CODE
        LEFT JOIN {{ ref('D_Location_DW') }} b ON CAST(e.LOCATION_NUM AS NVARCHAR(50)) = b.LOCATION_CODE
        WHERE d1.FISCAL_YEAR = @FISCAL_YEAR
            AND d1.FISCAL_MONTH = @FISCAL_PERIOD
            AND e.LEDGER_CODE = 'GL'
    ) X
    GROUP BY X.FULL_DATE
) ERP
ON ERP.FULL_DATE_ERP = NEW.RECORD_DATE
GROUP BY 
    NEW.RECORD_DATE,
    NEW.Quantity_Sum_DW,
    ERP.QUANTITY_ERP
ORDER BY NEW.RECORD_DATE;