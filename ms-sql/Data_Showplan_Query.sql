/****** Script for SelectTopNRows command from SSMS ******/

SET SHOWPLAN_ALL ON;
GO

-- Query to retrieve top records
SELECT TOP (1000) 
    *
FROM {{ ref('V_FT_FinancialSummary_DW') }};

GO
SET SHOWPLAN_ALL OFF;
GO