


SELECT 

   
	  max(DD.FULL_DATE)
	  
	  FROM [DWH].[FT_SalesDiscount] F
left join DWH.D_Date DD
ON DD.DATE_KEY = F.DATE_KEY

--WHERE DD.FULL_DATE >= '2022-11-01'



-- [DWH].[FT_SalesOther] 2022-12-19
-- [DWH].[FT_SalesDirect]2022-12-21
-- [DWH].[FT_SalesDiscount] 2022-03-01
-- [DWH].[FT_SalesSummary_Monthly] 2022-12-01
--[DWH].[FT_SalesSummary_Weekly]  2022-12-19

SELECT 

[COMPANY_KEY]
      ,[ORDER_TYPE_KEY]
      ,[ORDER_NUM]
      ,[ORDER_LINE_NUM]
      ,[LINE_TYPE_KEY]
      ,[INVOICE_NUM]
      ,[INVOICE_TYPE_KEY]
      ,[SELL_TO_CUST_ACCT_KEY]
      ,[SHIP_TO_CUST_ACCT_KEY]
      ,[GL_DATE_KEY]
      ,[ITEM_KEY]
      ,[BRANCH_PLANT_KEY]
      ,[QTY_SHIPPED]
      ,[QTY_SHIPPED_UOM_KEY]
      ,[QTY_IN_LT]
      ,[QTY_IN_UNIT]
      ,[LOCAL_CURRENCY_KEY]
      ,[EXT_GSV_AMT_LOCAL]
      ,[INV_AMT_LOCAL]
      ,[EXCISE_AMT_LOCAL]
      ,[WET_AMT_LOCAL]
      ,[QA2]
      ,[BONUS_QTY_IN_LT]
      ,[EXCISE_CALC]
      ,[COGS_CALC]
      ,[DIST_COST_CALC]
      ,[TRANS_TERRITORY_KEY]
      ,[TRANS_BANNER_REGION_KEY]
      ,[PROCESSED_FLAG]
      ,[EXTRACT_DATE]
	  ,DD.FULL_DATE
	  
	  FROM [DWH].[FT_SalesDirect] F
left join DWH.D_Date DD
ON DD.DATE_KEY = F.GL_DATE_KEY

WHERE DD.FULL_DATE >= '2022-11-01'




select distinct schema_name(v.schema_id) as schema_name,
       v.name as view_name,
       schema_name(o.schema_id) as referenced_schema_name,
       o.name as referenced_entity_name,
       o.type_desc as entity_type
from sys.views v
join sys.sql_expression_dependencies d
     on d.referencing_id = v.object_id
     and d.referenced_id is not null
join sys.objects o
     on o.object_id = d.referenced_id

	 WHERE o.name IN  ('FT_SalesSummary_Monthly','FT_SalesSummary_Weekly','FT_SalesOther','FT_SalesDirect','FT_SalesDiscount')


 order by schema_name,
          view_name;
