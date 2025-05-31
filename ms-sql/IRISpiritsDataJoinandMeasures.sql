/****** Object:  StoredProcedure [EXT_IRI].[Update_NKA_Spirits_Review]    Script Date: 17/05/2023 11:17:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




-- =============================================
-- Author:      Daniel Aguayo
-- Create Date: 15/03/2022
-- Description: Create Table EXT_IRI.NKA_Spirits_Review
--				Join IRI Spirits Fact Table and Dimensions and calculate relevant RevUp measures. 
--				Filters are supposed to stay fixed. 
--				
-- =============================================


ALTER PROCEDURE [EXT_IRI].[Update_NKA_Spirits_Review] AS

BEGIN

IF OBJECT_ID('tempdb..#SpiritsTemp') IS NOT NULL DROP TABLE #SpiritsTemp;

TRUNCATE TABLE EXT_IRI.NKA_Spirits_Review; 

SELECT * INTO #SpiritsTemp FROM  
(
SELECT 

		 S_Date.[Time_Date]
		,S_Date.[TimeId]
		,S_Market.[Market Description] as Market_Description
		,S_Market.[MarketId]
		,S_Product.[Product Description] as Product_Description
		,S_Product.[ProductId]
		,S_Product.[Manufacturer]
		,S_Product.[Aztec Code] as Aztec_Code
		,CASE WHEN S_Product.[EAN] IS NULL THEN 'N/A'
			  ELSE S_Product.[EAN] END AS [EAN]
		,S_Product.[Trademark]
		,S_Product.[Brand]
		,S_Product.[Category]
		,S_Product.[Subcategory]
		,S_Product.[Malt/Blended] as Malt_Blended
		,S_Product.[Global]
		,S_Product.[Size]
		,S_Product.[Multiple]
		,S_Product.[Brand] as BrandQuality
		,CASE 
		WHEN S_Product.Trademark = 'Other Trademark' THEN S_Product.Brand
		ELSE S_Product.Trademark 
		END AS BrandCompetitor
		,CAST(S_Sales.[Dollars (000s)] AS decimal(10,6)) AS Dollars_000
		,CAST(S_Sales.[9 Litre Equiv] AS decimal(10,6)) AS Nine_ltr_Equivalient
		,CAST(S_Sales.[Quarter Weighted Distribution] AS decimal(10,6)) AS Qrtr_Weighted_Distribution
		FROM [EXT_IRI].[Spirits_Part_1] S_Sales
		LEFT JOIN  
			(SELECT
			X.*,
			DATEFROMPARTS(CONCAT(20,right([Time Description],2)),CONVERT(int,SUBSTRING([Time Description],4,2)),left([Time Description],2)) as Time_Date
			FROM [EXT_IRI].[Spirits_Part_1_tim] X
			WHERE [Time Description] not like 'MAT%')  S_Date
		ON S_Date.TimeId = S_Sales.TimeId 
		LEFT JOIN [EXT_IRI].[Spirits_Part_1_mkt] S_Market
		ON S_Market.MarketId = S_Sales.MarketId 
		LEFT JOIN [EXT_IRI].[Spirits_Part_1_prd] S_Product 
		ON S_Product.ProductId = S_Sales.ProductId		
		LEFT JOIN [EXT_IRI].[Aztec_Pcode] AZMAP
		ON S_Product.[Aztec Code] = AZMAP.AZTEC_CODE
		WHERE S_Sales.TimeId > 2 and S_Market.[Market Description] in ( SELECT DISTINCT Market_Description FROM  [EXT_IRI].[BannerMapping]) 
)X ;


INSERT INTO EXT_IRI.NKA_Spirits_Review 

SELECT
X.Time_Date as Aztec_Date
,X.TimeId AS Aztec_TimeID
,X.MarketId AS Aztec_MarketID
,X.ProductId AS Aztec_ProductID
,X.Start_Date_Wed as RevUp_StartDate
,X.End_Date_Tue as RevUp_EndDate
,X.Market_Description
,X.Banner
,X.Product_Description
,X.Manufacturer
,X.ProdCat
,X.ProductCategoryCode
,CASE 
	WHEN X.PCODE IS NULL THEN 'N/A'
	ELSE X.PCODE
END AS Prod_ERPCode
,X.Aztec_Code
,CASE 
		 WHEN X.EAN IS NULL THEN 'N/A'
			WHEN X.EAN  = 'Na' THEN 'N/A'
			ELSE X.EAN 
END 
AS EAN
,CASE 
	WHEN X.Manufacturer = 'Pernod Ricard' THEN ISNULL(CONVERT(varchar(50), X.PCODE),'N/A')
	ELSE ISNULL(CONVERT(varchar(50),X.Aztec_Code),'N/A')
END 
AS Item
,X.Trademark
,X.Brand
,X.BrandQuality
,X.BrandQualityCode
,X.BrandCompetitor
,X.BrandCompetitorCode
,X.Category
,X.Subcategory
,X.Malt_Blended
,X.Global
,X.Size
,X.Multiple
,X.Dollars
,X.Litres
,X.Price_per_litre
,X.Qrtr_Weighted_Distribution
,CASE
	WHEN X.Dollars > 0 AND X.Litres > 0 THEN 'Inc' 
	ELSE 'Exc'
END 
AS Inc_Exc


FROM
(
SELECT

Y.*
,Y.Dollars_000*1000 as Dollars
,Y.Nine_ltr_Equivalient*9 as Litres
,CASE
WHEN (Y.Dollars_000*1000)/NULLIF((Y.Nine_ltr_Equivalient*9),0) IS NULL THEN 0
ELSE (Y.Dollars_000*1000)/NULLIF((Y.Nine_ltr_Equivalient*9),0)
END
AS Price_per_litre
,DATEADD(day,-4,Y.Time_Date)  AS Start_Date_Wed
,DATEADD(day,+2,Y.Time_Date) AS  End_Date_Tue
,Pcat.code as ProductCategoryCode
,AZMAP.PCODE
,BM.Banner
,BQ.BrandQualityCode
,BC.BrandCompetitorCode
,Pcat.[Spirits_ProdCat] AS ProdCat

FROM
#SpiritsTemp Y


LEFT JOIN (SELECT * FROM [EXT_IRI].[ProductCatSpirits_D] PCIRI 
			LEFT JOIN [EXT_IRI].[ProductCategory_D] PC
			ON  PCIRI.Spirits_ProdCat = PC.[name]) Pcat
ON CONCAT(Y.[Category],Y.[Subcategory],Y.[Global],CASE WHEN Y.[Subcategory] = 'Scotch Whisky'  THEN Y.[Malt_Blended] ELSE '' END) = Pcat.[Spirits_ProdCat_Key]

LEFT JOIN  [EXT_IRI].[Aztec_Pcode] AZMAP
ON Y.Aztec_Code = AZMAP.AZTEC_CODE

LEFT JOIN [EXT_IRI].[BannerMapping] BM
ON Y.Market_Description = BM.Market_Description

LEFT JOIN [EXT_IRI].[REF_BrandCompetitor] BC
ON Y.BrandCompetitor = BC.BrandCompetitor

LEFT JOIN [EXT_IRI].[REF_BrandQuality] BQ
ON Y.BrandQuality = BQ.BrandQuality

) X


DROP TABLE  #SpiritsTemp    



END
GO


