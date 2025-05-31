/****** Object:  StoredProcedure [ZZINT_REVUP].[Update_PROMAX_DateRange]    Script Date: 17/05/2023 11:17:51 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




-- =============================================
-- Author:      Daniel Aguayo
-- Create Date: 19/05/2022
-- Description: Truncate/Update Table Update_PROMAX_DateRange. This table is the base of all PROMAX extraxts to be uploaded in RevUP.
--				This Procedure helps group promotions that are same/similar in consecutive weeks. 
--				It reads the relevant promax Data needed for revup, split it by week and then group it by date ranges based on keys. 
--				Filters on PROMAX Data are fixed as they are not meant to change. Data starting from 28/09/2022 because this is the data the data started to be entered right into the system 
-- =============================================


ALTER PROCEDURE [ZZINT_REVUP].[Update_PROMAX_DateRange] AS

BEGIN

IF OBJECT_ID('tempdb..#PromaxTemp1') IS NOT NULL DROP TABLE #PromaxTemp1;
IF OBJECT_ID('tempdb..#PromaxTemp2') IS NOT NULL DROP TABLE #PromaxTemp2;

TRUNCATE TABLE [ZZINT_REVUP].[PROMAX_DateRange];

SELECT * INTO #PromaxTemp1 FROM   --Query to cast and transform base Promax data. 

(

SELECT

Z.ac_code
, CASE 
WHEN Z.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
ELSE Z.sku_profitcentre END 
AS BQ_Code
,Z.BottleSize
,Z.p_startdate
,Z.p_stopdate
,Z.p_confirmed
,Z.p_closed
,MAX(Z.sku_uompersaleable) AS sku_uompersaleable  --- required to avoid duplication of records 08/062022 there are promotions registered with different values here in same dates, same BQS
,Z.pt_longname
,MAX(Z.pisd_rate) AS max_psid_rate
,SUM(Z.pisd_accruedamount) as sum_accruedamount
,SUM(Z.pisd_paidamount) AS sum_paid_amount
,SUM(Z.pisd_quantity) AS sum_psid_quantity
,SUM(Z.pisd_amount) AS sum_psidamount
,MAX(Z.pis_featurepriceplan) as max_featurepriceplan
,Z.Start_Wed
,DATEADD(day,Z.Duration-1, Z.Start_Wed) 
AS End_Tue
,Z.Duration
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) 
AS Weeks
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) 
AS Original_Weeks
,CONCAT(Z.ac_code,CASE 
	WHEN Z.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END ,Z.BottleSize,MAX(Z.pisd_rate),pt_longname) 
AS SKU_KEY
,CONCAT(Z.ac_code,CASE 
	WHEN Z.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END ,Z.BottleSize,MAX(Z.pisd_rate),Z.pt_longname,CONVERT(varchar(10),Z.Start_Wed,12),CONVERT(varchar(10),DATEADD(day,Z.Duration-1,Z.Start_Wed),12)) 
AS WEEK_KEY

FROM(

SELECT 
 pg_rowid
,p_rowid
,pg_longname
,ag_shortname
,pt_longname 
,p_promonumber
,p_note
,p_fieldnote
,p_startdate
,p_stopdate
,p_pubid
,ac_code
,ac_longname
,gltt_shortname
,gltt_longname
,sku_stockcode
,sku_longname
,sku_casenetweight
,sku_profitcentre
,sku_uompersaleable
,PXMap.level_11_name
,PXMap.level_11_id
,PXMap.BottleSize
,PXMap.CaseConfig
,pis_shelfpriceplan
,pis_featurepriceplan
,pis_deleted
,pis_skurowid
,pisd_accruedamount
,pisd_quantityuom
,pisd_quantity
,pisd_rate
,pisd_amount
,pisd_deleted
,pisd_paidamount
,pisd_forecastspend
,p_confirmed				
,p_closed
,CASE 
	WHEN DATEPART(WEEKDAY,p_startdate) = 4 THEN p_startdate
	ELSE DATEADD(day,4-DATEPART(WEEKDAY,p_startdate),p_startdate) 
END AS Start_Wed  
,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7,0) 
END)*7 
AS Duration


FROM QLIK_EXT_PROMAXPD.promoitemskudetail

LEFT JOIN QLIK_EXT_PROMAXPD.gltype
ON pisd_typerowid = gltt_rowid AND pisd_typepubid = gltt_pubid
LEFT JOIN QLIK_EXT_PROMAXPD.promoitemsku
ON pis_rowid = pisd_promoitemskurowid AND pis_pubid = pisd_promoitemskupubid
LEFT JOIN QLIK_EXT_PROMAXPD.sku
ON pis_skurowid = sku_rowid and pis_skupubid = sku_pubid
LEFT JOIN QLIK_EXT_PROMAXPD.promotion
ON  p_rowid = pis_promotionrowid AND p_pubid = pis_promotionpubid 
LEFT JOIN QLIK_EXT_PROMAXPD.account_raw
ON p_accountrowid = ac_rowid and p_accountpubid= ac_pubid    
LEFT JOIN QLIK_EXT_PROMAXPD.promotionmember
ON  p_rowid = pm_promotionrowid AND p_pubid = pm_promotionpubid
LEFT JOIN QLIK_EXT_PROMAXPD.promotiongroup
ON pm_promotiongrouprowid = pg_rowid AND pm_promotiongrouppubid = pg_pubid
LEFT JOIN QLIK_EXT_PROMAXPD.activity
on p_activityrowid = pt_rowid AND p_activitypubid = pt_pubid  
LEFT JOIN QLIK_EXT_PROMAXPD.activitygroup
ON pt_activitygrouprowid = ag_rowid AND pt_activitygrouppubid = ag_pubid 
LEFT JOIN  (SELECT         /**PXMap Logic designed by Michael Howard ***/
			level_12_id, level_12_name, 
			level_11_id, level_11_name, 
			level_10_id, level_10_name, 
			sku_shortname AS PCode, 
			ROUND(sku_casenetweight,4) AS BottleSize, 
			sku_uompersaleable AS CaseConfig, 
			sku_profitcentre AS Fastar, 
			sku_accesscode AS [Status]
			FROM (SELECT 
					ph_skurowid, ph_rowid AS level_12_id, 
					ph_longname AS level_12_name, 
					ph_producthierarchyrowid AS level_11
				  FROM QLIK_EXT_PROMAXPD.producthierarchy
				  WHERE ph_level = 12  
				  ) AS T12 
				  LEFT JOIN (SELECT 
								ph_rowid AS level_11_id, 
								ph_longname AS level_11_name, 
								ph_producthierarchyrowid AS level_10
								FROM QLIK_EXT_PROMAXPD.producthierarchy
							) AS T11
				  ON T12.level_11 = T11.level_11_id  

				  LEFT JOIN (SELECT 
								ph_rowid AS level_10_id, 
								ph_longname AS level_10_name, 
								ph_producthierarchyrowid AS level_9
								FROM QLIK_EXT_PROMAXPD.producthierarchy
							) AS T10
				  ON T11.level_10 = T10.level_10_id

				  LEFT JOIN (SELECT sku_shortname, sku_casenetweight, sku_uompersaleable, sku_profitcentre, sku_accesscode, sku_rowid FROM QLIK_EXT_PROMAXPD.sku) AS TS
				  ON T12.ph_skurowid = TS.sku_rowid
				) AS PXMap
ON PXMap.PCode = sku_stockcode

WHERE 
 p_deleted <> 1  AND p_confirmed = 1
AND pis_deleted <> 1
AND pisd_deleted <> 1 AND pisd_amount > 0
AND ac_code not in ('BA-ALHH','BA-DMPH')  
AND gltt_shortname  LIKE '%'
AND PXMap.level_11_name LIKE  '%'
AND gltt_longname = 'Case Deal Deferred'
AND  (p_closed <> 1 OR pisd_accruedamount <> 0)
AND pt_longname NOT IN ('5. EDD_QA')
) Z

WHERE Start_Wed >= '2022-10-05'     ---start date required as prior this all data has been manually manipulated in Rev-Up


GROUP BY
Z.BottleSize
,Z.p_startdate
,Z.p_stopdate
,Z.p_confirmed
,Z.p_closed
,CASE 
WHEN Z.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
ELSE Z.sku_profitcentre END
--,Z.sku_uompersaleable  --- required to avoid duplication of records 08/062022 there are promotions registered with different values here in same dates, same BQS
,Z.BottleSize
,Z.ac_code
,Z.Start_Wed
,Z.pt_longname
,DATEADD(day,Z.Duration-1, Z.Start_Wed) 
,Z.Duration
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) 

) Y;


;WITH CTE_PXWEEKS as   --CTE to split Promax Base Data per week. 
(
SELECT 
*
FROM #PromaxTemp1

UNION ALL

SELECT
cte.ac_code
,cte.BQ_Code
,cte.BottleSize
,cte.p_startdate
,cte.p_stopdate
,cte.p_confirmed
,cte.p_closed
,cte.sku_uompersaleable
,cte.pt_longname
,cte.max_psid_rate
,cte.sum_accruedamount
,cte.sum_paid_amount
,cte.sum_psid_quantity
,cte.sum_psidamount
,cte.max_featurepriceplan
,cte.Start_Wed
,cte.End_Tue
,cte.Duration
,(cte.Weeks -1)
,cte.Original_Weeks
,cte.SKU_KEY
,cte.WEEK_KEY


FROM CTE_PXWEEKS cte INNER JOIN #PromaxTemp1 PX
ON  cte.WEEK_KEY = PX.WEEK_KEY
WHERE cte.Weeks > 1

)


SELECT * INTO #PromaxTemp2 FROM
(
SELECT
ROW_NUMBER() OVER (ORDER BY SKU_KEY ASC, New_Start_Wed ASC) AS ROWNUMBER
,CONCAT(ac_code,BQ_Code,BottleSize,Rate,pt_longname,CONVERT(varchar(10),New_Start_Wed,12),CONVERT(varchar(10),New_End_Tue,12)) 
AS ROW_KEY
,X.* 
FROM
(

SELECT 
ac_code
,BQ_Code
,PK.sku_longname
,PK.sku_stockcode
,BottleSize
,p_confirmed
,p_closed
,pt_longname
,sku_uompersaleable
,max_psid_rate as Rate
,max_featurepriceplan as featurePrice
,CASE 
WHEN CAST((NULLIF(sum_accruedamount,0)/Original_weeks) as float) IS NULL THEN 0 
ELSE CAST((NULLIF(sum_accruedamount,0)/Original_weeks) as float) END
as Accrued_Amount
,CASE 
WHEN CAST((NULLIF(sum_paid_amount,0)/Original_weeks) as float) IS NULL THEN 0
ELSE CAST((NULLIF(sum_paid_amount,0)/Original_weeks) as float) END
as Paid_Amount
,CAST((NULLIF(cast(sum_psid_quantity as float),0)/Original_weeks) as float) as Case_Qty
,sum_psid_quantity as real_Case
,CASE WHEN CAST((NULLIF(sum_psidamount,0)/Original_weeks) as float) IS NULL THEN 0
ELSE CAST((NULLIF(sum_psidamount,0)/Original_weeks) as float) END
as Amount
,Start_Wed
,End_Tue
,CASE 
WHEN ROW_NUMBER() OVER (PARTITION BY WEEK_KEY ORDER BY WEEK_KEY) =1 
THEN Start_Wed 
ELSE DATEADD(DAY,(ROW_NUMBER() OVER (PARTITION BY WEEK_KEY ORDER BY WEEK_KEY)-1)*7,Start_Wed) END 
AS New_Start_Wed
,DATEADD(DAY,(ROW_NUMBER() OVER (PARTITION BY WEEK_KEY ORDER BY WEEK_KEY)*7)-1,Start_Wed) 
AS New_End_Tue
,ROW_NUMBER() OVER (PARTITION BY WEEK_KEY ORDER BY WEEK_KEY) 
AS Week_ID
,Original_Weeks
,SKU_KEY
,WEEK_KEY

FROM CTE_PXWEEKS cte

LEFT JOIN [ZZINT_REVUP].[V_PROMAX_REF_longsku]  PK
ON cte.SKU_KEY = PK.SKU_ID  )X

)Z ;




;WITH CTEDATES1      --CTE to group the promax weeks by a date range based on a SKU_KEY
AS
(
   SELECT #PromaxTemp2.*, 1 AS GROUPID FROM #PromaxTemp2 WHERE ROWNUMBER=1
   UNION ALL
   SELECT A.*, CASE WHEN DATEDIFF(d, B.New_Start_Wed,A.New_Start_Wed) = 7 AND A.SKU_KEY = B.SKU_KEY THEN B.GROUPID ELSE B.GROUPID+1 END AS GAP FROM #PromaxTemp2 A INNER JOIN CTEDATES1 B ON A.ROWNUMBER-1 = B.ROWNUMBER
)

INSERT INTO [ZZINT_REVUP].[PROMAX_DateRange]

SELECT

* 

FROM(

SELECT 
 ac_code
,BQ_Code
,sku_longname
,sku_stockcode
,cast(BottleSize as decimal(10,3)) as Size  --EPOS KEY requires this BottleSize type to match 10,2 rounding
,p_confirmed
,p_closed
,pt_longname
,sku_uompersaleable
,Rate
,SUM(Accrued_Amount) as Accrued_Amount
,SUM(Paid_Amount) as Paid_Amount
,SUM(Case_Qty) as Case_Quantity
,SUM(Amount) as Amount
,MAX(featurePrice) as featurePrice
,SKU_KEY
,MIN(New_Start_Wed) as startdate
,CASE WHEN MAX(New_Start_Wed) = MAX(New_Start_Wed) THEN MAX(New_End_Tue) ELSE MAX(New_Start_Wed) END AS enddate 
,CONCAT(ac_code,'_',BQ_Code,'_',cast(BottleSize as decimal(10,3)),'_',CONVERT(varchar(10),MIN(New_Start_Wed),12)) AS ShopperOffer
,CONCAT(ac_code,' ','SB','-',BQ_Code,' ',cast(BottleSize as decimal(10,3))) as EPOS_KEY
,COUNT(CONCAT(ac_code,'_',BQ_Code,'_',cast(BottleSize as decimal(10,3)),'_',CONVERT(varchar(10),MIN(New_Start_Wed),12))) OVER (PARTITION BY CONCAT(ac_code,'_',BQ_Code,'_',cast(BottleSize as decimal(10,3)),'_',CONVERT(varchar(10),MIN(New_Start_Wed),12))) ShopperOfferCount

FROM CTEDATES1 

GROUP BY 
ac_code
,BQ_Code
,sku_longname
,sku_stockcode
,pt_longname
,cast(BottleSize as decimal(10,3))
,Rate
,p_confirmed
,p_closed
,sku_uompersaleable
,SKU_KEY
,GROUPID
,CONCAT(ac_code,' ','SB','-',BQ_Code,' ',cast(BottleSize as decimal(10,3))) 

) Z

OPTION (maxrecursion 0)

DROP TABLE #PromaxTemp1;
DROP TABLE #PromaxTemp2;

END
GO


