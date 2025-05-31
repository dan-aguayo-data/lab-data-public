USE PROMAX_PD

IF OBJECT_ID('tempdb..#PROMAX_SKUKEY_VIEW') IS NOT NULL DROP TABLE #PROMAX_SKUKEY_VIEW;
IF OBJECT_ID('tempdb..#PromaxTemp1') IS NOT NULL DROP TABLE #PromaxTemp1;
IF OBJECT_ID('tempdb..#PromaxTemp2') IS NOT NULL DROP TABLE #PromaxTemp2;



SELECT * INTO #PROMAX_SKUKEY_VIEW FROM

(


SELECT DISTINCT

Unique_ID
,sku_longname

FROM(

SELECT 
 AC.ac_code as ac_code
 , CASE 
	WHEN PXMap.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE PIS.sku_profitcentre END 
AS BQ_Code 
,PXMap.BottleSize as Bottlesize
,PX.p_startdate
,PX.p_stopdate
,PX.p_confirmed				
,PX.p_closed
,PIS.sku_stockcode
,PIS.sku_longname
,PIS.sku_uompersaleable
,MAX(PIS.pisd_rate) as Case_Rate
,CONCAT(AC.ac_code, CASE 
	WHEN PXMap.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE PIS.sku_profitcentre END ,PXMap.BottleSize,MAX(PIS.pisd_rate),CASE
WHEN PX.p_closed = 1 and SUM(PIS.pisd_paidamount) = 0 then 'x' else '' end) as Unique_ID
,ROW_NUMBER() OVER(PARTITION BY CONCAT(AC.ac_code, CASE 
	WHEN PXMap.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE PIS.sku_profitcentre END ,PXMap.BottleSize,MAX(PIS.pisd_rate),CASE
WHEN PX.p_closed = 1 and SUM(PIS.pisd_paidamount) = 0 then 'x' else '' end) ORDER BY DATEDIFF(day,PX.p_startdate,PX.p_stopdate) DESC, PIS.sku_longname DESC) AS ID_RANK


FROM promotion AS PX   

LEFT JOIN account_raw AS AC
ON PX.p_accountrowid = AC.ac_rowid and PX.p_accountpubid= AC.ac_pubid    

LEFT JOIN ( SELECT * FROM activity
				LEFT JOIN activitygroup
				ON pt_activitygrouprowid = ag_rowid AND pt_activitygrouppubid = ag_pubid   
		 ) AS AG
ON  PX.p_activityrowid = AG.pt_rowid AND PX.p_activitypubid = AG.pt_pubid   

LEFT JOIN(SELECT *FROM promotionmember
			LEFT JOIN promotiongroup
			ON pm_promotiongrouprowid = pg_rowid AND pm_promotiongrouppubid = pg_pubid
		  )  AS PG
ON  PX.p_rowid = PG.pm_promotionrowid AND PX.p_pubid = PG.pm_promotionpubid

LEFT JOIN (SELECT * FROM promoitemsku
				LEFT JOIN (SELECT * FROM promoitemskudetail
								LEFT JOIN gltype
								ON pisd_typerowid = gltt_rowid AND pisd_typepubid = gltt_pubid) AS PISD
				 ON pis_rowid = PISD.pisd_promoitemskurowid AND pis_pubid = PISD.pisd_promoitemskupubid
			LEFT JOIN sku  ON pis_skurowid = sku_rowid and pis_skupubid = sku_pubid
		   ) AS PIS
ON PX.p_rowid = PIS.pis_promotionrowid AND PX.p_pubid = PIS.pis_promotionpubid 



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
				  FROM producthierarchy
				  WHERE ph_level = 12  
				  ) AS T12 
				  LEFT JOIN (SELECT 
								ph_rowid AS level_11_id, 
								ph_longname AS level_11_name, 
								ph_producthierarchyrowid AS level_10
								FROM producthierarchy
							) AS T11
				  ON T12.level_11 = T11.level_11_id  

				  LEFT JOIN (SELECT 
								ph_rowid AS level_10_id, 
								ph_longname AS level_10_name, 
								ph_producthierarchyrowid AS level_9
								FROM producthierarchy
							) AS T10
				  ON T11.level_10 = T10.level_10_id

				  LEFT JOIN (SELECT * FROM sku) AS TS
				  ON T12.ph_skurowid = TS.sku_rowid
				) AS PXMap
ON PXMap.PCode = PIS.sku_stockcode


WHERE 

PX.p_deleted <> 1  AND PX.p_confirmed = 1 
AND PIS.pis_deleted <> 1
AND PIS.pisd_deleted <> 1 AND PIS.pisd_amount <> 0
AND AC.ac_code not in ('BA-ALHH','BA-DMPH') 
AND PIS.gltt_shortname  LIKE '%'
AND PXMap.level_11_name LIKE  '%'
AND gltt_longname = 'Case Deal Deferred'

GROUP BY 

 AC.ac_code
 , CASE 
	WHEN PXMap.level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE PIS.sku_profitcentre END 
,PXMap.BottleSize
,PX.p_startdate
,PX.p_stopdate
,PX.p_confirmed				
,PX.p_closed
,PIS.sku_stockcode
,PIS.sku_longname
,PIS.sku_uompersaleable

) X

WHERE ID_RANK = 1 )Z ;



/* ##########################################################################################*/

SELECT * INTO #PromaxTemp1 FROM

(

SELECT

Z.ac_code
, CASE 
	WHEN Z.Level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END 
AS BQ_Code
,Z.BottleSize
,Z.p_startdate
,Z.p_stopdate
,Z.p_confirmed
,Z.p_closed
,Z.sku_uompersaleable
,MAX(Z.pisd_rate) AS max_psid_rate
,SUM(Z.pisd_accruedamount) as sum_accruedamount
,SUM(Z.pisd_paidamount) AS sum_paid_amount
,SUM(Z.pisd_quantity) AS sum_psid_quantity
,SUM(Z.pisd_amount) AS sum_psidamount

,Z.Start_Wed
,DATEADD(day,Z.Duration-1, Z.Start_Wed) AS End_Tue
,Z.Duration
,CASE
WHEN Z.p_closed = 1 and SUM(Z.pisd_paidamount) = 0 then 'x' else '' end as DUPLICATION_MARK
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) AS cte_Weeks
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) AS original_Weeks
,CONCAT(Z.ac_code,CASE 
	WHEN Z.Level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END ,Z.BottleSize,MAX(Z.pisd_rate),CASE WHEN Z.p_closed = 1 and SUM(Z.pisd_paidamount) = 0 then 'x' else '' end) AS SKU_KEY
,CONCAT(Z.ac_code,CASE 
	WHEN Z.Level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END ,Z.BottleSize,MAX(Z.pisd_rate),CONVERT(varchar(10),Z.Start_Wed,12),CONVERT(varchar(10),DATEADD(day,Z.Duration-1,Z.Start_Wed),12),CASE
WHEN Z.p_closed = 1 and SUM(Z.pisd_paidamount) = 0 then 'x' else '' end) AS KEY_WEEK_AGGREGATION




FROM(
SELECT 
 PG.pg_rowid
,PX.p_rowid
,PG.pg_longname
,AG.ag_shortname
,AG.pt_longname 
,PX.p_promonumber
,PX.p_note
,PX.p_fieldnote
,PX.p_startdate
,PX.p_stopdate
,PX.p_pubid
,AC.ac_code
,AC.ac_longname
,PIS.gltt_shortname
,PIS.gltt_longname
,PIS.sku_stockcode
,PIS.sku_longname
,PIS.sku_casenetweight
,PIS.sku_profitcentre
,PIS.sku_uompersaleable
,PXMap.level_11_name
,PXMap.level_11_id
,PXMap.BottleSize
,PXMap.CaseConfig
,PIS.pis_shelfpriceplan
,PIS.pis_featurepriceplan
,PIS.pis_deleted
,PIS.pis_skurowid
,PIS.pisd_accruedamount
,PIS.pisd_quantityuom
,PIS.pisd_quantity
,PIS.pisd_rate
,PIS.pisd_amount
,PIS.pisd_deleted
,PIS.pisd_paidamount
,PIS.pisd_forecastspend
,PX.p_confirmed				
,PX.p_closed
,CASE 
	WHEN DATEPART(WEEKDAY,p_startdate) = 4 THEN p_startdate
	ELSE DATEADD(day,4-DATEPART(WEEKDAY,p_startdate),p_startdate) 
END AS Start_Wed  
,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,PX.p_startdate,PX.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,PX.p_startdate,PX.p_stopdate))/7,0) 
END)*7 
AS Duration
,PIS.pisd_rate/PIS.sku_uompersaleable AS Rate_per_unit 
,PIS.pisd_quantity*PIS.sku_uompersaleable AS Qty_units 
,(PIS.pisd_quantity*PIS.sku_uompersaleable)*PIS.sku_casenetweight AS Qty_lts
,CASE
	WHEN  (CASE
		WHEN PIS.gltt_shortname = 'Case Defer' THEN 'PRU'
		WHEN PIS.gltt_shortname = 'Coop' THEN 'COM'
		ELSE 'NA' END) <> 'N/A' THEN 'FCP'
	ELSE 'N/A'	
END AS PromoDealDepthCode	
,CASE 
	WHEN PIS.gltt_shortname = 'Coop' THEN 'DIS'
	WHEN PX.p_closed = 1 OR (PIS.pisd_paidamount/NULLIF(PIS.pisd_accruedamount,0)) > .95001 THEN 'SOA'
	ELSE  'SOE'
	END 
	AS  PromoCostType
,ISNULL(PIS.pisd_paidamount/NULLIF(PIS.pisd_accruedamount,0),0) AS Percentage_Paid
,CASE 
	WHEN PX.p_closed =1  AND PIS.pisd_paidamount = 0 THEN ''
	ELSE CONCAT(pg_rowid,'-',sku_stockcode,'-',CONVERT(varchar (10),CASE 
																		WHEN DATEPART(WEEKDAY,p_startdate) = 4 THEN p_startdate
																		ELSE DATEADD(day,4-DATEPART(WEEKDAY,p_startdate),p_startdate) 
																		END, 12)) 
END AS ShopperID


FROM promotion AS PX   

LEFT JOIN account_raw AS AC
ON PX.p_accountrowid = AC.ac_rowid and PX.p_accountpubid= AC.ac_pubid    

LEFT JOIN ( SELECT * FROM activity
				LEFT JOIN activitygroup
				ON pt_activitygrouprowid = ag_rowid AND pt_activitygrouppubid = ag_pubid   
		 ) AS AG
ON  PX.p_activityrowid = AG.pt_rowid AND PX.p_activitypubid = AG.pt_pubid   

LEFT JOIN(SELECT *FROM promotionmember
			LEFT JOIN promotiongroup
			ON pm_promotiongrouprowid = pg_rowid AND pm_promotiongrouppubid = pg_pubid
		  )  AS PG
ON  PX.p_rowid = PG.pm_promotionrowid AND PX.p_pubid = PG.pm_promotionpubid

LEFT JOIN (SELECT * FROM promoitemsku
				LEFT JOIN (SELECT * FROM promoitemskudetail
								LEFT JOIN gltype
								ON pisd_typerowid = gltt_rowid AND pisd_typepubid = gltt_pubid) AS PISD
				 ON pis_rowid = PISD.pisd_promoitemskurowid AND pis_pubid = PISD.pisd_promoitemskupubid
			LEFT JOIN sku  ON pis_skurowid = sku_rowid and pis_skupubid = sku_pubid
		   ) AS PIS
ON PX.p_rowid = PIS.pis_promotionrowid AND PX.p_pubid = PIS.pis_promotionpubid 



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
				  FROM producthierarchy
				  WHERE ph_level = 12  
				  ) AS T12 
				  LEFT JOIN (SELECT 
								ph_rowid AS level_11_id, 
								ph_longname AS level_11_name, 
								ph_producthierarchyrowid AS level_10
								FROM producthierarchy
							) AS T11
				  ON T12.level_11 = T11.level_11_id  

				  LEFT JOIN (SELECT 
								ph_rowid AS level_10_id, 
								ph_longname AS level_10_name, 
								ph_producthierarchyrowid AS level_9
								FROM producthierarchy
							) AS T10
				  ON T11.level_10 = T10.level_10_id

				  LEFT JOIN (SELECT * FROM sku) AS TS
				  ON T12.ph_skurowid = TS.sku_rowid
				) AS PXMap
ON PXMap.PCode = PIS.sku_stockcode


WHERE 

p_stopdate > '2021-06-30'  
AND p_startdate < '2024-06-24'
AND PX.p_deleted <> 1  AND PX.p_confirmed = 1 
AND PIS.pis_deleted <> 1
AND PIS.pisd_deleted <> 1 AND PIS.pisd_amount > 0
AND AC.ac_code not in ('BA-ALHH','BA-DMPH')  ---  'BA-MGW'
AND PIS.gltt_shortname  LIKE '%'
AND PXMap.level_11_name LIKE  '%'
AND gltt_longname = 'Case Deal Deferred'
) Z


GROUP BY

Z.BottleSize
,Z.p_startdate
,Z.p_stopdate
,Z.p_confirmed
,Z.p_closed
,CASE 
	WHEN Z.Level_11_name = 'St Hugo Premium 750ml' THEN 'JGX'
	ELSE Z.sku_profitcentre END
,Z.sku_uompersaleable
,Z.BottleSize
,Z.ac_code
,Z.Start_Wed
,DATEADD(day,Z.Duration-1, Z.Start_Wed) 
,Z.Duration
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,Z.p_startdate,Z.p_stopdate))/7,0) 
END)) 


) Y;


SELECT * FROM #PromaxTemp1

;WITH cte as
(
SELECT 
ac_code
,BQ_Code
,BottleSize
,p_startdate
,p_stopdate
,p_confirmed
,p_closed
,sku_uompersaleable
,max_psid_rate
,sum_accruedamount
,sum_paid_amount
,sum_psid_quantity
,sum_psidamount
,Start_Wed
,End_Tue
,Duration
,cte_Weeks
,original_weeks
,SKU_KEY
,KEY_WEEK_AGGREGATION

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
,cte.max_psid_rate
,cte.sum_accruedamount
,cte.sum_paid_amount
,cte.sum_psid_quantity
,cte.sum_psidamount
,cte.Start_Wed
,cte.End_Tue
,cte.Duration
,(cte.cte_weeks -1)
,cte.original_weeks
,cte.SKU_KEY
,cte.KEY_WEEK_AGGREGATION

FROM cte INNER JOIN #PromaxTemp1 as t
ON  cte.KEY_WEEK_AGGREGATION = t.KEY_WEEK_AGGREGATION
	WHERE cte.cte_weeks > 1

	)



SELECT * INTO #PromaxTemp2 FROM

(
SELECT 
ac_code
,BQ_Code
,PK.sku_longname
,BottleSize
,p_confirmed
,p_closed
,sku_uompersaleable
,max_psid_rate as Rate
,CASE 
WHEN CAST((NULLIF(sum_accruedamount,0)/original_weeks) as float) IS NULL THEN 0 
ELSE CAST((NULLIF(sum_accruedamount,0)/original_weeks) as float) END
as Accrued_Amount
,CASE 
WHEN CAST((NULLIF(sum_paid_amount,0)/original_weeks) as float) IS NULL THEN 0
ELSE CAST((NULLIF(sum_paid_amount,0)/original_weeks) as float) END
as Paid_Amount
,CAST((NULLIF(cast(sum_psid_quantity as float),0)/original_weeks) as float) as Case_Qty
,sum_psid_quantity as real_Case
,CASE WHEN CAST((NULLIF(sum_psidamount,0)/original_weeks) as float) IS NULL THEN 0
ELSE CAST((NULLIF(sum_psidamount,0)/original_weeks) as float) END
as Amount
,Start_Wed
,End_Tue
,CASE 
WHEN row_number() OVER (PARTITION BY KEY_WEEK_AGGREGATION ORDER BY KEY_WEEK_AGGREGATION) =1 
THEN Start_Wed 
ELSE DATEADD(DAY,(row_number() OVER (PARTITION BY KEY_WEEK_AGGREGATION ORDER BY KEY_WEEK_AGGREGATION)-1)*7,Start_Wed) END 
AS New_Start_Wed
,DATEADD(DAY,(row_number() OVER (PARTITION BY KEY_WEEK_AGGREGATION ORDER BY KEY_WEEK_AGGREGATION)*7)-1,Start_Wed) 
AS New_End_Tue
,row_number() OVER (PARTITION BY KEY_WEEK_AGGREGATION ORDER BY KEY_WEEK_AGGREGATION) 
AS Week_ID
,original_weeks
,SKU_KEY
,KEY_WEEK_AGGREGATION

FROM cte 

LEFT JOIN #PROMAX_SKUKEY_VIEW PK
ON cte.SKU_KEY = PK.Unique_ID  )X;


SELECT * FROM #PromaxTemp2



;WITH CTEDATES
AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY SKU_KEY asc, New_Start_Wed asc ) AS ROWNUMBER, #PromaxTemp2.* FROM #PromaxTemp2

),
 CTEDATES1
AS
(
   SELECT ROWNUMBER, CTEDATES.*, 1 as groupid FROM CTEDATES WHERE ROWNUMBER=1
   UNION ALL
   SELECT a.ROWNUMBER, a.*, case when datediff(d, b.New_Start_Wed,a.New_Start_Wed) = 7 AND a.SKU_KEY = b.SKU_KEY then b.groupid else b.groupid+1 end as gap FROM CTEDATES A INNER JOIN CTEDATES1 B ON A.ROWNUMBER-1 = B.ROWNUMBER
)


select 
ac_code
,BQ_Code
,sku_longname
,Bottlesize
,p_confirmed
,p_closed
,sku_uompersaleable
,Rate
,SUM(Accrued_Amount) as Accrued_Amount
,SUM(Paid_Amount) as Paid_Amount
,SUM(Case_Qty) as Case_Quantity
,SUM(Amount) as Amount
,SKU_KEY
,min(New_Start_Wed) as startdate
, max(New_Start_Wed) as enddate 

from CTEDATES1 

group by 
ac_code
,BQ_Code
,sku_longname
,Bottlesize
,Rate
,p_confirmed
,p_closed
,sku_uompersaleable
,SKU_KEY

option (maxrecursion 0)