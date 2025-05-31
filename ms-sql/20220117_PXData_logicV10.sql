SELECT * INTO #RevUp2 FROM
(
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
,ac_code
,gltt_shortname
,sku_stockcode
,sku_longname
,sku_profitcentre
,level_11_name
,pis_shelfpriceplan
,pis_featurepriceplan
,pisd_quantityuom
,pisd_quantity
,pisd_rate
,pisd_amount
,pisd_accruedamount
,pisd_paidamount
,p_confirmed
,p_closed
,sku_casenetweight
,sku_uompersaleable
,Start_Wed
,DATEADD(day,Duration-1, Start_Wed) AS End_Tue
,Duration
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7,0) 
END)) AS Weeks											--> Calculated column, promotion duration in weeks					--IMPROVEMENT ADDED 12/12/2021
,CONVERT(int,(CASE 
WHEN CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7 <1 THEN 1
ELSE ROUND(CONVERT(decimal,DATEDIFF(day,p_startdate,p_stopdate))/7,0) 
END)) as Weeks_Original	
,Rate_per_unit
,Qty_units
,Qty_lts
,PromoDealDepthCode
,CASE
	WHEN CASE 
			WHEN gltt_shortname = 'Coop' THEN 'DIS'
			WHEN p_closed = 1 THEN 'SOA'
			WHEN (pisd_accruedamount/NULLIF(pisd_paidamount,0)) > .95001 THEN 'SOA' 
			ELSE  'SOE' 
		 END = 'DIS' THEN CASE	
										WHEN PromoCostType = 'SOA' THEN pisd_paidamount
										WHEN PromoCostType = 'SOE' THEN pisd_amount
										WHEN PromoCostType = 'DIS' THEN  (CASE WHEN p_closed = 1 THEN pisd_paidamount ELSE pisd_amount END)
									END
	ELSE Rate_per_unit/sku_casenetweight
END AS PromoUnitCost
,PromoCostType
,Percentage_Paid
,CASE 
	WHEN PromoCostType = 'SOA' THEN pisd_paidamount
	WHEN PromoCostType = 'SOE' THEN pisd_amount
	WHEN PromoCostType = 'DIS' THEN  (CASE WHEN p_closed = 1 THEN pisd_paidamount ELSE pisd_amount END)
END AS Amount_RevUp
,ISNULL(
CASE
	WHEN PromoCostType = 'DIS' THEN 1
	WHEN PromoCostType = 'SOE' THEN 0
	WHEN PromoCostType = 'SOA' THEN  (CASE 
										WHEN PromoCostType = 'SOA' THEN pisd_paidamount
										WHEN PromoCostType = 'SOE' THEN pisd_amount
										WHEN PromoCostType = 'DIS' THEN  (CASE WHEN p_closed = 1 THEN pisd_paidamount ELSE pisd_amount END)
									  END)/
											(CASE
												WHEN CASE 
														WHEN gltt_shortname = 'Coop' THEN 'DIS'
														WHEN p_closed = 1 THEN 'SOA'
														WHEN (pisd_accruedamount/NULLIF(pisd_paidamount,0)) > .95001 THEN 'SOA' 
														ELSE  'SOE' 
													 END = 'DIS' THEN CASE	
																					WHEN PromoCostType = 'SOA' THEN pisd_paidamount
																					WHEN PromoCostType = 'SOE' THEN pisd_amount
																					WHEN PromoCostType = 'DIS' THEN  (CASE WHEN p_closed = 1 THEN pisd_paidamount ELSE pisd_amount END)
																				END
												ELSE Rate_per_unit/sku_casenetweight
											END)
END,0) AS Lts_RevUp
,ShopperID
,MIN(ShopperID) OVER (PARTITION BY CONCAT(ac_code,sku_stockcode,CONVERT(varchar (10),Start_Wed,12),CONVERT(varchar (10),DATEADD(day,Duration-1, Start_Wed),12))) AS SingleShopperID   --- Matches original when Duration = CEILING(convert(decimal,(DATEDIFF(day,p_startdate,p_stopdate))/convert(decimal, 7)))*(7)-1
,CONCAT(pg_rowid,'-',sku_stockcode,'-',ac_code,'-',(pisd_quantity*sku_uompersaleable)*sku_casenetweight,'-',pisd_accruedamount,'-',CONVERT(varchar (10),Start_Wed,12)) as UNIQUE_ID


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
,AC.ac_code
,PIS.gltt_shortname
,PIS.sku_stockcode
,PIS.sku_longname
,PIS.sku_profitcentre
,PXMap.level_11_name
,PIS.pis_shelfpriceplan
,PIS.pis_featurepriceplan
,PIS.pisd_quantityuom
,PIS.pisd_quantity
,PIS.pisd_rate
,PIS.pisd_amount
,PIS.pisd_accruedamount	
,PIS.pisd_paidamount  	
,PX.p_confirmed				
,PX.p_closed
,PIS.sku_casenetweight
,PIS.sku_uompersaleable 
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

CASE 
WHEN DATEPART(WEEKDAY,p_startdate) = 4 THEN p_startdate
ELSE DATEADD(day,4-DATEPART(WEEKDAY,p_startdate),p_startdate) 
END >= '2021-07-01'  
AND PX.p_deleted <> 1  AND PX.p_confirmed = 1 
AND PIS.pis_deleted <> 1
AND PIS.pisd_deleted <> 1 AND PIS.pisd_amount <> 0
AND AC.ac_code not in ('BA-ALHH','BA-MGW')  
AND PIS.gltt_shortname  LIKE '%'
AND PXMap.level_11_name LIKE  '%'
) Z


	) X


;WITH cte AS 
(
SELECT * FROM #RevUp2

	UNION ALL

	SELECT

	 cte.pg_rowid
	,cte.p_rowid
	,cte.pg_longname
	,cte.ag_shortname
	,cte.pt_longname
	,cte.p_promonumber
	,cte.p_note
	,cte.p_fieldnote
	,cte.p_startdate	
	,cte.p_stopdate
	,cte.ac_code
	,cte.gltt_shortname
	,cte.sku_stockcode
	,cte.sku_longname
	,cte.sku_profitcentre
	,cte.level_11_name
	,cte.pis_shelfpriceplan
	,cte.pis_featurepriceplan
	,cte.pisd_quantityuom
	,cte.pisd_quantity
	,cte.pisd_rate
	,cte.pisd_amount
	,cte.pisd_accruedamount
	,cte.pisd_paidamount
	,cte.p_confirmed
	,cte.p_closed
	,cte.sku_casenetweight
	,cte.sku_uompersaleable
	,cte.Start_Wed
	,cte.End_Tue
	,cte.Duration
	,(cte.[Weeks]-1)
	,cte.Weeks_Original
	,cte.Rate_per_unit
	,cte.Qty_units
	,cte.Qty_lts
	,cte.PromoDealDepthCode
	,cte.PromoUnitCost
	,cte.PromoCostType
	,cte.Percentage_Paid
	,cte.Amount_RevUp
	,cte.Lts_RevUp
	,cte.ShopperID
	,cte.SingleShopperID
	,cte.UNIQUE_ID
	FROM cte INNER JOIN #RevUp2 as t
	ON  cte.UNIQUE_ID = T.UNIQUE_ID
	WHERE cte.[Weeks] > 1
	)

SELECT * INTO #RevUp3 FROM

(
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
,ac_code
,gltt_shortname
,sku_stockcode
,sku_longname
,sku_profitcentre
,level_11_name
,pis_shelfpriceplan
,pis_featurepriceplan
,pisd_quantityuom
,pisd_quantity
,pisd_rate
,pisd_amount
,pisd_accruedamount
,pisd_paidamount
,p_confirmed
,p_closed
,sku_casenetweight
,sku_uompersaleable
,Start_Wed
,End_Tue
,Duration
,1 [Weeks]
,Weeks_Original
,Rate_per_unit
,Qty_units
,Qty_lts
,PromoDealDepthCode
,PromoUnitCost
,PromoCostType
,Percentage_Paid
,Amount_RevUp
,Lts_RevUp
,ShopperID
,UNIQUE_ID
,SingleShopperID
,row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID) AS Week_ID
, CASE 
WHEN row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID) =1 
THEN Start_Wed 
ELSE
DATEADD(DAY,(row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID)-1)*7,Start_Wed) END AS New_Start_Wed
,DATEADD(DAY,(row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID)*7)-1,Start_Wed) AS New_End_Tue
,CASE
	WHEN gltt_shortname = 'Coop'
		THEN PromoUnitCost/Weeks_Original
	ELSE PromoUnitCost
END AS New_UnitCost,

CASE 
	WHEN ShopperID IS NULL 
		THEN NULL 
	ELSE CONCAT(ac_code,'-',sku_stockcode,'-',CONVERT(VARCHAR(10),
				CASE 
					WHEN row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID) =1 
						THEN Start_Wed 
							ELSE
								DATEADD(DAY,(row_number() OVER (PARTITION BY UNIQUE_ID ORDER BY UNIQUE_ID)-1)*7,Start_Wed) END
								,12)
				)

END AS ShopperCode

FROM cte 

) Y


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
,ac_code
,gltt_shortname
,sku_stockcode
,sku_longname
,sku_profitcentre
,level_11_name
,pis_shelfpriceplan
,pis_featurepriceplan
,pisd_quantityuom
,pisd_quantity
,pisd_rate
,pisd_amount
,pisd_accruedamount
,pisd_paidamount
,p_confirmed
,p_closed
,sku_casenetweight
,sku_uompersaleable
,Start_Wed
,End_Tue
,Duration
,Weeks
,Weeks_Original
,Rate_per_unit
,Qty_units
,Qty_lts
,PromoDealDepthCode
,PromoUnitCost
,PromoCostType
,Percentage_Paid
,Amount_RevUp
,Lts_RevUp
,ShopperID
,SingleShopperID
,Week_ID
,New_Start_Wed 
,New_End_Tue
,New_UnitCost
,ShopperCode
,UNIQUE_ID
,CASE 
		WHEN p_closed =1  AND pisd_paidamount = 0 THEN ''

		ELSE CONCAT(pg_rowid,'-',sku_stockcode,'-',CONVERT(varchar (10),New_Start_Wed, 12)) 
END AS New_ShopperID  --Promax Unique ID in promax					-- IMPROVEMENT ADDED 12/12/2021 (ALAN DONG)

,MIN(
CASE 
		WHEN p_closed =1  AND pisd_paidamount = 0 THEN ''

		ELSE(CASE 
		WHEN p_closed =1  AND pisd_paidamount = 0 THEN ''

		ELSE CONCAT(pg_rowid,'-',sku_stockcode,'-',CONVERT(varchar (10),New_Start_Wed, 12)) 
END) 
END)	
		over (PARTITION BY CONCAT(ac_code,sku_stockcode,CONVERT(varchar (10),New_Start_Wed, 12),CONVERT(varchar (10),New_End_Tue, 12))) AS New_SingleShopperID--When (Banner - Sku - Date Range) is the same in two or more rows select the first ShopperID and assign to all					-- IMPROVEMENT ADDED 07/12/2021 (ALAN DONG)
FROM #RevUp3


ORDER BY MIN(
CASE 
		WHEN p_closed =1  AND pisd_paidamount = 0 THEN ''

		ELSE(CASE 
		WHEN p_closed =1  AND pisd_paidamount = 0 THEN ''

		ELSE CONCAT(pg_rowid,'-',sku_stockcode,'-',CONVERT(varchar (10),New_Start_Wed, 12)) 
END) 
END)	
		over (PARTITION BY CONCAT(ac_code,sku_stockcode,CONVERT(varchar (10),New_Start_Wed, 12),CONVERT(varchar (10),New_End_Tue, 12))) desc



--------------------------------------------------------------------------------
DROP TABLE #RevUp2
DROP TABLE #RevUp3