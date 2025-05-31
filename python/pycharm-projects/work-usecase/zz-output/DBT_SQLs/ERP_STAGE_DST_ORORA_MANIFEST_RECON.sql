{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}




WITH 
SAWITH0 AS 
(select T289163.C527751494 as c2,
     T289163.C369898840 as c3,
     T289163.C231832793 as c5,
     T289163.C498687441 as c6,
     T289163.C342081810 as c7,
     T289163.C349622373 as c8,
     T289163.C214686596 as c10,
     T289163.C364048903 as c11,
     T289163.C51067786 as c12,
     T289163.C287914453 as c13,
     T289163.C379200345 as c14,
     T289163.C356728513 as c16,
     T289163.C480708916 as c19,
     T289163.C39336874 as c20,
     T289163.C121244272 as c21,
     T289163.C23351190 as c22,
     T289163.C60429349 as c23,
     T289163.C183425870 as c24
from 
     (
     SELECT 
     V405133203.PARTY_NAME AS C527751494,
     V405133203.PARTY_NUMBER AS C369898840,         
     V202750511.TRX_DATE AS C231832793,         
     V202750511.TRX_NUMBER AS C498687441,         
     V62756817.NAME AS C342081810,         
     V202750511.DESCRIPTION181 AS C349622373,         
     V102674681.PARTY_SITE_NAME AS C214686596,         
     V102674681.PARTY_SITE_NUMBER AS C364048903,         
     V220364535.NAME AS C51067786,         
     V202750511.TRX_CLASS AS C287914453,         
     V220364535.ORGANIZATION_ID1 AS C379200345,         
     V62756817.CUST_TRX_TYPE_SEQ_ID AS C356728513,         
     V202750511.LINE_TYPE223 AS C480708916,         
     V202750511.EXTENDED_AMOUNT1 AS C39336874,         
     V202750511.LINE_NUMBER AS C121244272,         
     V202750511.LINE_NUMBER3 AS C23351190,         
     V202750511.LINK_TO_CUST_TRX_LINE_ID AS C60429349,         
     V405133203.PARTY_ID AS C183425870,         
     V62756817.TYPE AS C41833683,         
     V202750511.CUSTOMER_TRX_LINE_ID AS PKA_CustomerTrxLineId0,        
     V202750511.CUSTOMER_TRX_ID1 AS PKA_TransactionHeaderCustomer0,         
     V202750511.CUSTOMER_TRX_LINE_ID4 AS PKA_TransactionLineLinkCustom0,               
     V220364535.EFFECTIVE_START_DATE651 AS PKA_BusinessUnitDateFrom0,         
     V220364535.EFFECTIVE_END_DATE8 AS PKA_BusinessUnitDateTo0,         
     V220364535.ORGANIZATION_ID AS PKA_OrganizationUnitTranslati0,         
     V220364535.EFFECTIVE_START_DATE AS PKA_OrganizationUnitTranslati1,         
     V220364535.EFFECTIVE_END_DATE AS PKA_OrganizationUnitTranslati2,         
     V220364535.LANGUAGE AS PKA_OrganizationUnitTranslati3 
     FROM (
          SELECT 
          TransactionLine.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID AS CUSTOMER_TRX_LINE_ID,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_DESCRIPTION AS DESCRIPTION181,         
          (NVL(TransactionLine.RA_CUSTOMER_TRX_LINE_GROSS_EXTENDED_AMOUNT,TransactionLine.RA_CUSTOMER_TRX_LINE_EXTENDED_AMOUNT)) AS EXTENDED_AMOUNT1,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_EXTENDED_AMOUNT AS EXTENDED_AMOUNT,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_GROSS_EXTENDED_AMOUNT AS GROSS_EXTENDED_AMOUNT,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_LINE_NUMBER AS LINE_NUMBER,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_LINE_TYPE AS LINE_TYPE223,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_LINK_TO_CUST_TRX_LINE_ID AS LINK_TO_CUST_TRX_LINE_ID,         
          TransactionLine.RA_CUSTOMER_TRX_LINE_ORG_ID AS ORG_ID,         
          TransactionHeader.RA_CUSTOMER_TRX_BILL_TO_SITE_USE_ID as BILL_TO_SITE_USE_ID,         
          TransactionHeader.RA_CUSTOMER_TRX_CUST_TRX_TYPE_SEQ_ID as CUST_TRX_TYPE_SEQ_ID,         
          TransactionHeader.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID AS CUSTOMER_TRX_ID1,         
          TransactionHeader.RA_CUSTOMER_TRX_TRX_CLASS as TRX_CLASS,         
          TransactionHeader.RA_CUSTOMER_TRX_TRX_DATE as TRX_DATE,         
          TransactionHeader.RA_CUSTOMER_TRX_TRX_NUMBER as TRX_NUMBER,         
          TransactionLineLink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID AS CUSTOMER_TRX_LINE_ID4,         
          TransactionLineLink.RA_CUSTOMER_TRX_LINE_LINE_NUMBER AS LINE_NUMBER3,         
          CustAcct.PARTY_ID AS PARTY_ID_031 
          FROM 
          CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_LINE_EXTRACT_PVO TransactionLine, --RA_CUSTOMER_TRX_LINES_ALL
          CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_HEADER_EXTRACT_PVO TransactionHeader, --RA_CUSTOMER_TRX_ALL
          CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_LINE_EXTRACT_PVO TransactionLineLink, --RA_CUSTOMER_TRX_LINES_ALL
          CES_ERP.FSCM_PROD_PARTIESANALYTICS.CUSTOMER_ACCOUNT CustAcct, --HZ_CUST_ACCOUNTS
          CES_ERP.FSCM_PROD_FINEXTRACT_FUNBICCEXTRACT.BUSINESS_UNIT_EXTRACT_PVO  BusinessUnit   --FUN_BU_PERF_V 
          WHERE 
          (TransactionLine.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_ID = TransactionHeader.RA_CUSTOMER_TRX_CUSTOMER_TRX_ID 
          AND TransactionLine.RA_CUSTOMER_TRX_LINE_LINK_TO_CUST_TRX_LINE_ID = TransactionLineLink.RA_CUSTOMER_TRX_LINE_CUSTOMER_TRX_LINE_ID(+) 
          AND TransactionHeader.RA_CUSTOMER_TRX_BILL_TO_CUSTOMER_ID = CustAcct.CUST_ACCOUNT_ID(+) 
          AND TransactionLine.RA_CUSTOMER_TRX_LINE_ORG_ID = BusinessUnit.FUN_BU_PERF_PEOBUSINESS_UNIT_ID
          )  
) V202750511, 
(
SELECT 
CustomerAccountSiteUsePEO.SITE_USE_ID as SITE_USE_ID ,                
CustomerAccountSiteUsePEO.PARTY_SITE_NAME as PARTY_SITE_NAME,
CustomerAccountSiteUsePEO.PARTY_SITE_NUMBER as PARTY_SITE_NUMBER,         
FROM 
CES_ERP.FSCM_PROD_PARTIESANALYTICS.CUST_ACCOUNT_SITE_USE CustomerAccountSiteUsePEO
) V102674681, 

(SELECT 
OrganizationUnit.ORGANIZATION_UNIT_PEOORGANIZATION_ID AS ORGANIZATION_ID1,         
OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE AS EFFECTIVE_START_DATE651,         
OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE AS EFFECTIVE_END_DATE8,         
OrganizationInformation.ORGANIZATION_INFORMATION_ORG_INFORMATION_ID AS ORG_INFORMATION_ID,

OrganizationUnitTranslation.ORGANIZATION_UNIT_TRANSLATION_PEONAME AS NAME,         
OrganizationUnitTranslation.ORGANIZATION_ID AS ORGANIZATION_ID,         
OrganizationUnitTranslation.EFFECTIVE_START_DATE AS EFFECTIVE_START_DATE,         
OrganizationUnitTranslation.EFFECTIVE_END_DATE AS EFFECTIVE_END_DATE,         
OrganizationUnitTranslation.LANGUAGE  AS LANGUAGE
FROM 
CES_ERP.FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_TRANSLATION_PVO OrganizationUnit, -- HR_ALL_ORGANIZATION_UNITS_F
 CES_ERP.FSCM_PROD_FINFUNBUSINESSUNITS.BUSINESS_UNIT_PVO OrganizationInformation, -- HR_ORGANIZATION_INFORMATION_F  
CES_ERP.FSCM_PROD_ORGANIZATION.ORGANIZATION_UNIT_TRANSLATION_PVO OrganizationUnitTranslation -- HR_ORGANIZATION_UNITS_F_TL
WHERE 
OrganizationUnit.ORGANIZATION_UNIT_PEOORGANIZATION_ID = OrganizationInformation.ORGANIZATION_INFORMATION_ORGANIZATION_ID
AND ('FUN_BUSINESS_UNIT') = OrganizationInformation.ORG_UNIT_CLASSIFICATION_CLASSIFICATION_CODE
AND OrganizationUnit.ORGANIZATION_UNIT_PEOORGANIZATION_ID = OrganizationUnitTranslation.ORGANIZATION_ID 
AND OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE = OrganizationUnitTranslation.EFFECTIVE_START_DATE 
AND OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE = OrganizationUnitTranslation.EFFECTIVE_END_DATE 
--AND (USERENV('LANG')) = OrganizationUnitTranslation.LANGUAGE 
AND ( SYSDATE() BETWEEN OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_START_DATE AND OrganizationUnit.ORGANIZATION_UNIT_PEOEFFECTIVE_END_DATE) 
AND ( SYSDATE() BETWEEN OrganizationInformation.ORGANIZATION_INFORMATION_EFFECTIVE_START_DATE AND OrganizationInformation.ORGANIZATION_INFORMATION_EFFECTIVE_END_DATE) 
AND ( SYSDATE() BETWEEN OrganizationUnitTranslation.EFFECTIVE_START_DATE AND OrganizationUnitTranslation.EFFECTIVE_END_DATE)
) V220364535, 
(SELECT 
CustomerPartyPEO.PARTY_PARTY_ID AS PARTY_ID,        --ParentPartyId 
CustomerPartyPEO.PARTY_PARTY_NAME AS PARTY_NAME,         
CustomerPartyPEO.PARTY_PARTY_NUMBER AS PARTY_NUMBER 
FROM 
CES_ERP.FSCM_PROD_PRCPOZPUBLICVIEW.SUPPLIER_PVO CustomerPartyPEO  -- HZ_PARTIES
) V405133203, 
(SELECT 
TransactionType.RA_CUST_TRX_TYPE_CUST_TRX_TYPE_SEQ_ID AS CUST_TRX_TYPE_SEQ_ID,         
TransactionType.RA_CUST_TRX_TYPE_NAME AS NAME,         
TransactionType.RA_CUST_TRX_TYPE_TYPE AS TYPE 
FROM 
CES_ERP.FSCM_PROD_FINEXTRACT_ARBICCEXTRACT.TRANSACTION_TYPE_EXTRACT_PVO TransactionType  --RA_CUST_TRX_TYPES_ALL      -- FscmTopModelAM.FinExtractAM.ArBiccExtractAM.TransactionHeaderExtractPVO
) V62756817 
WHERE 
V202750511.BILL_TO_SITE_USE_ID = V102674681.SITE_USE_ID(+) 
AND V202750511.ORG_ID = V220364535.ORGANIZATION_ID1 
AND V202750511.PARTY_ID_031 = V405133203.PARTY_ID (+) 
AND V202750511.CUST_TRX_TYPE_SEQ_ID = V62756817.CUST_TRX_TYPE_SEQ_ID 
AND ( ( (V405133203.PARTY_NAME = 'ORORA PACKAGING AUSTRALIA PTY LTD' ) )  
AND ( NOT ( (V62756817.TYPE = 'DEP' ) ) )  
AND ( NOT ( (V62756817.TYPE = 'GUAR' ) ) )  
AND ( NOT ( (V62756817.TYPE = 'PMT' ) ) )  
AND ( ( (V220364535.ORGANIZATION_ID1 = '300000026644042' ) )  
OR ( (V220364535.ORGANIZATION_ID1 = '300000033327288' ) )  
OR ( (V220364535.ORGANIZATION_ID1 = '300000033327301' ) ) )  
AND ( ( (V202750511.TRX_CLASS = 'INV' ) )  
OR ( (V202750511.TRX_CLASS = 'PMT' ) ) )  
AND ( ( (V62756817.NAME = 'Auction Neg MRF Cr' ) )  
OR ( (V62756817.NAME = 'Auction Neg MRF Inv' ) )  
OR ( (V62756817.NAME = 'Auction Processor Cr' ) )  
OR ( (V62756817.NAME = 'AuctionMRFRecyclr Cr' ) )  
OR ( (V62756817.NAME = 'AuctionMRFRecyclrInv' ) )  
OR ( (V62756817.NAME = 'AuctionPlatfomMRF Cr' ) )  
OR ( (V62756817.NAME = 'AuctionPlatfomMRFInv' ) )  
OR ( (V62756817.NAME = 'AuctionPrcRecyclr Cr' ) )  
OR ( (V62756817.NAME = 'AuctionPrcRecyclrInv' ) )  
OR ( (V62756817.NAME = 'AuctionProcessor Inv' ) ) ) )) T289163

where  ( substr(T289163.C349622373 , -7 , 7) like 'LM%' )          
),
SAWITH1 AS (select sum(case  when D1.c19 = 'LINE' then D1.c20 else 0 end ) as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D1.c5 as c5,
     D1.c6 as c6,
     D1.c7 as c7,
     D1.c8 as c8,
     case  when D1.c19 = 'LINE' then D1.c21 when D1.c19 = 'TAX' then D1.c22 when D1.c19 = 'FREIGHT' then case  when D1.c23 is null then D1.c21 else D1.c22 end  else NULL end  as c9,
     D1.c10 as c10,
     D1.c11 as c11,
     D1.c12 as c12,
     D1.c13 as c13,
     D1.c14 as c14,
     substr(D1.c8 , -7 , 7) as c15,
     D1.c16 as c16,
     cast(D1.c24 as  VARCHAR ( 20 ) ) as c17
from 
SAWITH0 D1
group by D1.c2, D1.c3, D1.c5, D1.c6, D1.c7, D1.c8, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c16, case  when D1.c19 = 'LINE' then D1.c21 when D1.c19 = 'TAX' then D1.c22 when D1.c19 = 'FREIGHT' then case  when D1.c23 is null then D1.c21 else D1.c22 end  else NULL end , cast(D1.c24 as  VARCHAR ( 20 ) )
),
SAWITH2 AS (
select T289164.C389230568 as c1,
T289164.C164164071 as c2
from 
(
SELECT 
V72673585.MEANING AS C389230568,         
V72673585.LOOKUP_CODE AS C164164071,         
V72673585.LANGUAGE AS C343259318,         
V72673585.SET_ID AS C497682495,         
V72673585.LOOKUP_TYPE AS C417433804,         
V72673585.VIEW_APPLICATION_ID AS C456636657 
FROM CES_ERP.FSCM_PROD_ANALYTICSSERVICE.LOOKUP_VALUES_TLPVO V72673585 --FND_LOOKUP_VALUES_TL
WHERE ( ( (V72673585.LOOKUP_TYPE = 'AR_ALL_DOC_CLASSES' ) )  
AND ( (V72673585.VIEW_APPLICATION_ID = 222 ) )  
AND ( (V72673585.SET_ID = 0 ) )  
AND ( (V72673585.LANGUAGE = 'US' ) ) )) T289164
),
SAWITH3 AS (select D1.c1 as c1,
     D1.c2 as c2,
     D1.c3 as c3,
     D2.c1 as c4,
     D1.c13 as c5,
     D1.c5 as c6,
     D1.c6 as c7,
     D1.c7 as c8,
     D1.c8 as c9,
     D1.c9 as c10,
     D1.c10 as c11,
     D1.c11 as c12,
     D1.c12 as c13,
     D1.c14 as c14,
     D1.c15 as c15,
     D1.c16 as c16,
     D1.c17 as c17
from 
SAWITH1 D1 
left outer join SAWITH2 D2 On D1.c13 = D2.c2
),
SAWITH4 AS (select D901.c1 as c1,
     D901.c2 as c2,
     D901.c3 as c3,
     nvl(D901.c4 , D901.c5) as c4,
     D901.c6 as c5,
     D901.c7 as c6,
     D901.c8 as c7,
     D901.c9 as c8,
     D901.c10 as c9,
     D901.c11 as c10,
     D901.c12 as c11,
     D901.c13 as c12,
     D901.c5 as c13,
     D901.c14 as c14,
     D901.c15 as c15,
     D901.c16 as c16,
     D901.c17 as c17
from 
     SAWITH3 D901
),
SAWITH5 AS (select D1.c1 as c1,
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
     D1.c18 as c18,
     D1.c19 as c19
from 
     (select 0 as c1,
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
               D1.c1 as c16,
               D1.c16 as c18,
               D1.c17 as c19,
               ROW_NUMBER() OVER (PARTITION BY D1.c2, D1.c3, D1.c5, D1.c6, D1.c7, D1.c8, D1.c9, D1.c10, D1.c11, D1.c12, D1.c13, D1.c14, D1.c16, D1.c17 ORDER BY D1.c2 ASC, D1.c3 ASC, D1.c5 ASC, D1.c6 ASC, D1.c7 ASC, D1.c8 ASC, D1.c9 ASC, D1.c10 ASC, D1.c11 ASC, D1.c12 ASC, D1.c13 ASC, D1.c14 ASC, D1.c16 ASC, D1.c17 ASC) as c20
          from 
               SAWITH4 D1
     ) D1
where  ( D1.c20 = 1 ) 
),
SAWITH6 AS (select D1.c1 as c1,
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
     D1.c18 as c18,
     D1.c19 as c19
from 
SAWITH5 D1
),
SAWITH7 AS (select D1.c1 as c1,
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
     D1.c18 as c20,
     D1.c19 as c21,
     ROW_NUMBER() OVER (PARTITION BY D1.c11, D1.c10, D1.c7, D1.c8, D1.c5, D1.c13, D1.c6, D1.c2, D1.c14, D1.c3, D1.c15, D1.c18, D1.c9, D1.c12, D1.c19, D1.c4 ORDER BY D1.c11 DESC, D1.c10 DESC, D1.c7 DESC, D1.c8 DESC, D1.c5 DESC, D1.c13 DESC, D1.c6 DESC, D1.c2 DESC, D1.c14 DESC, D1.c3 DESC, D1.c15 DESC, D1.c18 DESC, D1.c9 DESC, D1.c12 DESC, D1.c19 DESC, D1.c4 DESC) as c22
from 
     SAWITH6 D1)
select 
D1.c2 as BILL_TO_CUSTOMER_NAME, 
D1.c3 as BILL_TO_CUSTOMER_NUMBER, 
D1.c4 as TRANSACTION_CLASS, 
D1.c5 as INVOICE_DATE, 
D1.c6 as INVOICE_NUMBER, 
D1.c7 as TRANSACTION_TYPE_NAME, 
D1.c8 as TRANSACTION_LINE_DESCRIPTION, 
D1.c9 as TRANSACTION_LINE_NUMBER, 
D1.c10 as BILL_TO_SITE_NAME, 
D1.c11 as BILL_TO_SITE_NUMBER, 
D1.c12 as BUSINESS_UNIT_NAME, 
D1.c13 as TRANSACTION_CLASS_CODE, 
D1.c14 as BUSINESS_UNIT_NAME_ID, 
D1.c15 as MANIFEST_ID, 
D1.c16 as LINE_AMOUNT, 
D1.c17 as LINE_AMOUNT_NEW 
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
     sum(case D1.c22 when 1 then D1.c16 else NULL end ) over (partition by D1.c11, D1.c10, D1.c7, D1.c8, D1.c5, D1.c13, D1.c6, D1.c2, D1.c14, D1.c3, D1.c15)  as c17
from 
     SAWITH7 D1
) D1