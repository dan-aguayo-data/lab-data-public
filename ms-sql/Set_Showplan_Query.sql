/****** Script for SelectTopNRows command from SSMS  ******/

  
  SET SHOWPLAN_ALL ON;  
GO  
-- First query.  
SELECT TOP (1000) 
*
  FROM [DWH_FINANCE].[V_FT_AccountBalanceActual_FRQ_NBL]


GO  
SET SHOWPLAN_ALL OFF;  
GO  