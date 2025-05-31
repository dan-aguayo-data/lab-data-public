--############ DATA WAREHOUSE MART ############--

SELECT
--Dimension Key
  {{ dbt_utils.generate_surrogate_key(['PROGRAM.PROGRAM_CODE']) }} AS DIM_PROGRAM_KEY

--Natural Key
, PROGRAM.PROGRAM_CODE AS PROGRAM_CODE

--Attributes
, PROGRAM.TITLE AS PROGRAM
, DECODE(PROGRAM_CODE, CUSTOMER) AS DISPLAY_NAME
, PROGRAM.REGION AS REGION

--Metadata Attributes
, SYSDATE() AS LOAD_UTC_TIMESTAMP

FROM
{{ source('DATA_HUB', 'PROGRAM') }} AS PROGRAM

UNION ALL

SELECT
--Dimension Key
  {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} AS DIM_PROGRAM_KEY

--Natural Key
, -1 AS PROGRAM_CODE

--Attributes
, 'HUB' AS PROGRAM
, 'HUB - SHARED' AS DISPLAY_NAME
, 'Australia/Melbourne' AS REGION

--Metadata Attributes
, SYSDATE() AS LOAD_UTC_TIMESTAMP

UNION ALL

SELECT
--Dimension Key
  {{ dbt_utils.generate_surrogate_key(["'0'"]) }} AS DIM_PROGRAM_KEY

--Natural Key
, 0 AS PROGRAM_CODE

--Attributes
, 'HUB' AS PROGRAM
, 'HUB - INTERNAL' AS DISPLAY_NAME
, 'Australia/Melbourne' AS REGION

--Metadata Attributes
, SYSDATE() AS LOAD_UTC_TIMESTAMP