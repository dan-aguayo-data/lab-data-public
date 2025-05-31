
IF OBJECT_ID('tempdb..#InfoDateTest') IS NOT NULL DROP TABLE #InfoDateTest;

CREATE TABLE #InfoDateTest(InfoDate date)

INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-07-28')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-04')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-11')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-10-27')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-11-03')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-11-10')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-12-22')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-25')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-07-28')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-04')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-11')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-10-27')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-11-03')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-11-10')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-12-22')
INSERT INTO #InfoDateTest (InfoDate) VALUES('2021-08-25')


SELECT * FROM #InfoDateTest


;WITH CTEDATES
AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY Infodate asc ) AS ROWNUMBER,infodate FROM #InfoDateTest  

),
 CTEDATES1
AS
(
   SELECT ROWNUMBER, infodate, 1 as groupid FROM CTEDATES WHERE ROWNUMBER=1
   UNION ALL
   SELECT a.ROWNUMBER, a.infodate,case datediff(d, b.infodate,a.infodate) when 7 then b.groupid else b.groupid+1 end as gap FROM CTEDATES A INNER JOIN CTEDATES1 B ON A.ROWNUMBER-1 = B.ROWNUMBER
)

select min(infodate) as startdate, max(infodate) as enddate from CTEDATES1 group by groupid


