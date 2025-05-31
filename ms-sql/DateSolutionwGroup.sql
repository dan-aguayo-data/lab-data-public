
IF OBJECT_ID('tempdb..#InfoDateTest') IS NOT NULL DROP TABLE #InfoDateTest;

CREATE TABLE #InfoDateTest(InfoDate date, GroupKey varchar(max))

INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-07-28','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-04','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-11','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-10-27','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-03','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-10','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-12-22','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-25','A1')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-07-28','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-04','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-11','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-10-27','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-03','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-10','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-12-22','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-25','A2')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-07-28','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-04','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-11','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-10-27','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-03','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-11-10','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-12-22','A3')
INSERT INTO #InfoDateTest (InfoDate,GroupKey) VALUES('2021-08-25','A3')


SELECT * FROM #InfoDateTest


;WITH CTEDATES
AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY GroupKey asc, Infodate asc ) AS ROWNUMBER,infodate,GroupKey FROM #InfoDateTest  

),
 CTEDATES1
AS
(
   SELECT ROWNUMBER, infodate, GroupKey, 1 as groupid FROM CTEDATES WHERE ROWNUMBER=1
   UNION ALL
   SELECT a.ROWNUMBER, a.infodate, a.Groupkey, case when datediff(d, b.infodate,a.infodate) = 7 AND a.GroupKey = b.GroupKey then b.groupid else b.groupid+1 end as gap FROM CTEDATES A INNER JOIN CTEDATES1 B ON A.ROWNUMBER-1 = B.ROWNUMBER
)


select GroupKey, min(infodate) as startdate, max(infodate) as enddate from CTEDATES1 group by groupkey, groupid


