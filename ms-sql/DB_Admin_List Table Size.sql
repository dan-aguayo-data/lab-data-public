if exists(select name from tempdb..sysobjects where name='##tmp')
drop table ##tmp

create table ##tmp(nam varchar(50), rows int, res varchar(15),data varchar(15),ind_sze varchar(15),unsed varchar(15))
go

declare @tblname varchar(50)
declare tblname CURSOR for SELECT TABLE_SCHEMA + '.' + TABLE_NAME FROM information_schema.tables where TABLE_TYPE = 'BASE TABLE'

open tblname
fetch next from tblname into @tblname

WHILE @@FETCH_STATUS = 0
BEGIN
  insert into ##tmp
  exec sp_spaceused @tblname
  FETCH NEXT FROM tblname INTO @tblname
END
CLOSE tblname

deallocate tblname
go

select nam Table_Name,rows Total_Rows, cast(replace(data, ' KB', '') as INT) Data_size_KB from ##tmp
order by Data_size_KB DESC
