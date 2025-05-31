create table #temp (
    Id int identity(1,1) primary key clustered, 
    SKU varchar(10),
    QtyRec int,
    Expiry date,
    Rec date
);



insert into #temp(SKU, QtyRec, Expiry, Rec)
    select SKU, QtyRec, Expiry, Rec
    from @Data
    order by id;

