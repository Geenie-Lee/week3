select * from (
select a.stock_code
     , a.stock_name
     , a.market_type
     , a.ma1
     , b.ma5
     , c.ma20
     , d.ma60
     , c.min20
     , c.max20
     , a.ma1-c.ma20 as gap
     , (c.min20+c.max20)/2 as avg_mm
     , case when b.ma5 > c.ma20 and c.ma20 > d.ma60 then 'Y' else 'N' end as chk1
     , case when b.ma5 < c.ma20 and c.ma20 > d.ma60 then 'Y' else 'N' end as chk2
  from 
	 (
	   select stock_code
	        , stock_name
		    , close_price as ma1
		    , market_type
		 from stock_day_info
		where day_num = 1
	 ) a
	 ,
	 (
	   select stock_code
		    , sum(close_price)/count(*) as ma5
		    , min(close_price) as min5
		    , max(close_price) as max5
		 from stock_day_info
		where day_num between 1 and 5
		group by stock_code
	 ) b
	 ,
	 (
	   select stock_code
		    , sum(close_price)/count(*) as ma20
		    , min(close_price) as min20
		    , max(close_price) as max20
		 from stock_day_info
		where day_num between 1 and 20
		group by stock_code
	 ) c
	 ,
	 (
	   select stock_code
		    , sum(close_price)/count(*) as ma60
		    , min(close_price) as min60
		    , max(close_price) as max60
		 from stock_day_info
		where day_num between 1 and 60
		group by stock_code
	 ) d
 where a.stock_code = b.stock_code
   and b.stock_code = c.stock_code
   and c.stock_code = d.stock_code
   and a.stock_name is not null
   and (a.ma1-c.ma20) between 5000 and 10000
) x
 where x.chk1 = 'Y'
   ;