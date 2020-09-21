
drop table stock_base_info cascade;

create table stock_base_info (
	   stock_code varchar(20) not null
	 , stock_name varchar(200) not null
	 , market_type varchar(2) not null
	 , dt varchar(8)
	 , volume integer default 0 not null
	 , last_price bigint default 0 not null
	 , close_price integer default 0 not null
	 , open_price integer default 0 not null
	 , high_price integer default 0 not null
	 , low_price integer default 0 not null
	 , per varchar(20)
	 , eps varchar(20)
	 , roe varchar(20)
	 , pbr varchar(20)
	 , bps varchar(20)
	 , listed_stocks varchar(20)
	 , outstanding_stocks varchar(20)
	 , settlement_m varchar(8)
	 , market_capitalization varchar(20)
	 , turnover varchar(20)
	 , business_profit varchar(20)
	 , net_profit varchar(20)
);


drop table stock_day_info cascade;

create table stock_day_info (
	   stock_code varchar(20) not null
	 , stock_name varchar(200) not null
	 , market_type varchar(2) not null
	 , day_num integer not null
	 , dt varchar(8) not null
	 , volume integer default 0 not null
	 , trading_value bigint default 0 not null
	 , close_price integer default 0 not null
	 , open_price integer default 0 not null
	 , high_price integer default 0 not null
	 , low_price integer default 0 not null
);

select stock_code
     , ma_120
     , ma_005
     , ma_120-ma_005 as gap_price
  from 
     ( 
	   select a.stock_code
		    , a.sum_close_price/a.cnt as ma_120
		    , b.sum_close_price/b.cnt as ma_005
		 from 
		    (
		   	  select stock_code
			       , sum(close_price) as sum_close_price
			       , count(*) as cnt
			 	from stock_day_info
			   where day_num <= 120
			   group by stock_code  
		    ) a
		    ,
		    (
		      select stock_code
			       , sum(close_price) as sum_close_price
			       , count(*) as cnt
			    from stock_day_info
			   where day_num <= 20
			   group by stock_code  
		    ) b
		where a.stock_code = b.stock_code      
     ) c
 where ma_005 <= ma_120     
;


select stock_code
     , ma_120
     , ma_005
     , ma_120-ma_005 as gap_price
  from 
     ( 
	   select a.stock_code
		    , a.sum_close_price/a.cnt as ma_120
		    , b.sum_close_price/b.cnt as ma_005
		 from 
		    (
		   	  select stock_code
			       , sum(close_price) as sum_close_price
			       , count(*) as cnt
			 	from stock_day_info
			   where day_num <= 112
			     and stock_code = '207760'
			   group by stock_code  
		    ) a
		    ,
		    (
		      select stock_code
			       , sum(close_price) as sum_close_price
			       , count(*) as cnt
			    from stock_day_info
			   where day_num <= 5
			     and stock_code = '207760'
			   group by stock_code  
		    ) b
		where a.stock_code = b.stock_code      
     ) c
 where ma_005 <= ma_120     
;


select *
  from stock_day_info 
 where stock_code = '207760'
   and day_num <= 6;
  

  
select *
  from stock_day_info 
 where day_num = 1;


-------------------------------------------------------------------------------
-- 그랜빌 매수 원칙 적용: 120일선 20일선 비교
-------------------------------------------------------------------------------
-- 1. 120일 평균보다 20일 평균가격이 낮아야한다.
-- 2. 최근 20일 동안 한번도 120일선을 돌파한 적이 없어야한다.
-- 3. 당일자 고가와 저가 사이에 120일선이 걸려있어야 한다.
-------------------------------------------------------------------------------

select b.stock_code
     , b.ma_120
     , b.ma_020
     , b.gap_price
  from
	 ( select a.stock_code
	        , max(a.close_price) as close_price
	     from stock_day_info a
	    where a.day_num <= 20
	    group by a.stock_code
	 ) a
     ,
     (
	   select stock_code
		    , ma_120
		    , ma_020
		    , ma_120-ma_020 as gap_price
		 from 
		    ( 
		      select a.stock_code
			       , a.sum_close_price/a.cnt as ma_120
			       , b.sum_close_price/b.cnt as ma_020
				from 
				   (
				 	 select stock_code
					      , sum(close_price) as sum_close_price
					      , count(*) as cnt
					   from stock_day_info
					  where day_num <= 120
					  group by stock_code  
				   ) a
				   ,
				   (
				     select stock_code
					      , sum(close_price) as sum_close_price
					      , count(*) as cnt
					   from stock_day_info
					  where day_num <= 20
					  group by stock_code  
				   ) b
			   where a.stock_code = b.stock_code      
		     ) c
		 where ma_020 <= ma_120      
     ) b
     , 
     (
	   select stock_code
		    , close_price
		    , high_price
		    , low_price
		 from stock_day_info 
		where day_num = 1
     ) c
 where a.stock_code = b.stock_code
   and a.close_price <= ma_120
   and b.stock_code = c.stock_code
   and b.ma_120 between c.low_price and c.high_price;
--   and b.ma_120 <= c.high_price;
  

select * from stock_day_info where stock_code = '030000';
  
select * 
  from stock_day_info 
 where day_num = 1 
   and volume >= 10000000 
   and market_type = '8'
 order by volume desc;
 
  
-- 112, 5로 변환 처리 필요(06.14)  
select b.stock_code
     , b.ma_120
     , b.ma_020
     , b.gap_price
  from
	 ( select a.stock_code
	        , max(a.close_price) as close_price
	     from stock_day_info a
	    where a.day_num <= 20
	    group by a.stock_code
	 ) a
     ,
     (
	   select stock_code
		    , ma_120
		    , ma_020
		    , ma_120-ma_020 as gap_price
		 from 
		    ( 
		      select a.stock_code
			       , a.sum_close_price/a.cnt as ma_120
			       , b.sum_close_price/b.cnt as ma_020
				from 
				   (
				 	 select stock_code
					      , sum(close_price) as sum_close_price
					      , count(*) as cnt
					   from stock_day_info
					  where day_num <= 120
					  group by stock_code  
				   ) a
				   ,
				   (
				     select stock_code
					      , sum(close_price) as sum_close_price
					      , count(*) as cnt
					   from stock_day_info
					  where day_num <= 20
					  group by stock_code  
				   ) b
			   where a.stock_code = b.stock_code      
		     ) c
		 where ma_020 <= ma_120      
     ) b
     , 
     (
	   select stock_code
		    , close_price
		    , high_price
		    , low_price
		 from stock_day_info 
		where day_num = 1     
     ) c
 where a.stock_code = b.stock_code
   and a.close_price <= ma_120
   and b.stock_code = c.stock_code
   and b.ma_120 between c.low_price and c.high_price;
   

  
  
select *
  from stock_day_info
 where market_type = '8';



update stock_day_info
   set day_num = day_num + 1
 where    
 