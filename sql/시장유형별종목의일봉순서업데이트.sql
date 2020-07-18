select *
  from stock_day_info sdi 
 order by stock_code,day_num ;

update stock_day_info s
   set day_num = a.day_num
  from 
     (
	   select row_number() over(partition by stock_code order by dt desc) as day_num
		    , stock_code
		    , dt
		    , market_type 
		 from stock_day_info 
		where market_type = '8'
     ) as a
 where s.stock_code = a.stock_code
   and s.dt = a.dt
   and s.market_type = a.market_type
   and s.market_type = '8'
;

commit;


	   select row_number() over(partition by stock_code order by dt desc) as day_num
		    , stock_code
		    , dt
		    , market_type 
		 from stock_day_info  
		where market_type = '8';

select * from stock_day_info where market_type = '8' and stock_code = '185680' order by day_num ;
 
create index stock_day_info_ix01 on stock_day_info (stock_code);

create index stock_day_info_ix02 on stock_day_info (stock_code,dt);

create index stock_day_info_ix03 on stock_day_info (stock_code,day_num);

create index stock_day_info_ix03 on stock_day_info (stock_code,dt,day_num);
 
drop index stock_day_info_uk;

select *
  from 