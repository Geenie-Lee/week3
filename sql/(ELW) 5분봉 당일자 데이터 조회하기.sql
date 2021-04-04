-------------------------------------------------------------------------------
-- (57G166) CALL
-------------------------------------------------------------------------------

select *
  from stock_5min_elw
 where stock_code = '57G166'
   and close_price = 250
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd');
 
select *
  from stock_5min_elw
 where stock_code = '57G166'
   and close_price = 330;

select stock_code
     , stock_name
     , max(close_price) as max_close_price
     , min(close_price) as min_close_price
     , max(high_price) as max_high_price
     , min(low_price) as min_low_price
     , max(conclusion_time) as conclusion_time
     , round(avg(close_price)) as avg_close_price
  from stock_5min_elw
 where stock_code in ('57G166','57G188','57G194')
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
 group by stock_code,stock_name;
 

select *
  from stock_5min_elw
 where stock_code in ('57G166','57G188')
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
   and substring(conclusion_time,9,2) between '11' and '13'
 order by stock_code, min_num;


select *
  from stock_5min_elw
 where stock_code = '57G166'
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
   and substring(conclusion_time,9,2) = '10'
 order by min_num desc;
 

-------------------------------------------------------------------------------
-- (57G188) PUT
-------------------------------------------------------------------------------

select stock_code
     , stock_name
     , min_num
     , substring(conclusion_time,9,2)||':'||substring(conclusion_time,11,2) as hhmm
     , volume
     , close_price
     , open_price
     , high_price
     , low_price
     , high_price-low_price as hl_gap_price
  from stock_5min_elw
 where stock_code = '57G188'
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
 order by min_num desc;
 



-------------------------------------------------------------------------------
-- 거래량 순위
-------------------------------------------------------------------------------

select *
  from
     (
		select *
		     , row_number() over(partition by stock_code order by volume desc) as seq
		  from stock_5min_elw
		 where substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')     
     ) a
 where seq = 1;




select *
  from 
     (
		select stock_code
		     , stock_name
		     , sum(volume) as sum_volume
		     , round(avg(volume)) as avg_volume
		     , max(volume) as max_volume
		     , min(volume) as min_volume
		  from stock_5min_elw
		 where substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
		 group by stock_code,stock_name     
     ) a
 where 1=1
 order by sum_volume desc;

;




 

