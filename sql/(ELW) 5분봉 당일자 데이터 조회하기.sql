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
  from stock_5min_elw
 where stock_code = '57G166'
 group by stock_code,stock_name;
 


select *
  from stock_5min_elw
 where stock_code = '57G166'
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd')
 order by min_num desc;
 

-------------------------------------------------------------------------------
-- (57G188) PUT
-------------------------------------------------------------------------------

select *
  from stock_5min_elw
 where stock_code = '57G188'
   and substring(conclusion_time,1,8) = to_char(current_date,'yyyymmdd');