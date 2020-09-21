CREATE TABLE public.stock_day_theme (
	tmid int4 NOT NULL DEFAULT 0,
	tmcode varchar(20) NOT NULL,
	tmname varchar(200) NOT NULL,
	dt varchar(8) NULL,
	totcnt int4 NOT NULL DEFAULT 0,
	upcnt int4 NOT NULL DEFAULT 0,
	dncnt int4 NOT NULL DEFAULT 0,
	uprate varchar(20) NOT NULL DEFAULT '0.00'::character varying,
	diff_vol varchar(20) NOT NULL DEFAULT '0.00'::character varying,
	avgdiff varchar(20) NOT NULL DEFAULT '0.00'::character varying,
	chgdiff varchar(20) NOT NULL DEFAULT '0.00'::character varying
);

 


drop table stock_day_kosdaq cascade;

create table stock_day_kosdaq (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int4 not null default 0,
	trading_value int8 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0
);


drop table stock_day_kospi cascade;

create table stock_day_kospi (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int4 not null default 0,
	trading_value int8 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0
);


drop table stock_day_etf cascade;

create table stock_day_etf (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int4 not null default 0,
	trading_value int8 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0
);



drop table stock_day_konex cascade;

create table stock_day_konex (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int4 not null default 0,
	trading_value int8 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0
);


drop table stock_day_elw cascade;

create table stock_day_elw (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int4 not null default 0,
	trading_value int8 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0
);

drop table stock_day_kotc cascade;

create table stock_day_kotc (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	market_type varchar(2) not null,
	day_num int4 not null,
	dt varchar(8) not null,
	volume int8 not null default 0,
	trading_value int8 not null default 0,
	close_price int8 not null default 0,
	open_price int8 not null default 0,
	high_price int8 not null default 0,
	low_price int8 not null default 0
);




insert into stock_day_kospi
select *
  from stock_day_info sdi 
 where market_type = '0'
   and dt <= '20200730';

update stock_day_kospi s                                                     
   set day_num = a.day_num                                                            
  from                                                                                
     (                                                                                
       select row_number() over(partition by stock_code order by dt desc) as day_num  
            , stock_code                                                              
            , dt                                                                      
            , market_type                                                             
         from stock_day_kospi                                                       
     ) as a                                                                           
 where s.stock_code = a.stock_code                                                    
   and s.dt = a.dt                                                                    
   and s.market_type = a.market_type;       
  
create unique index stock_day_kospi_uk on stock_day_kospi (stock_code, dt desc);  

insert into stock_day_kosdaq
select *
  from stock_day_info sdi 
 where market_type = '10'
   and dt <= '20200730';
   
create unique index stock_day_kosdaq_uk on stock_day_kosdaq (stock_code, dt desc);

update stock_day_kosdaq s                                                     
   set day_num = a.day_num                                                            
  from                                                                                
     (                                                                                
       select row_number() over(partition by stock_code order by dt desc) as day_num  
            , stock_code                                                              
            , dt                                                                      
            , market_type                                                             
         from stock_day_kosdaq                                                       
     ) as a                                                                           
 where s.stock_code = a.stock_code                                                    
   and s.dt = a.dt                                                                    
   and s.market_type = a.market_type;       

commit;  