
-- drop table


-- stock_code,stock_name,min_num,conclusion_time,volume,close_price,open_price,high_price,low_price,market_type

drop table if exists public.stock_30min_kosdaq cascade;

create table public.stock_30min_kosdaq (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	min_num int4 not null,
	conclusion_time varchar(20) not null,
	volume int4 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0,
	market_type varchar(2) not null
);

create unique index stock_30min_kosdaq_uk on public.stock_30min_kosdaq using btree (stock_code, conclusion_time desc);



drop table if exists public.stock_30min_kospi cascade;

create table public.stock_30min_kospi (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	min_num int4 not null,
	conclusion_time varchar(20) not null,
	volume int4 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0,
	market_type varchar(2) not null
);

create unique index stock_30min_kospi_uk on public.stock_30min_kospi using btree (stock_code, conclusion_time desc);



drop table if exists public.stock_30min_elw cascade;

create table public.stock_30min_elw (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	min_num int4 not null,
	conclusion_time varchar(20) not null,
	volume int4 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0,
	market_type varchar(2) not null
);

create unique index stock_30min_elw_uk on public.stock_30min_elw using btree (stock_code, conclusion_time desc);





drop table if exists public.stock_5min_elw cascade;

create table public.stock_5min_elw (
	stock_code varchar(20) not null,
	stock_name varchar(200) not null,
	min_num int4 not null,
	conclusion_time varchar(20) not null,
	volume int4 not null default 0,
	close_price int4 not null default 0,
	open_price int4 not null default 0,
	high_price int4 not null default 0,
	low_price int4 not null default 0,
	market_type varchar(2) not null
);

create unique index stock_5min_elw_uk on public.stock_5min_elw using btree (stock_code, conclusion_time desc);


