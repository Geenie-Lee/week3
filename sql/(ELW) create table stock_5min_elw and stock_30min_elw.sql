
DROP table if exists public.stock_30min_elw cascade;

CREATE TABLE public.stock_30min_elw (
	stock_code varchar(20) NOT NULL,
	stock_name varchar(200) NOT NULL,
	market_type varchar(2) NOT NULL,
	min_num int4 NOT NULL,
	conclusion_time varchar(14) NOT NULL,
	volume int4 NOT NULL DEFAULT 0,
	close_price int4 NOT NULL DEFAULT 0,
	open_price int4 NOT NULL DEFAULT 0,
	high_price int4 NOT NULL DEFAULT 0,
	low_price int4 NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX stock_30min_elw_uk ON public.stock_30min_elw USING btree (stock_code, min_num, conclusion_time DESC);




DROP table if exists public.stock_5min_elw cascade;

CREATE TABLE public.stock_5min_elw (
	stock_code varchar(20) NOT NULL,
	stock_name varchar(200) NOT NULL,
	market_type varchar(2) NOT NULL,
	min_num int4 NOT NULL,
	conclusion_time varchar(14) NOT NULL,
	volume int4 NOT NULL DEFAULT 0,
	close_price int4 NOT NULL DEFAULT 0,
	open_price int4 NOT NULL DEFAULT 0,
	high_price int4 NOT NULL DEFAULT 0,
	low_price int4 NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX stock_5min_elw_uk ON public.stock_5min_elw USING btree (stock_code, min_num, conclusion_time DESC);