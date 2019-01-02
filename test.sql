-- 'DBNAME': 'mqdashboard',
-- 'USER': 'dbreader',
-- 'PASSWORD': 'cmcrc2018!',
-- 'SERVER': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds
join api_tradable t on (ds.tradable_id = t.id)
join api_market m on (ds.market_id = m.id)
where ds.market_id in (1, 3, 33, 38, 208)
and ds.date between '2018-04-06' and '2018-05-03' LIMIT 10;

select * from api_market a where a.uptick_name = ANY(array['asx','sgx','johannesburg','istanbul','sao_paulo']);

select distinct trading_market as Market, symbol as RIC from
refdata_tradablesymbolmap where trading_market in ('asx', 'sgx',
'istanbul', 'johannesburg', 'sao_paulo') and tradable in
('sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD',
'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL',
'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD',
'asx:BEL:AUD', 'sgx:MDRT:SGD') and date between '2018-04-06' and
'2018-05-03' limit 10;

select * from refdata_refdata where key='company' and date between '2017-11-07' and '2017-12-07' and symbol='1AD.AX' and stream_name='ASX';

select * from refdata_refdata where key='company' and date between '2017-11-07' and '2017-12-07' and symbol='MRNS.OQ' and stream_name in ('NAS', 'NMS', 'NSM');

select distinct value from refdata_refdata where symbol='ZNH.SI' and stream_name='SGX';

-- ticker
select * from refdata_refdata
where stream_name in ('LSE')
and value = 'ABC' and key = 'ticker_symbol' limit 2;

-- dangerous user
-- 'ENGINE': 'django.db.backends.postgresql_psycopg2',
-- 'NAME': 'mqdashboard',
-- 'USER': 'mqdashboard',
-- 'PASSWORD': 'I99ub6Lw',
-- 'HOST': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com',

select * from refdata_tradablesymbolmap
where stream_name='NSM' and trading_market='nasdaq' and tradable='nasdaq:ZG:USD'
limit 1;

select date, stream_name, symbol from refdata_tradablesymbolmap
where trading_market='nasdaq' and date<='2015-10-19' and date>='2015-09-01' order by abs(date
'2016-03-31'-date) limit 10;

select date, stream_name, symbol from refdata_tradablesymbolmap
where trading_market='nasdaq' and tradable='nasdaq:PLTMnv:USD' limit 10;

-- 'dbname': 'refdata',
-- 'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
-- 'port': '5432',
-- 'user': 'reader',
-- 'password': 'refdatareader2017!'

-- find currency when convert_upload missing
select * from refdata_refdata where symbol='AMOS.SI' and stream_name='SES' and key='currency' and date>='2018-11-01' limit 2;
