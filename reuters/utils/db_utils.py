# -*- coding: utf-8 -*-
import psycopg2 as pg
import pandas as pd

mkt_dbconfig = {
    'dbname': 'mqdashboard',
    'user': 'dbreader',
    'password': 'cmcrc2018!',
    'host': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com'
}
mkt_conn = pg.connect(**mkt_dbconfig)
mkt_cur = mkt_conn.cursor()

ref_dbconfig = {
    'dbname': 'refdata',
    'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
    'port': '5432',
    'user': 'reader',
    'password': 'refdatareader2017!'
}
ref_conn = pg.connect(**ref_dbconfig)
ref_cur = ref_conn.cursor()


def get_mkt_info(mkt_uptick_name_list, cur):
  """
    Test market id query
    mkt_uptick_name_list = ['asx','sgx','johannesburg','istanbul','sao_paulo', 'lse', 'nasdaq']
    asx ASX
    sgx SES
    istanbul IST
    lse LSE
    nasdaq NAQ NMQ NSQ (NAS NMS NSM)
    """
  mkt_info_query = "select * from api_market a where a.uptick_name = ANY(%s);"
  cur.execute(mkt_info_query, [mkt_uptick_name_list])
  mkt_info_list = cur.fetchall()
  return mkt_info_list


def get_mkt_id(mkt_info_list):
  mkt_id_list = [i[0] for i in mkt_info_list]
  return mkt_id_list


def get_mkt_uptick_name(mkt_info_list):
  mkt_uptickname_list = [i[-1] for i in mkt_info_list]
  return mkt_uptickname_list


def get_mkt_sec_cur(mkt_id_list, be_date, en_date, cur):
  """
    mkt_id_list = [1, 3, 33, 38, 208]
    be_date = '2017-11-07'
    en_date = '2017-11-07'

    Test Query
    mkt_sec_cur_query = "select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds join api_tradable t on (ds.tradable_id = t.id) join api_market m on (ds.market_id = m.id) where ds.market_id in (1, 3, 33, 38, 208) and ds.date between '2017-11-07' and '2017-11-07' limit 10;"
    cur.execute(mkt_sec_cur_query)

    cur.mogrify(mkt_sec_cur_query, [mkt_id_list, be_date, en_date])
    """
  mkt_sec_cur_query = "select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds join api_tradable t on (ds.tradable_id = t.id) join api_market m on (ds.market_id = m.id) where ds.market_id = ANY(%s) and ds.date between %s and %s;"
  cur.execute(mkt_sec_cur_query, [mkt_id_list, be_date, en_date])
  mkt_sec_cur_list = cur.fetchall()

  return mkt_sec_cur_list


def get_tradable_list(mkt_sec_cur_list):
  tradable_list = list({":".join(i) for i in mkt_sec_cur_list})
  return tradable_list


def get_mkt_rics(mkt_uptick_name_list, tradable_list, be_date, en_date, cur):
  """

    Test Query
    mkt_rics_query = "select distinct trading_market as Market, symbol as RIC from refdata_tradablesymbolmap where trading_market in ('asx', 'sgx', 'istanbul', 'johannesburg', 'sao_paulo') and tradable in ('sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD', 'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL', 'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD', 'asx:BEL:AUD', 'sgx:MDRT:SGD') and date between '2018-04-06' and '2018-05-03' limit 10;"
    tradable_list = ['sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD', 'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL', 'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD', 'asx:BEL:AUD', 'sgx:MDRT:SGD']
    """
  mkt_rics_query = "select distinct trading_market as Market, symbol as RIC, tradable from refdata_tradablesymbolmap where trading_market = ANY(%s) and tradable = ANY(%s) and date between %s and %s;"
  cur.execute(mkt_rics_query,
              [mkt_uptick_name_list, tradable_list, be_date, en_date])
  mkt_rics_list = cur.fetchall()
  return mkt_rics_list

def save_ric_isin_map(mkt_rics_isin_list):
  df = pd.DataFrame(mkt_rics_isin_list)
  df.columns=['mkt','ric','ric_or_isin']
  df['ric_or_isin'] = df['ric_or_isin'].apply(lambda x: x.split(':')[1])
  df.to_csv('mkt_ric_isin_map.csv', index=False)

mkt_list = [
    'asx', 'sgx', 'johannesburg', 'istanbul', 'sao_paulo', 'nasdaq', 'lse'
]
mkt_info_list = get_mkt_info(mkt_list, mkt_cur)
mkt_id_list = get_mkt_id(mkt_info_list)
mkt_uptick_name_list = get_mkt_uptick_name(mkt_info_list)

be_date = '2018-02-01'
en_date = '2018-06-20'
mkt_sec_cur_list = get_mkt_sec_cur(mkt_id_list, be_date, en_date, mkt_cur)
tradable_list = get_tradable_list(mkt_sec_cur_list)
mkt_rics_list = get_mkt_rics(mkt_uptick_name_list, tradable_list, be_date,
                             en_date, ref_cur)
save_ric_isin_map(mkt_rics_list)

stream_dict = {
    'asx': 'ASX',
    'sgx': 'SGX',
    'johannesburg': 'JNB',
    'istanbul': 'IST',
    'sao_paulo': 'SAO',
    'lse': 'LSE',
    'nasdaq': ['NAS', 'NMS', 'NSM']
}


def get_compname_list(mkt_rics_list, be_date, en_date, cur):
  compname_query = "select distinct value from refdata_refdata where key='company' and date between %s and %s and symbol=%s and stream_name in %s;"
  compname_list = []
  for i in mkt_rics_list:
    s = stream_dict[i[0]]
    if isinstance(s, str):
      # WHERE IN operator
      # the trailing comma is mandantory for psql successfully map () to tuple
      # for WHERE IN operator
      s = (s,)
    else:
      s = tuple(s)

    # cur.mogrify(compname_query, (be_date, en_date, i[1], s))
    cur.execute(compname_query, (be_date, en_date, i[1], s))

    # todo: handle multi comp names
    comp_name = ref_cur.fetchall()
    if comp_name:
      if comp_name[0]:
        comp_name = comp_name[0][0]
    compname_list.append(comp_name)
  return compname_list


compname_list = get_compname_list(mkt_rics_list, be_date, en_date, ref_cur)


def get_mkt_ric_compname_df(mkt_rics_list, compname_list):
  tmp_mkt_ric_name_list = []
  for m, c in zip(mkt_rics_list, compname_list):
    tmp_mkt_ric_name_list.append(list(m) + [c])
  mkt_ric_compname_df = pd.DataFrame(tmp_mkt_ric_name_list)
  return mkt_ric_compname_df


mkt_ric_compname_df = get_mkt_ric_compname_df(mkt_rics_list, compname_list)

# mqd database without johannesburg, sao_paulo, sgx
out_df = mkt_ric_compname_df[mkt_ric_compname_df[0].str.contains('asx|istanbul|lse|nasdaq', regex=True)]
out_df.columns=['mkt','#RIC', 'Directory Company Name']
out_df.to_csv('ric_cmpname.csv', index=False)
