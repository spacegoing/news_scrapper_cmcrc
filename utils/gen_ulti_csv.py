# -*- coding: utf-8 -*-
import re
import psycopg2 as pg
import pandas as pd


def filter_not_in_dailystats(df, be_date, en_date):
  ric_ser = df.loc[df['Market'] != 'lse', 'RIC']

  mkt_dbconfig = {
      'dbname': 'mqdashboard',
      'user': 'mqdashboard',
      'port': '5432',
      'password': 'I99ub6Lw',
      'host': 'mqd-staging-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com'
  }
  mkt_conn = pg.connect(**mkt_dbconfig)
  mkt_cur = mkt_conn.cursor()
  mkt_info_query = "select * from api_dailystats where market_id in (1,3,7,208) and date between %s and %s"
  mkt_cur.execute(mkt_info_query, [be_date, en_date])
  mkt_info_list = mkt_cur.fetchall()
  mkt_df = pd.DataFrame(mkt_info_list)
  mkt_ric_set = set(mkt_df[3])

  notin_ser = ric_ser.apply(lambda x: x[:x.find('.')] not in mkt_ric_set)
  notin_mkt_count_df = df.loc[ric_ser[notin_ser].index].groupby(
      'Market').count()
  in_mkt_ratio_df = df.loc[df['Market'] != 'lse'].groupby('Market').count(
  ).subtract(
      notin_mkt_count_df,
      fill_value=0) / df.loc[df['Market'] != 'lse'].groupby('Market').count()

  print("not in dailystats numbers")
  print(notin_mkt_count_df['RIC'])
  print("in dailystats ratio")
  print(in_mkt_ratio_df['RIC'])

  append_lse_df = pd.concat(
      [df.loc[ric_ser[~notin_ser].index], df[df['Market'] == 'lse']], axis=0)
  return append_lse_df


if __name__ == "__main__":
  import pickle

  # 0 market 1 security
  be_date = '2018-11-01'
  en_date = '2018-11-30'
  df = pd.read_csv(
      'result_asx_lse_nasdaq_sgx_sao_paulo_%s_%s.csv' % (be_date, en_date))
  # Fix sao_paulo ric errors in scrape urls
  mapdir = '/home/lchang/mqdCodeLab/news_scrapper_cmcrc/reuters/sao_map_dict.pickle'
  with open(mapdir, 'rb') as f:
    sao_map_dict = pickle.load(f)

  def map_sao_ric(x):
    if x.endswith('.SA'):  # only sao_paulo
      return sao_map_dict[x]
    else:
      return x

  df['RIC'] = df['RIC'].apply(map_sao_ric)
  df = filter_not_in_dailystats(df, be_date, en_date)
  df.to_csv(
      'result_asx_lse_nasdaq_sgx_sao_paulo_%s_%s.csv' % (be_date, en_date),
      index=False,
      column=False)
