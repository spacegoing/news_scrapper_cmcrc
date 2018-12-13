# -*- coding: utf-8 -*-
import re
import psycopg2 as pg
import pandas as pd
import pickle
csvdir = '/home/lchang/mqdCodeLab/news_scrapper_cmcrc/reuters/'
raw_df = pd.read_csv(
    csvdir + 'final_mqd_nodata.csv', index_col=None, header=None)
# 0 market 1 security
raw_ticker_ser = raw_df.loc[raw_df[0] == 'sao_paulo', 1]


def ft(x):
  reg = re.compile('[a-zA-Z]{4}\d{0,2}')
  return reg.match(x)


def trunc(x):
  if x.startswith('1'):
    x = x[1:]
  if x.endswith('F'):
    x = x[:-1]
  x += '.SA'
  return x


sao_map_dict = dict()


def isf1(s):
  sao_map_dict[s] = s
  x = s[:-3]  # remove suffix
  if x.startswith('1') or x.endswith('F'):
    x = trunc(x)
    sao_map_dict[s] = x
    return True
  return False


raw_ticker_ser.apply(isf1)
with open(csvdir+'sao_map_dict.pickle', 'wb') as f:
  pickle.dump(sao_map_dict, f)

# toremove_ser = raw_ticker_ser[raw_ticker_ser.apply(isf1)]
# trunced_ser = toremove_ser.apply(trunc)
# raw_ticker_ser.loc[trunced_ser.index] = trunced_ser
# raw_ticker_ser[raw_ticker_ser.apply(ft).isnull()]

if __name__ == "__main__":
  mkt_dbconfig = {
      'dbname': 'mqdashboard',
      'user': 'dbreader',
      'port': '5432',
      'password': 'cmcrc2018!',
      'host': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com'
  }
  mkt_conn = pg.connect(**mkt_dbconfig)
  mkt_cur = mkt_conn.cursor()

  mkt_info_query = "select * from api_dailystats where market_id=3 and date='2018-11-01'"
  mkt_cur.execute(mkt_info_query)
  mkt_info_list = mkt_cur.fetchall()

  df = pd.DataFrame(mkt_info_list)
  ticker_ser = df[3]
  apidaily_set = set(ticker_ser)

  gdf = pd.read_csv('/home/lchang/mqdCodeLab/news_scrapper_cmcrc/utils/result_asx_lse_nasdaq_sgx_2018-11-01_2018-11-30.csv', index_col=None)
  sgx_ser = gdf.loc[gdf['Market']=='sgx', 'RIC']
  notin_mask = sgx_ser.apply(lambda x: x[:-3] not in apidaily_set)
