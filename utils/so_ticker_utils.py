# -*- coding: utf-8 -*-
import pickle
import re
import pandas as pd
import psycopg2 as pg

ref_dbconfig = {
    'dbname': 'refdata',
    'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
    'port': '5432',
    'user': 'reader',
    'password': 'refdatareader2017!'
}
ref_conn = pg.connect(**ref_dbconfig)
ref_cur = ref_conn.cursor()

def get_mkt_ticker_map(df, stream_tuple, ftr_func=None):
  ticker_df = df['ticker']
  ticker_ric_dict = dict()

  not_found = []  # ticker not found in refdata
  not_match = []  # found tickers but none pass ftr rule
  for ticker in ticker_df:
    ticker = ticker.split('.')[0]

    ref_cur.execute(
        "select date, symbol from refdata_refdata where stream_name in %s and value = %s and key = 'ticker_symbol' order by date desc limit 5",
        (stream_tuple, ticker))
    # ref_cur.mogrify(
    #     "select date, symbol from refdata_refdata where stream_name in %s and value = %s and key = 'ticker_symbol' order by date desc limit 5",
    #     (stream_tuple, ticker))
    map_list = ref_cur.fetchall()

    if map_list:
      map_df = pd.DataFrame(map_list)
      map_df.columns = ['date', 'ric']
      if ftr_func:
        map_df = map_df[map_df['ric'].apply(ftr_func)]
      if map_df.empty:
        not_match.append([ticker, map_list])
      else:
        # only use latest ticker
        ric = map_df.iloc[0]['ric']
        ticker_ric_dict[ticker] = ric
    else:
      not_found.append(ticker)

  return ticker_ric_dict, not_found, not_match


def ticker2ric_df(df, ticker_ric_dict):

  def recover_ric(x):
    ticker = x.split('.')[0]
    return ticker_ric_dict.get(ticker, None)

  df['RIC'] = df['ticker'].apply(recover_ric)
  df = df.dropna(inplace=True)


def sgx_pipeline(df):
  stream_tuple = ('SES',)

  # df = pd.read_csv('sgx.csv', index_col=None)
  def sgx_ftr_func(x):
    '''
    filter candidate ric results returned from refdata
    for a ticker
    '''
    ftr = re.compile(r'^[A-Z0-9]*\.SI$')
    return ftr.search(x) != None

  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(
      df, stream_tuple, sgx_ftr_func)

  ticker2ric_df(df, ticker_ric_dict)

  # df.to_csv('sgx_new.csv', index=False)

  # with open('ticker2ric.pickle', 'wb') as f:
  #   pickle.dump([ticker_ric_dict, not_found], f)

  # # load pcikle
  # with open('ticker2ric.pickle', 'rb') as f:
  #   # The protocol version used is detected automatically, so we do not
  #   # have to specify it.
  #   ticker_ric_dict, not_found_list = pickle.load(f)

  return not_found, not_match


def lse_pipeline(df):
  stream_tuple = ('LSE',)
  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(df, stream_tuple)
  ticker2ric_df(df, ticker_ric_dict)
  return not_found, not_match


def nasdaq_pipeline(df):
  stream_tuple = ('NAS', 'NMS', 'NSM', 'NBA', 'NBN')
  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(df, stream_tuple)
  ticker2ric_df(df, ticker_ric_dict)
  return not_found, not_match


def asx_pipeline(df):
  stream_tuple = ('ASX',)
  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(df, stream_tuple)
  ticker2ric_df(df, ticker_ric_dict)
  return not_found, not_match


def johannesburg_pipeline(df):
  stream_tuple = ('JNB_ETF',)
  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(df, stream_tuple)
  ticker2ric_df(df, ticker_ric_dict)
  return not_found, not_match


def sao_paulo_pipeline(df):
  stream_tuple = ('SAO',)
  ticker_ric_dict, not_found, not_match = get_mkt_ticker_map(df, stream_tuple)
  ticker2ric_df(df, ticker_ric_dict)
  return not_found, not_match

pipelines_dict = {
    'sgx': sgx_pipeline,
    'sao_paulo': sao_paulo_pipeline,
    'asx': asx_pipeline,
    'lse': lse_pipeline,
    'nasdaq': nasdaq_pipeline,
    'johannesburg': johannesburg_pipeline
}


