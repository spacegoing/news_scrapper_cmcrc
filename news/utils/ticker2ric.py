# -*- coding: utf-8 -*-
import pickle
import re
import pandas as pd
import psycopg2 as pg

ftr = re.compile(r'^[A-Z0-9]*\.SI$')

ref_dbconfig = {
    'dbname': 'refdata',
    'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
    'port': '5432',
    'user': 'reader',
    'password': 'refdatareader2017!'
}
ref_conn = pg.connect(**ref_dbconfig)
ref_cur = ref_conn.cursor()

# get ticker_ric_dict dict
df = pd.read_csv('sgx.csv', index_col=None)
ticker_df = df['RIC']
ticker_ric_dict = dict()
not_found = []
not_match = []
for ticker in ticker_df:
  ticker = ticker.split('.')[0]
  mkt = 'SES'
  # date = '2018-07-01'

  ref_cur.execute(
      "select date, symbol from refdata_refdata where stream_name = '%s' and value = '%s' and key = 'ticker_symbol' order by date desc limit 5"
      % (mkt, ticker))
  map_list = ref_cur.fetchall()

  if map_list:
    map_df = pd.DataFrame(map_list)
    map_df.columns = ['date', 'ric']
    map_df = map_df[map_df['ric'].apply(lambda x: ftr.search(x) != None)]
    if map_df.empty:
      not_match.append([ticker, map_list])
    else:
      ric = map_df.iloc[0]['ric']
      ticker_ric_dict[ticker] = ric
  else:
    not_found.append(ticker)

with open('ticker2ric.pickle', 'wb') as f:
  pickle.dump([ticker_ric_dict, not_found], f)


# recover sgx.csv
def recover_ric(x):
  ticker = x.split('.')[0]
  if ticker in ticker_ric_dict:
    return ticker_ric_dict[ticker]
  else:
    return None


df['RIC'] = df['RIC'].apply(recover_ric)
df = df.dropna()
df.to_csv('sgx_new.csv', index=False)

# load pcikle
with open('ticker2ric.pickle', 'rb') as f:
  # The protocol version used is detected automatically, so we do not
  # have to specify it.
  ticker_ric_dict, not_found_list = pickle.load(f)
