# -*- coding: utf-8 -*-
import pandas as pd
import dateparser as dp
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['stockopedia_news']
# mkt_list = [
#     'asx', 'sgx', 'johannesburg', 'sao_paulo', 'lse', 'nasdaq'
# ]
mkt_list = [
    'NAQ_News', 'NMQ_News', 'NSQ_News', 'ASX_News', 'SES_News', 'LSE_News'
]
uptick_list = ['nasdaq', 'nasdaq', 'nasdaq', 'asx', 'sgx', 'lse']
col_list = [db[k] for k in mkt_list]
uptick_dict = {k: v for k, v in zip(mkt_list, uptick_list)}
suf_map = {'nasdaq': 'OQ', 'asx': 'AX', 'sgx': 'SI'}

# be_date = '2018-01-31'
# en_date = '2018-05-04'
be_date = '2018-07-31'
en_date = '2018-09-01'
news_df_list = []
for col in col_list:
  news_list = list(
      col.find({
          "date": {
              "$gt": dp.parse(be_date),
              "$lt": dp.parse(en_date)
          }
      }))
  news_df = pd.DataFrame(news_list)
  news_df['Market'] = uptick_dict[col.name]
  news_df_list.append(news_df)


def recover_isin(out_total):
  mkt_ric_isin_map = pd.read_csv('mkt_ric_isin_map.csv', index_col=None)
  # mkt_ric_isin_map = mkt_ric_isin_map[mkt_ric_isin_map['mkt']=='lse']
  # mkt_ric_isin_map.to_csv('mkt_ric_isin_map.csv', index=False)
  # dict: ric_no_suffix: [isin, ric_suffix]
  mkt_ric_isin_dict = {
      (i[0], i[1].split('.')[0]): [i[2], i[1]] for i in mkt_ric_isin_map.values
  }

  def recover(x):
    '''
    x['RIC']: ric withouth suffix
    '''
    string = x['RIC']
    if x['Market'] == 'lse':
      candidate = mkt_ric_isin_dict.get((x['Market'], x['RIC']), '')
      if candidate:
        isin, ric_suffix = candidate
        if len(isin) >= 10:
          string = isin
          x['RIC'] = ric_suffix
    return string

  def append_suf(out_total):
    for mkt, suf in suf_map.items():
      out_total.loc[
          out_total['Market'] == mkt,
          'RIC'] = out_total[out_total['Market'] == mkt]['RIC'] + '.' + suf

    mkt = 'lse'
    out_total.loc[out_total['Market'] == mkt, 'RIC'] = out_total[out_total[
        'Market'] == mkt]['RIC'].apply(
            lambda x: mkt_ric_isin_dict.get(('lse', x['RIC']), ''))

  out_total['ISIN'] = out_total.apply(recover, axis=1)


def gen_csv(df_list, fn):
  df_total = pd.concat(df_list)
  df_total.rename(
      columns={
          'date': 'TimestampUTC',
          'ticker': 'RIC',
          'title': 'Headline'
      },
      inplace=True)

  out_total = df_total[['RIC', 'Market', 'TimestampUTC', 'Headline']]
  recover_isin(out_total)

  def filter_double_per(x):
    return x.count('.') < 2

  out_total = out_total[out_total['RIC'].apply(filter_double_per)]
  out_total.to_csv(fn + '_%s_%s.csv' % (be_date, en_date), index=False)


gen_csv(news_df_list[:3], 'nasdaq')
gen_csv([news_df_list[3]], 'asx')
gen_csv([news_df_list[4]], 'sgx')
gen_csv([news_df_list[5]], 'lse')
