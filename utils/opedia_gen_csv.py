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
    'ASX_News', 'NAQ_News', 'NMQ_News', 'NSQ_News', 'SES_News', 'LSE_News'
]
# mkt_list = [
#     'asx', 'sao_paulo', 'lse', 'nasdaq'
# ]
col_list = [db[k] for k in mkt_list]

# be_date = '2018-01-31'
# en_date = '2018-05-04'
be_date = '2018-03-31'
en_date = '2018-05-01'
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
  news_df['Market'] = col.name
  news_df_list.append(news_df)

df_total = pd.concat(news_df_list)
df_total.rename(
    columns={
        'date': 'TimestampUTC',
        'ric': 'RIC',
        'title': 'Headline'
    },
    inplace=True)

df_total.to_csv('0410.csv', index=False)


def title_filter(x):
  want = ['brief', 'update']
  flag_list = []
  for w in want:
    tmp_flag = False
    if w in x.lower():
      tmp_flag = True
    flag_list.append(tmp_flag)
  return any(flag_list)


real_news_rows_id = df_total['Headline'].apply(title_filter)

out_total = df_total[['RIC', 'Market', 'TimestampUTC', 'Headline']]
out_total = out_total[real_news_rows_id]


def recover_isin(out_total):
  mkt_ric_isin_map = pd.read_csv('mkt_ric_isin_map.csv', index_col=None)
  mkt_ric_isin_dict = {(i[0], i[1]): i[2] for i in mkt_ric_isin_map.as_matrix()}

  def recover(x):
    string = x['RIC']
    candidate = mkt_ric_isin_dict.get((x['Market'], x['RIC']), '')
    if len(candidate) >= 10:
      string = candidate
    return string

  out_total['ISIN'] = out_total.apply(recover, axis=1)


recover_isin(out_total)


def filter_double_per(x):
  return x.count('.') < 2


out_total = out_total[out_total['RIC'].apply(filter_double_per)]

out_total.to_csv(
    'result_asx_sgx_johannesburg_istanbul_sao_paulo_lse_nasdaq_%s_%s.csv' %
    (be_date, en_date),
    index=False)
