# -*- coding: utf-8 -*-
import pandas as pd
import dateparser as dp
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['reuters_news_content']


def concat_mg_col(mkt_list, be_date, en_date):
  col_list = [db[k] for k in mkt_list]

  news_df_list = []
  for col in col_list:
    news_list = list(
        col.find({
            "date": {
                "$gte": dp.parse(be_date),
                "$lte": dp.parse(en_date)
            }
        }))
    news_df = pd.DataFrame(news_list)
    news_df['Market'] = col.name
    news_df_list.append(news_df)

  df_total = pd.concat(news_df_list)
  return df_total


def title_filter(x):
  want = ['brief', 'update']
  flag_list = []
  for w in want:
    tmp_flag = False
    if w in x.lower():
      tmp_flag = True
    flag_list.append(tmp_flag)
  return any(flag_list)


def recover_isin(out_total):
  mkt_ric_isin_map = pd.read_csv('mkt_ric_isin_map.csv', index_col=None)
  mkt_ric_isin_dict = {(i[0], i[1]): i[2] for i in mkt_ric_isin_map.values}

  def recover(x):
    string = x['RIC']
    candidate = mkt_ric_isin_dict.get((x['Market'], x['RIC']), '')
    if len(candidate) >= 10:
      string = candidate
    return string

  out_total['ISIN'] = out_total.apply(recover, axis=1)


def filter_double_per(x):
  return x.count('.') < 2


def reuters_pipeline(df_total):
  # rename column
  rename_dict = {'date': 'TimestampUTC', 'ric': 'RIC', 'title': 'Headline'}
  df_total.rename(columns=rename_dict, inplace=True)

  # select columns
  out_total = df_total[['RIC', 'Market', 'TimestampUTC', 'Headline']]

  # # filter noise news
  # real_news_rows_id = df_total['Headline'].apply(title_filter)
  # out_total = out_total[real_news_rows_id]

  # recover isin
  recover_isin(out_total)
  out_total = out_total[out_total['RIC'].apply(filter_double_per)]

  out_total.to_csv(
      'result_%s_%s_%s.csv' % ('_'.join(mkt_list), be_date, en_date),
      index=False)


if __name__ == "__main__":
  # mkt_list = [
  #     'asx', 'sgx', 'lse', 'nasdaq', 'johannesburg', 'sao_paulo'
  # ]
  # mkt_list = [
  #     'asx', 'sgx', 'lse', 'nasdaq'
  # ]
  mkt_list = ['sao_paulo', 'johannesburg']

  # be_date = '2018-01-31'
  # en_date = '2018-05-04'
  be_date = '2018-09-01'
  en_date = '2018-09-30'
  df_total = concat_mg_col(mkt_list, be_date, en_date)
  reuters_pipeline(df_total)

  '''
  mongodb script:
  find news in date range
  db.sao_paulo.find({'date':{$gte:ISODate("2018-09-01"),$lte:ISODate("2018-09-31")}}).count()
  '''
