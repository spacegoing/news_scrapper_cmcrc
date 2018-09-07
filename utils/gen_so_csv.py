# -*- coding: utf-8 -*-
import pandas as pd
import dateparser as dp
from pymongo import MongoClient
from so_ticker_utils import pipelines_dict

client = MongoClient('mongodb://localhost:27017/')
db = client['stockopedia_news']

mqd_opedia_dict = {
    "asx": ("ASX_News",),
    "sgx": ("SES_News",),
    'lse': ("LSE_News",),
    'nasdaq': ("NAQ_News", "NMQ_News", "NSQ_News")
    # "johannesburg": ("JSE_News",),
    # 'sao_paulo': ("SAO_News",),
}


def get_mqd_mkt(col_name):
  uptick_name = ''
  for k, v in mqd_opedia_dict.items():
    if col_name in v:
      uptick_name = k
      break
  return uptick_name


def concat_mg_col(mkt_list, be_date, en_date):
  col_list = [db[k] for k in mkt_list]

  news_df_list = []
  missing_ticker_dict = dict()
  for col in col_list:
    news_list = list(
        col.find({
            "date": {
                "$gte": dp.parse(be_date),
                "$lte": dp.parse(en_date)
            }
        }))
    news_df = pd.DataFrame(news_list)
    uptick_name = get_mqd_mkt(col.name)
    news_df['Market'] = uptick_name
    # stockopedia: ticker to ric
    missing_list = pipelines_dict[uptick_name](news_df)
    missing_ticker_dict[col.name] = missing_list
    news_df_list.append(news_df)

  df_total = pd.concat(news_df_list)
  return df_total, missing_ticker_dict


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


def reuters_pipeline(df_total):
  # rename column
  rename_dict = {'date': 'TimestampUTC', 'ric': 'RIC', 'title': 'Headline'}
  df_total.rename(columns=rename_dict, inplace=True)

  # select columns
  out_total = df_total[['RIC', 'Market', 'TimestampUTC', 'Headline']]

  # # filter noise news
  # real_news_rows_id = df_total['Headline'].apply(title_filter)
  # out_total = out_total[real_news_rows_id]


def filter_double_per(x):
  return x.count('.') < 2


if __name__ == "__main__":
  mkt_list = [
      'ASX_News', 'NAQ_News', 'NMQ_News', 'NSQ_News', 'SES_News', 'LSE_News'
  ]

  # be_date = '2018-01-31'
  # en_date = '2018-05-04'
  be_date = '2018-08-01'
  en_date = '2018-08-31'
  df_total, missing_ticker_dict = concat_mg_col(mkt_list, be_date, en_date)

  out_total = out_total[out_total['RIC'].apply(filter_double_per)]

  out_total.to_csv(
      'result_%s_%s_%s.csv' % ('_'.join(mkt_list), be_date, en_date),
      index=False)
