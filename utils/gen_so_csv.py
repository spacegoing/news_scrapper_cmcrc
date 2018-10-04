# -*- coding: utf-8 -*-
import pandas as pd
import dateparser as dp
from pymongo import MongoClient
from so_ticker_utils import pipelines_dict

client = MongoClient('mongodb://localhost:27017/')
db = client['stockopedia_news']

mkt_ric_isin_map = pd.read_csv('mkt_ric_isin_map.csv', index_col=None)
# dict: (mkt, ric_prefix): [isin, ric_suffix]
mkt_ricpre_isin_dict = {
    (i[0], i[1].split('.')[0]): [i[2], i[1]] for i in mkt_ric_isin_map.values
}


def get_news_df(uptick_name, col, be_date, en_date):
  news_list = list(
      col.find({
          "date": {
              "$gte": dp.parse(be_date),
              "$lte": dp.parse(en_date)
          }
      }))
  news_df = pd.DataFrame(news_list)
  news_df['Market'] = uptick_name
  return news_df


def get_mkt_news_df_dict(mqd_opedia_dict, be_date, en_date):
  mkt_news_df_dict = {}
  for k, v in mqd_opedia_dict.items():
    news_df_list = []
    for col_name in v:
      col = db[col_name]
      news_df = get_news_df(k, col, be_date, en_date)
      news_df_list.append(news_df)
    mkt_news_df_dict[k] = pd.concat(news_df_list)

  return mkt_news_df_dict


def recover_ric_suf(df):
  '''
  should be replaced by ticker2ric pipelines
  '''

  def recover(x):
    ric_str = None

    mkt = x['Market']
    ticker = x['ticker']
    isin_ric_list = mkt_ricpre_isin_dict.get((mkt, ticker), '')
    if isin_ric_list:
      ric_str = isin_ric_list[1]
    return ric_str

  df['RIC'] = df.apply(recover, axis=1)
  df.dropna(inplace=True)


def recover_isin(df):

  def recover(x):
    isin_str = None

    mkt = x['Market']
    ticker = x['ticker']
    # todo: temp use mkt_ricpre
    isin_ric_list = mkt_ricpre_isin_dict.get((mkt, ticker), '')
    if isin_ric_list:
      if len(isin_ric_list[0]) >= 10:
        isin_str = isin_ric_list[0]
    return isin_str

  df['ISIN'] = df.apply(recover, axis=1)
  df.dropna(inplace=True)


def filter_ric(df):

  def filter_double_per(x):
    return x.count('.') == 1

  df = df[df['RIC'].apply(filter_double_per)]


def filter_title(df):

  def title_filter(x):
    want = ['brief', 'update']
    flag_list = []
    for w in want:
      tmp_flag = False
      if w in x.lower():
        tmp_flag = True
      flag_list.append(tmp_flag)
    return any(flag_list)

  df = df[df['Headline'].apply(title_filter)]


def general_pipeline(df):
  # append suffix to ticker; should be replaced
  # with ticker2ric
  recover_ric_suf(df)

  filter_ric(df)
  # filter_title(df)

  # rename column
  rename_dict = {'date_time': 'TimestampUTC', 'title': 'Headline'}
  df.rename(columns=rename_dict, inplace=True)

  # select columns
  df = df[['RIC', 'Market', 'TimestampUTC', 'Headline']]
  return df


def sgx_pipeline(df):
  # append suffix to ticker; should be replaced
  # with ticker2ric
  not_found, not_match = pipelines_dict['sgx'](df)

  filter_ric(df)
  # filter_title(df)

  # rename column
  rename_dict = {'date_time': 'TimestampUTC', 'title': 'Headline'}
  df.rename(columns=rename_dict, inplace=True)

  # select columns
  df = df[['RIC', 'Market', 'TimestampUTC', 'Headline']]
  return df


def lse_pipeline(df):
  # append suffix to ticker; should be replaced
  # with ticker2ric
  # should be recovered to ticker first
  recover_isin(df)
  recover_ric_suf(df)

  # filter_title(df)

  # rename column
  rename_dict = {'date_time': 'TimestampUTC', 'title': 'Headline'}
  df.rename(columns=rename_dict, inplace=True)

  # select columns
  df = df[['RIC', 'Market', 'TimestampUTC', 'Headline', 'ISIN']]
  return df


def gen_csv(mqd_opedia_dict, be_date, en_date):
  mkt_news_df_dict = get_mkt_news_df_dict(mqd_opedia_dict, be_date, en_date)

  for k, v in mkt_news_df_dict.items():
    if k == 'lse':
      mkt_news_df_dict[k] = lse_pipeline(v)
    elif k == 'sgx':
      mkt_news_df_dict[k] = sgx_pipeline(v)
    else:
      mkt_news_df_dict[k] = general_pipeline(v)
    print(v.shape)

  total_df = pd.concat(mkt_news_df_dict.values(), sort=False)

  total_df.to_csv(
      'result_%s_%s_%s.csv' % ('_'.join(mkt_news_df_dict), be_date, en_date),
      index=False)

  # import pickle
  # with open('total.pickle', 'wb') as f:
  #   pickle.dump(mkt_news_df_dict, f)

  # with open('sgx.pickle', 'rb') as f:
  #   df = pickle.load(f)

  # for k, v in mkt_news_df_dict.items():
  #   print(v.shape)


if __name__ == "__main__":

  mqd_opedia_dict = {
      "asx": ("ASX_News",),
      'lse': ("LSE_News",),
      'nasdaq': ("NAQ_News", "NMQ_News", "NSQ_News"),
      "sgx": ("SES_News",)
      # "johannesburg": ("JSE_News",),
      # 'sao_paulo': ("SAO_News",),
  }
  be_date = '2018-09-01'
  en_date = '2018-09-30'

  gen_csv(mqd_opedia_dict, be_date, en_date)

  a= pd.read_csv('result_asx_lse_nasdaq_sgx_%s_%s.csv' % (be_date, en_date), index_col=None)
  b= pd.read_csv('result_sao_paulo_johannesburg_%s_%s.csv' % (be_date, en_date), index_col=None)
  c=pd.concat([a,b])
  c.to_csv(
      'result_asx_lse_nasdaq_sgx_sao_paulo_%s_%s.csv' % (be_date, en_date),
      index=False)

