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

def gen_csv(df_list, fn):
  df_total = pd.concat(df_list)
  df_total.rename(
      columns={
          'date': 'TimestampUTC',
          'ticker': 'RIC',
          'title': 'Headline'
      },
      inplace=True)

  df_total.to_csv(fn+'.csv', index=False)

gen_csv(news_df_list[:3],'nasdaq')
gen_csv([news_df_list[3]],'asx')
gen_csv([news_df_list[4]],'sgx')
gen_csv([news_df_list[5]],'lse')
