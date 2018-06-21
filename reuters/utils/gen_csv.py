# -*- coding: utf-8 -*-
import pandas as pd
import dateparser as dp
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['reuters_6_news']
mkt_list = [
    'asx', 'sgx', 'johannesburg', 'istanbul', 'sao_paulo', 'lse', 'nasdaq'
]
col_list = [db[k] for k in mkt_list]

news_df_list = []
for col in col_list:
  news_list = list(
      col.find({
          "date": {
              "$gt": dp.parse('2018-05-03'),
              "$lt": dp.parse('2018-06-01')
          }
      }))
  news_df = pd.DataFrame(news_list)
  news_df['Market'] = col.name
  news_df_list.append(news_df)

df_total = pd.concat(news_df_list)
df_total.rename(columns={'date': 'TimestampUTC', 'ric': 'RIC', 'title': 'Headline'}, inplace=True)

out_total = df_total[['RIC', 'Market', 'TimestampUTC', 'Headline']]
out_total.to_csv('result_asx_sgx_johannesburg_istanbul_sao_paulo_lse_nasdaq_2018-05-04_2018-05-31.csv', index=False)
