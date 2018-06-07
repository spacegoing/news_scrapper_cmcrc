# -*- coding: utf-8 -*-
import os

import pandas as pd
from pymongo import MongoClient
import dateparser as dp

client = MongoClient('mongodb://localhost:27017/')
mqd_db = client['mqd_daily_info']

market_strs = [
    'johannesburg', 'sgx', 'lse', 'sao_paulo', 'nasdaq', 'istanbul', 'asx'
]
pwd = "/home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/s3_data/"

def parse_ric(x):
  return x.split(':')[-1]

for m in market_strs:
  date_market_dirs = os.listdir(pwd + ("%s" % m))
  print(m)
  for d in date_market_dirs:
    cur_dir = pwd + ("%s/%s" % (m, d))
    f_list = [i for i in os.listdir(cur_dir) if i.endswith(".csv")]
    f = cur_dir + '/' + f_list[0]

    info_col = mqd_db[m]
    news_df = pd.read_csv(f, index_col=None)
    if not news_df.empty:
      news_df['date'] = news_df['date'].apply(dp.parse)
      news_df['ric'] = news_df['listing'].apply(parse_ric)
      info_col.insert_many(news_df.to_dict(orient='records'))

