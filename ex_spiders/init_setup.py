# -*- coding: utf-8 -*-
import pandas as pd
import datetime
from pymongo import MongoClient

def init_get_date():
  date_list = pd.date_range('1/1/1998', datetime.datetime.today())
  return list(reversed(date_list))

# # Attention:
# # For first run, we crawl dates stored in 'Crawled_Dates' collection
# client = MongoClient('mongodb://localhost:27017/')
# db = client['ASX']
# col = db['Crawled_Dates']
# # col.insert_many(dates_df.to_dict(orient='record'))
