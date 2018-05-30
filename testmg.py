# -*- coding: utf-8 -*-
from pymongo import MongoClient
import dateparser as dp
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
db = client['stockopedia']
title_col = db['asx_title']

start = dp.parse("2017-11-07")
end = dp.parse('2017-11-08')
res = title_col.find({'date': {'$gte':start,'$lt':end}})

res_df = pd.DataFrame(list(res))
res_df.to_csv('asx_11_07_17.csv', header=False, index=False)
