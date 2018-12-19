# -*- coding: utf-8 -*-
import os
import pickle
import pandas as pd
from gen_ulti_csv import filter_not_in_dailystats

# f_list = os.listdir('news')
f_list = [
    'result_asx_sgx_johannesburg_istanbul_sao_paulo_lse_nasdaq_2018-05-04_2018-05-31.csv',
    'result_sgx_20180630_20180801.csv',
    'result_asx_johannesburg_istanbul_sao_paulo_lse_nasdaq_20180630_20180801.csv',
    'result_asx_lse_nasdaq_sgx_sao_paulo_2018-10-01_2018-10-31.csv',
    'result_asx_lse_nasdaq_sgx_sao_paulo_2018-09-01_2018-09-30.csv',
    'result_asx_sgx_johannesburg_istanbul_sao_paulo_lse_nasdaq_2018-06-01_2018-06-30.csv',
    'result_asx_lse_nasdaq_sgx_sao_paulo_2018-08-01_2018-08-31.csv',
    'result_lse_20180630_20180801.csv'
]
date_list = [('2018-05-04', '2018-05-31'), ('2018-06-30', '2018-08-01'),
             ('2018-06-30', '2018-08-01'), ('2018-10-01', '2018-10-31'),
             ('2018-09-01', '2018-09-30'), ('2018-06-01', '2018-06-30'),
             ('2018-08-01', '2018-08-31'), ('2018-06-30', '2018-08-01')]
mapdir = '/home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/reuters/sao_map_dict.pickle'
with open(mapdir, 'rb') as f:
  sao_map_dict = pickle.load(f)


def map_sao_ric(x):
  if x.endswith('.SA'):  # only sao_paulo
    return sao_map_dict.get(x)
  else:
    return x


for f, (be_date, en_date) in zip(f_list, date_list):
  df = pd.read_csv('news/' + f, index_col=None)
  df = pd.read_csv('news/' + f, index_col=None)
  df['RIC'] = df['RIC'].apply(map_sao_ric)
  df = df.dropna()
  tdf = df.loc[df['Market'].apply(
      lambda x: x in ['sao_paulo', 'nasdaq', 'asx', 'sgx'])]
  tdf = filter_not_in_dailystats(tdf, be_date, en_date)
  df.to_csv('news/' + f, index=False)
