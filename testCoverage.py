# -*- coding: utf-8 -*-
import pandas as pd
import re

url_list = pd.read_csv(
    'asx_11_07_17.csv', header=None, index_col=None).as_matrix()[:, 3]

ric_set = set()
for u in url_list:
    colon_pos = [i for i, c in enumerate(u) if c == ":"]
    slash_pos = [i for i, c in enumerate(u) if c == "/"]
    ric = u[colon_pos[-1] + 1:slash_pos[-1]]
    ric_set.add(ric)

trth_raw = pd.read_csv('./raw_data/metric-daily_info-asx-asx-2017-11-07.csv', header=None, index_col=None)

trth_rics_list = trth_raw.as_matrix()[1:,4]
trth_ric_set = set()
for r in trth_rics_list:
    colon_pos = r.find(':')
    trth_ric_set.add(r[colon_pos+1:])

not_in_stockopedia = trth_ric_set - ric_set
not_in_trth = ric_set - trth_ric_set

intersect = trth_ric_set.intersection(ric_set)

nis_news_list = []
for t in not_in_stockopedia:
    for r in trth_raw.as_matrix():
        if t in r[4]:
            nis_news_list.append(r)

pd.DataFrame(nis_news_list).to_csv('nis_news_list.csv', header=False, index=False)
