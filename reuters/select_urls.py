# -*- coding: utf-8 -*-
import pandas as pd

df = pd.read_csv('final_mqd_nodata.csv.old', header=None, index_col=None)
df = df[(df[0] == 'johannesburg') | (df[0] == 'sao_paulo')]
df.to_csv('final_mqd_nodata.csv', index=False,header=False)
