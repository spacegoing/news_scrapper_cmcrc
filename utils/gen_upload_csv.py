# -*- coding: utf-8 -*-
import pandas as pd
import glob
from filter_rics import a
be_date='20180630'
en_date='20180801'
path = r'/home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/utils/csv_to_up/'                     # use your path
all_files = glob.glob(os.path.join(path, "*.csv"))     # advisable to use os.path.join as this makes concatenation OS independent
df_from_each_file = (pd.read_csv(f, index_col=None) for f in all_files)
concatenated_df   = pd.concat(df_from_each_file, ignore_index=True)


a=set(a)
concatenated_df = concatenated_df[concatenated_df['RIC'].apply(lambda x: x not in a)]

concatenated_df.to_csv(path+'%s_%s.csv'%(be_date, en_date), index=False)
