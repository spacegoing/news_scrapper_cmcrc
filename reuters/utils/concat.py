import pandas as pd
files_str = [
    "result_johannesburg_sao_paulo_2018-07-31_2018-09-01.csv",
    "lse_2018-07-31_2018-09-01.csv", "sgx_2018-07-31_2018-09-01.csv",
    "asx_2018-07-31_2018-09-01.csv", "nasdaq_2018-07-31_2018-09-01.csv"
]

df_col = []
for i in files_str:
  df_col.append(pd.read_csv(i, index_col=None))

be_date = '2018-07-31'
en_date = '2018-09-01'
total_df = pd.concat(df_col)
total_df.to_csv(
    'result_asx_sgx_johannesburg_sao_paulo_lse_nasdaq_%s_%s.csv' %
    (be_date, en_date),
    index=False)
