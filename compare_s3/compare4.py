from pymongo import MongoClient
import dateparser as dp
import pandas as pd

client = MongoClient('mongodb://localhost:27017/')
mqd_agg_db = client['mqd_daily_info_agg']
opedia_agg_db = client['stockopedia_news_agg']
opedia_news_db = client['stockopedia_news']

# dbname in mqd_daily_info : dbname in stockopedia_news
mqd_opedia_dict = {
    "asx": ["ASX_News"],
    "sgx": ["SES_News"],
    "istanbul": ["IST_News"],
    'lse': ["LSE_News"],
    'nasdaq': ["NAQ_News", "NMQ_News", "NSQ_News"]
}

for mcol, scol_list in mqd_opedia_dict.items():
  scol = scol_list[0]

  mqd_col = mqd_agg_db[mcol]
  sto_col = opedia_agg_db[scol]

  date_mnum_snum_covrate = [[
      'date', 'daily_info_sec', 'scrapper_sec', 'coverage'
  ]]
  for i in mqd_col.find():
    mdate = i['datestr']
    mlist = i['ticker_list']
    b = sto_col.find_one({"datestr": {"$eq": mdate}})

    if b:
      slist = b['ticker_list']
      date_mnum_snum_covrate.append([
          mdate, i['num_news'], b['num_news'],
          len(set(mlist).intersection(slist)) / i['num_news']
      ])

  pd.DataFrame(date_mnum_snum_covrate).to_csv(
      mcol + '.csv', header=False, index=False)


def mkt_date_secs(mcol, mdate):
  """
  print secs for
  mcol : uptick name for market
  mdate: date

  lse istanbul isin
  nasdaq mkts
  sgx ticker ric
  asx
  """
  scol = mqd_opedia_dict[mcol][0]

  mqd_col = mqd_agg_db[mcol]
  sto_col = opedia_agg_db[scol]

  i = mqd_col.find_one({"datestr": {"$eq": mdate}})
  b = sto_col.find_one({"datestr": {"$eq": mdate}})

  slist = b['ticker_list']
  mlist = i['ticker_list']

  print(slist)
  print(mlist)
