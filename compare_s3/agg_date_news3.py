# -*- coding: utf-8 -*-
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
opedia_db = client['stockopedia_news']
opedia_agg_db = client['stockopedia_news_agg']


def insert_date_tickers_count(odb_name):
  '''
  query stockopedia_news.Market_news collection
  aggregate news by date
  list all tickers have news on that date

  insert date, list of tickers, num of tickers in
  stockopedia_news_agg.Market_News collection
  '''
  ocol = opedia_db[odb_name]
  agg_col = opedia_agg_db[odb_name]
  agg_col.drop()

  agg_pipeline = [{
      "$project": {
          "datestr": {
              "$dateToString": {
                  "format": "%Y-%m-%d",
                  "date": "$date"
              }
          },
          "ticker": 1,
      }
  }, {
      "$group": {
          "_id": {
              "datestr": "$datestr",
          },
          "ticker_list": {
              "$addToSet": "$ticker"
          }
      }
  }, {
      "$sort": {
          "_id": 1
      }
  }]
  agg_crs = ocol.aggregate(agg_pipeline, allowDiskUse=True)

  agg_col.insert_many(({
      'datestr': i['_id']['datestr'],
      "ticker_list": i['ticker_list'],
      "num_news": len(i['ticker_list'])
  } for i in agg_crs))


for i in opedia_db.collection_names():
  insert_date_tickers_count(i)
