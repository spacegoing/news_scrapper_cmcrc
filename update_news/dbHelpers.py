# -*- coding: utf-8 -*-
import pymongo as mg
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')

news_db = client['stockopedia_news']
col_names = news_db.collection_names()
col_names_dict = {v.split('_')[0]: v for v in col_names}

urls_col = client['stockopedia']['global_urls']

latest_col = client['news_db_meta']['latest_news']

def update_latest_news_col():
    '''
    update latest news collection
    '''
    for u in urls_col.find():

        mkt = u['mkt']
        ticker = u['ticker']
        name = u['name']

        latest_news = list(news_db[col_names_dict.get(mkt, 'no_market')].find({
            'ticker':
            ticker,
            'comp_name':
            name
        }).sort("date", mg.DESCENDING).limit(1))[0:1]

        if latest_news:
            latest_news[0].pop('_id',None)
            latest_news[0]['mkt'] = mkt
            latest_col.replace_one({
                'ticker': ticker,
                'comp_name': name
            }, latest_news[0], True)
