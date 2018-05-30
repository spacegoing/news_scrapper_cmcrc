# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
import dateparser as dp


class NewsPipeline(object):
    webpage = 'https://www.stockopedia.com/'

    def open_spider(self, spider):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['stockopedia_news']
        spider.url_db = self.client['stockopedia']

    def process_item(self, item, spider):
        news_list = item['news_list']
        comp_name = item['meta']['name']
        comp_url = item['meta']['url']
        ticker = item['meta']['ticker']

        bulk_list = []
        for n in news_list:
            date = dp.parse(n[0])
            title = n[1]
            news_url = self.webpage + n[2]
            record = {
                'comp_name':comp_name,
                'comp_url':comp_url,
                'date':date,
                'title':title,
                'news_url':news_url,
                'ticker': ticker
            }
            bulk_list.append(record)

        comp_mkt = item['meta']['mkt']
        self.title_col = self.db['%s_News' % comp_mkt]
        if bulk_list:
            self.title_col.insert_many(bulk_list)
        return item

    def close_spider(self, spider):
        self.client.close()
