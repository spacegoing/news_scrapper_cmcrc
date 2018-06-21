# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient

class ReutersPipeline(object):

    def open_spider(self, spider):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['reuters_6_news']
        mkt_list = ['asx','sgx','johannesburg','istanbul','sao_paulo', 'lse', 'nasdaq']
        self.col_dict = {k:self.db[k] for k in mkt_list}

    def process_item(self, item, spider):
        if item:
            mkt = item['mkt']
            self.col_dict[mkt].insert_many(item['news_list'])
        return item

    def close_spider(self, spider):
        self.client.close()
