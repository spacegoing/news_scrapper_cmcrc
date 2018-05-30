# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from pymongo import MongoClient
import dateparser as dp


class NewsPipeline(object):
    """
    db.global_urls.aggregate([{$group:{ _id : {mkt: "$mkt"},count: {$sum:1}}}])
    db.asx_title.aggregate([{$group:{ _id : {mkt: "$comp_mkt"},count: {$sum:1}}}])
    """

    def open_spider(self, spider):
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['stockopedia']
        self.col = self.db['global_urls']

    def process_item(self, item, spider):
        if item:
            self.col.insert_many(item['nation_name_mkt_href_ticker_list'])
        return item

    def close_spider(self, spider):
        self.client.close()
