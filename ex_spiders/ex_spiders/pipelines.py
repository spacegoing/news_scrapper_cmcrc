# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient


class ASXPipeline(object):

  def open_spider(self, spider):
    self.client = MongoClient('mongodb://localhost:27017/')
    self.db = self.client['Announcements_Exchange']
    mkt_list = [
        'asx', 'sgx', 'johannesburg', 'istanbul', 'sao_paulo', 'lse', 'nasdaq',
        'error_urls'
    ]
    self.col_dict = {k: self.db[k] for k in mkt_list}

  def process_item(self, item, spider):
    if item:
      # if item['error']:
      #   self.col_dict['error_urls'].insert_one(item['news_dict'])
      # else:
      # todo: change mkt
      mkt='asx'
      self.col_dict[mkt].insert_one(item)
    return item

  def close_spider(self, spider):
    self.client.close()


class ExSpidersPipeline(object):
    def process_item(self, item, spider):
        return item
