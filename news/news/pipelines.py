# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient


class NewsPipeline(object):
  webpage = 'https://www.stockopedia.com/'

  def open_spider(self, spider):
    self.client = MongoClient('mongodb://localhost:27017/')
    self.db = self.client['stockopedia_news']
    spider.url_db = self.client['stockopedia']

  def process_item(self, item, spider):
    date_time = item['date_time']
    news_content = item['news_content']
    tzinfo = item['tzinfo']
    comp_name = item['meta']['name']
    comp_url = item['meta']['comp_url']
    ticker = item['meta']['ticker']
    date = item['meta']['date']
    title = item['meta']['title']
    news_url = item['meta']['news_url']
    record = {
        'comp_name': comp_name,
        'comp_url': comp_url,
        'date': date,
        'title': title,
        'news_url': news_url,
        'ticker': ticker,
        'date_time': date_time,
        'tzinfo': tzinfo,
        'news_content': news_content
    }

    comp_mkt = item['meta']['mkt']
    self.title_col = self.db['%s_News' % comp_mkt]
    if record:
      self.title_col.insert(record)
    return item

  def close_spider(self, spider):
    self.client.close()
