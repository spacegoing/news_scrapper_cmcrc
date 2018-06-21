# -*- coding: utf-8 -*-
import scrapy
import pandas as pd


class ReutersSpider(scrapy.Spider):
  name = 'reuters_spider'

  def start_requests(self):
    raw_df = pd.read_csv('final_mqd_nodata.csv')
    raw_mat = raw_df.as_matrix()

    for i in raw_mat:
      meta = {'mkt': i[0], 'ric': i[1], 'comp_name': i[2]}
      yield scrapy.Request(i[4], callback=self.init_requests, meta=meta)
      yield scrapy.Request(i[5], callback=self.init_requests, meta=meta)

  def init_requests(self, response):
    from scrapy.shell import inspect_response
    inspect_response(response, self)

