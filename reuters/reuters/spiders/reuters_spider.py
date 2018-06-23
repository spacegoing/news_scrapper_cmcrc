# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
import dateparser as dp
import demjson as dj
from bs4 import BeautifulSoup as bs


class ReutersSpider(scrapy.Spider):
  name = 'reuters_spider'
  add_more_tmpl = "https://www.reuters.com/assets/searchArticleLoadMoreJson?blob=%s&bigOrSmall=big&articleWithBlog=true&sortBy=date&dateRange=all&numResultsToShow=10&pn=%d&callback=addMoreNewsResults"
  root_url = "https://www.reuters.com"

  def start_requests(self):
    raw_df = pd.read_csv('final_mqd_nodata.csv')
    raw_mat = raw_df.as_matrix()

    for i in raw_mat:
      meta = {
          'mkt': i[0],
          'ric': i[1],
          'comp_name': i[2],
          'name_query_str': i[3]
      }
      yield scrapy.Request(i[4], callback=self.ric_requests, meta=meta)
      yield scrapy.Request(i[5], callback=self.name_requests, meta=meta)
      # url = "https://www.reuters.com/search/news?sortBy=date&dateRange=all&blob=Pointerra+Ltd"
      # yield scrapy.Request(url, callback=self.name_requests, meta=meta)

  def ric_requests(self, response):
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)
    news_list = self.parse_web_page(response)
    if news_list:
      for request in self.parse_news_content(news_list, response.meta['mkt']):
        yield request

      init_pn = 2
      next_url = self.add_more_tmpl % (response.meta['ric'], init_pn)
      response.meta['pn'] = init_pn
      response.meta['next_keyword'] = response.meta['ric']
      yield scrapy.Request(
          next_url, meta=response.meta, callback=self.json_requests)

  def name_requests(self, response):
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)
    news_list = self.parse_web_page(response)
    if news_list:
      for request in self.parse_news_content(news_list, response.meta['mkt']):
        yield request

      init_pn = 2
      next_url = self.add_more_tmpl % (response.meta['name_query_str'], init_pn)
      response.meta['pn'] = init_pn
      response.meta['next_keyword'] = response.meta['name_query_str']
      yield scrapy.Request(
          next_url, meta=response.meta, callback=self.json_requests)

  def json_requests(self, response):
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)

    tmp_string = response.xpath('//text()').extract()

    if tmp_string:
      news_list = self.parse_json(tmp_string, response.meta)

      if news_list:
        for request in self.parse_news_content(news_list, response.meta['mkt']):
          yield request

        pn = response.meta['pn'] + 1
        response.meta['pn'] = pn
        next_url = self.add_more_tmpl % (response.meta['next_keyword'], pn)
        yield scrapy.Request(
            next_url, meta=response.meta, callback=self.json_requests)

  def parse_news_content(self, news_list, mkt):
    '''
    Inputs:
      news_list: list of dict contains: date, title, url, comp_name, ric
      mkt: string. meta['mkt']
    '''
    for n in news_list:
      yield scrapy.Request(
          n['url'],
          meta={
              'news_dict': n,
              'mkt': mkt
          },
          callback=self.news_content_requests,
          dont_filter=True)

  def news_content_requests(self, response):
    error = False
    news_content = ''
    try:
      news_content = response.xpath('//div[@class="body_1gnLA"]').extract()[0]
      news_content = '\n'.join(bs(news_content, 'lxml').stripped_strings)
    except IndexError:
      error = True
    news_dict = response.meta['news_dict']
    news_dict['news_content'] = news_content
    yield {'news_dict': news_dict, 'mkt': response.meta['mkt'], 'error': error}

  def parse_json(self, tmp_string, meta):
    '''

    return:
      news_list: list of dict contains: date, title, url, comp_name, ric
    '''
    tmp_str = ''
    for i in tmp_string:
      tmp_str += i
    tmp_string = tmp_str
    tmp_string = tmp_string.replace("\'", "'")
    tmp_string = tmp_string.replace("\r", "")
    tmp_string = tmp_string.replace("\n", "")
    tmp_string = tmp_string.replace("\t", "")

    # magic number: find by inspection
    json_string = tmp_string[71:-9]
    json_dict = dj.decode(json_string)

    news_list = json_dict['news']
    if news_list:
      news_df = pd.DataFrame(news_list)
      news_df['date'] = news_df['date'].apply(dp.parse)
      news_df['url'] = news_df['href'].apply(lambda x: self.root_url + x)
      news_df['title'] = news_df['headline']
      news_df = news_df[['date', 'url', 'title']]
      news_df['comp_name'] = meta['comp_name']
      news_df['ric'] = meta['ric']
      news_list = news_df.to_dict(orient='record')

    return news_list

  def parse_web_page(self, response):
    '''

    return:
      news_list: list of dict contains: date, title, url, comp_name, ric
    '''
    url_list = response.xpath('//h3/a/@href').extract()
    # using xpath string() function to avoid text() multi lines effect
    title_list = response.xpath('//h3')
    title_list = [a.xpath("string(a)").extract()[0] for a in title_list]
    date_list = response.xpath('//div[@class="search-result-content"]')
    date_list = [a.xpath("string(h5)").extract()[0] for a in date_list]

    news_list = []
    if url_list:
      tmp_df = pd.DataFrame({
          'url': url_list,
          'title': title_list,
          'date': date_list
      })
      tmp_df['date'] = tmp_df['date'].apply(dp.parse)
      tmp_df['url'] = tmp_df['url'].apply(lambda x: self.root_url + x)
      tmp_df['comp_name'] = response.meta['comp_name']
      tmp_df['ric'] = response.meta['ric']
      news_list = tmp_df.to_dict(orient='record')
    return news_list
