# -*- coding: utf-8 -*-
import re
import scrapy
import dateparser as dp


class LoginSpider(scrapy.Spider):
  name = 'news'
  website_url = 'https://www.stockopedia.com/'

  def start_requests(self):
    yield scrapy.Request(
        'https://www.stockopedia.com/auth/login/', callback=self.login)

  def login(self, response):
    yield scrapy.FormRequest.from_response(
        response,
        formdata={
            'username': 'spacebnbk',
            'password': 'qwertyuio',
            'remember': 'on',
            'redirect': 'auth%2Flogout'
        },
        callback=self.after_login)

  def after_login(self, response):
    # check login succeed before going on
    if b"incorrect" in response.body:
      self.logger.error("Login failed")
      return

    # defined in pipelines.py NewsPipeline->open_spider()
    # url_list = self.url_db.global_urls.find()
    url_list = [{
        "nation":
            "Singapore",
        "name":
            "Samurai 2K Aerosol",
        "mkt":
            "SES",
        "href":
            "https://www.stockopedia.com/share-prices/samurai-2k-aerosol-SGX:1C3/",
        "ticker":
            "1C3",
        "region":
            "Asia",
        "page_url":
            "https://www.stockopedia.com/share-prices/?page=7&region=sg"
    }, {
        "nation": "Singapore",
        "name": "San Teh",
        "mkt": "SES",
        "href": "https://www.stockopedia.com/share-prices/san-teh-SGX:S46/",
        "ticker": "S46",
        "region": "Asia",
        "page_url": "https://www.stockopedia.com/share-prices/?page=7&region=sg"
    }, {
        "nation":
            "Singapore",
        "name":
            "Sanli Environmental",
        "mkt":
            "SES",
        "href":
            "https://www.stockopedia.com/share-prices/sanli-environmental-SGX:1E3/",
        "ticker":
            "1E3",
        "region":
            "Asia",
        "page_url":
            "https://www.stockopedia.com/share-prices/?page=7&region=sg"
    }, {
        "nation":
            "Singapore",
        "name":
            "Santak Holdings",
        "mkt":
            "SES",
        "href":
            "https://www.stockopedia.com/share-prices/santak-holdings-SGX:580/",
        "ticker":
            "580",
        "region":
            "Asia",
        "page_url":
            "https://www.stockopedia.com/share-prices/?page=7&region=sg"
    }, {
        "nation": "Singapore",
        "name": "Sapphire",
        "mkt": "SES",
        "href": "https://www.stockopedia.com/share-prices/sapphire-SGX:BRD/",
        "ticker": "BRD",
        "region": "Asia",
        "page_url": "https://www.stockopedia.com/share-prices/?page=7&region=sg"
    }]
    for i in url_list:
      yield scrapy.Request(
          i['href'] + 'news/',
          callback=self.parse_first_page,
          meta={
              'name': i['name'],
              'mkt': i['mkt'],
              'comp_url': i['href'],
              'ticker': i['ticker'],
              'region': i['region'],
              'nation': i['nation']
          },
          dont_filter=True)

  def parse_first_page(self, response):
    """
        Do some initializtions before parsing:
        1. set up end_page_idx
        """

    for request in self.parse_return(response):
      yield request

    end_pg_idx = -2  # by inspection of webpage. -1 is "Next"
    # get pagination list
    pg_li_list = response.xpath('//div[@class="pagination"][1]/li')
    if pg_li_list:
      end_pg_li = pg_li_list[end_pg_idx]
      end_page_idx = int(end_pg_li.xpath('a/text()').extract()[0].strip())
      # visit next pages
      # from scrapy.shell import inspect_response
      # inspect_response(response, self)
      news_pages = [
          response.url + '?page=%d' % i for i in range(1, end_page_idx + 1)
      ]

      # debug
      for url in news_pages:
        yield scrapy.Request(
            url, callback=self.parse, dont_filter=True, meta=response.meta)

  def parse(self, response):
    for request in self.parse_return(response):
      yield request

  def parse_return(self, response):
    '''
        Parameters:
        response.meta: {'name', 'mkt', 'comp_url', 'ticker', 'region', 'nation'}
    '''
    self.logger.info(response.url)
    tr_list = response.xpath('//table[@class="noborder"]/tr')

    for i in tr_list:
      td_list = i.xpath('td')
      response.meta['date'] = td_list[0].xpath('text()').extract()[0].strip()
      response.meta['title'] = td_list[1].xpath('a/text()').extract()[0].strip()
      response.meta['news_url'] = self.website_url + td_list[1].xpath(
          'a/@href').extract()[0].strip()
      yield scrapy.Request(
          response.meta['news_url'],
          callback=self.parse_content,
          dont_filter=True,
          meta=response.meta)

  def parse_content(self, response):
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)
    # parse date, time
    time_str_list = response.xpath(
        "//div[@class='s116 italic dim']/text()").extract()
    time_str = '\n'.join(time_str_list)
    time_str = self.filter_spaces(time_str)
    date_time = dp.parse(time_str)

    # parse content
    news_content = response.xpath('//div[@class="s116"]/pre/text()').extract()[
        0]

    if date_time and news_content:
      yield {
          'date_time': date_time,
          'news_content': news_content,
          'meta': response.meta
      }

  def filter_spaces(self, string):
    '''
    filter out all white spaces (\r \t \n etc.)
    '''
    ftr = re.compile(r'[\S ]')
    return ftr.findall(string.strip())[0]
