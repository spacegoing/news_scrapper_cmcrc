# -*- coding: utf-8 -*-
import re
import scrapy
import dateparser as dp


class LSESpider(scrapy.Spider):
  name = 'lse_spider'
  web_url = "https://www.londonstockexchange.com/exchange/news/market-news/market-news-home.html?nameCodeText=&searchType=searchForNameCode&nameCode=&text=&rnsSubmitButton=Search&activatedFilters=true&newsSource=ALL&mostRead=&headlineCode=ONLY_EARNINGS_NEWS&headlineId=&ftseIndex=ASX&sectorCode=&rbDate=range&preDate=customizable&from=%s&to=%s&newsPerPage=500"
  root_url = "https://www.londonstockexchange.com"
  is_init_flag = True  # Is First Run?
  time_fmt = '%d/%m/%Y'
  tzinfo = 'Europe/London'

  # pdfs_dir = 'PDFs/'

  # pdf_mime_type = b'application/pdf'

  def start_requests(self):
    if self.is_init_flag:
      from init_setup import init_get_date
      date_list = init_get_date()
      date_list = [i.strftime('%d/%m/%Y') for i in date_list]
    else:
      date_list = []

    # todo: latest date
    for i in date_list[:365 * 2]:
      yield scrapy.Request(self.web_url % (i, i), callback=self.parse)

  def parse(self, response):
    # from scrapy.shell import inspect_response
    # inspect_response(response, self)

    td_list = response.xpath('//td[@class="RNS_data"]')
    for td in td_list:
      tr = td.xpath('.//tr[@class="firstRowRns"]')
      comp_name_ric = tr.xpath('.//span/text()').extract()
      comp_name = comp_name_ric[0].strip()
      ric = self.filter_ric(comp_name_ric[-1])
      title_str = tr.xpath('string(.//a)').extract()[0]
      title = self.filter_spaces(title_str)

      # parse url in javascript in @href
      url_str = tr.xpath('.//a/@href').extract()[0]
      be_url = url_str.find('/exchange')
      en_url = url_str.find('.html')
      url = ''
      if be_url and en_url:
        url = self.root_url + url_str[be_url:en_url + len('.html')]

      tr = td.xpath('.//tr[@class="secondRowRns"]')
      date_str = tr.xpath('.//span/text()').extract()[0]
      date_time = dp.parse(
          date_str,
          settings={
              'TIMEZONE': self.tzinfo,
              'RETURN_AS_TIMEZONE_AWARE': True
          })

      item = {
          'ric': ric,
          'date_time': date_time,
          'tzinfo': self.tzinfo,
          'title': title,
          'url': url,
          'comp_name': comp_name
      }
      # if url:
      #   yield scrapy.Request(url, callback=self.save_item, meta={'item': item})
      # else:
      yield item

  # def save_item(self, response):
  #   if response.headers['Content-Type'] == self.pdf_mime_type:
  #     filename = response.url.split('/')[-1]
  #     with open(self.pdfs_dir + filename, 'wb') as f:
  #       f.write(response.body)
  #     response.meta['item']['filename'] = filename
  #     yield response.meta['item']
  #   else:
  #     # from scrapy.shell import inspect_response
  #     # inspect_response(response, self)
  #     pdf_url = response.xpath('//input[@name="pdfURL"]/@value').extract()[0]
  #     yield scrapy.FormRequest.from_response(
  #         response,
  #         formdata={'pdfURL': pdf_url},
  #         callback=self.save_item,
  #         meta=response.meta)

  def filter_ric(self, string):
    '''
    filter out all white spaces (\r \t \n etc.)
    '''
    ftr = re.compile(r'[\w\d]+')
    return ftr.findall(string.strip())[0]

  def filter_spaces(self, string):
    '''
    filter out all white spaces (\r \t \n etc.)
    '''
    ftr = re.compile(r'[\S ]+')
    return ftr.findall(string.strip())[0]
