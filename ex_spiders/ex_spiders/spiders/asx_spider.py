# -*- coding: utf-8 -*-
import re
import scrapy
import dateparser as dp


class ASXSpider(scrapy.Spider):
  name = 'asx_spider'
  web_url = "https://www.asx.com.au/asx/statistics/announcements.do?by=asxCode&asxCode=&timeframe=R&dateReleased=%s"
  root_url = "https://www.asx.com.au"
  is_init_flag = True  # Is First Run?
  time_fmt = '%d/%m/%Y'
  tzinfo = 'Australia/Sydney'
  pdfs_dir = 'PDFs/'

  pdf_mime_type = b'application/pdf'

  def start_requests(self):
    if self.is_init_flag:
      from init_setup import init_get_date
      date_list = init_get_date()
      date_list = [i.strftime('%d/%m/%Y') for i in date_list]
    else:
      date_list = []

    # debug:
    for i in date_list[:5]:
      yield scrapy.Request(self.web_url % i, callback=self.parse)

  def parse(self, response):
    tr_list = response.xpath("//tr")
    for tr in tr_list:
      td_list = tr.xpath('td')
      if td_list:
        ric = td_list[0].xpath('text()').extract()[0].strip()
        date_str = td_list[1].xpath('string()').extract()[0]
        date_time = dp.parse(
            date_str,
            settings={
                'TIMEZONE': self.tzinfo,
                'RETURN_AS_TIMEZONE_AWARE': True
            })
        price_sens = td_list[2].xpath('contains(@class,"pricesens")').extract()[
            0]
        price_sens = bool(int(price_sens))
        title = td_list[3].xpath('string(a)').extract()[0]
        title = self.filter_spaces(title)
        url = self.root_url + td_list[3].xpath('a/@href').extract()[0]
        item = {
            'ric': ric,
            'date_time': date_time,
            'tzinfo': self.tzinfo,
            'pricesens': price_sens,
            'title': title,
            'url': url
        }
        yield scrapy.Request(url, callback=self.save_item, meta={'item': item})

  def save_item(self, response):
    if response.headers['Content-Type'] == self.pdf_mime_type:
      filename = response.url.split('/')[-1]
      with open(self.pdfs_dir + filename, 'wb') as f:
        f.write(response.body)
      response.meta['item']['filename'] = filename
      yield response.meta['item']
    else:
      # from scrapy.shell import inspect_response
      # inspect_response(response, self)
      pdf_url = response.xpath('//input[@name="pdfURL"]/@value').extract()[0]
      yield scrapy.FormRequest.from_response(
          response,
          formdata={'pdfURL': pdf_url},
          callback=self.save_item,
          meta=response.meta)

  def filter_spaces(self, string):
    '''
    filter out all white spaces (\r \t \n etc.)
    '''
    ftr = re.compile(r'[\S ]+')
    return ftr.findall(string.strip())[0]
