# -*- coding: utf-8 -*-
import scrapy
import pandas as pd

class LoginSpider(scrapy.Spider):
    name = 'stock_url'
    website_url = 'https://www.stockopedia.com/'
    share_prices_page_url = 'https://www.stockopedia.com/share-prices/?page=%d'
    start_page_idx = 1
    end_page_idx = start_page_idx

    def start_requests(self):
        yield scrapy.Request('https://www.stockopedia.com/auth/login/',callback=self.login)

    def login(self,response):
        yield scrapy.FormRequest.from_response(
            response,
            formdata={'username': 'spacebnbk',
                      'password': 'qwertyuio',
                      'remember': 'on',
                      'redirect': 'auth%2Flogout'
            },
            callback=self.after_login
        )

    def after_login(self, response):
        # check login succeed before going on
        if b"incorrect" in response.body:
            self.logger.error("Login failed")
            return
        init_url = self.share_prices_page_url % self.start_page_idx
        yield scrapy.Request(init_url, callback=self.parse_region)

    def parse_region(self, response):
        # from scrapy.shell import inspect_response
        # inspect_response(response, self)
        region_list = response.xpath('//ul[@class="dropdown-menu"]/li[@class="opener"]')
        region_country_url_list = []
        for r in region_list:
            region_str = r.xpath('a/text()').extract()[0].strip()
            for c in r.xpath('ul/li')[1:]: # [0] is "all region"
                url = c.xpath('a/@href').extract()[0]
                country_tmp_list = c.xpath('a/text()').extract()
                # there might be multiple whitespaces lines. filter out those:
                country_str = [i.strip() for i in country_tmp_list if i.strip()][0]
                region_country_url_list.append([region_str, country_str, url])

        for r in region_country_url_list:
            yield scrapy.Request(r[2], callback=self.parse_first_page, meta={
                'region': r[0],
                'country': r[1],
                'url_template': r[2].replace('page=1','page=%d')
            })


    def parse_first_page(self, response):
        """
        Do some initializtions before parsing:
        1. set up end_page_idx
        """
        end_pg_idx = -2 # by inspection of webpage. -1 is "Next"

        # get pagination list
        pg_li_list = response.xpath('//div[@class="pagination"][1]/li')
        end_pg_li = pg_li_list[end_pg_idx]
        self.end_page_idx = int(end_pg_li.xpath('a/text()').extract()[0])

        # visit next pages
        shares_pages = [response.meta['url_template'] % i for i in range(self.start_page_idx, self.end_page_idx+1)]

        for i,url in enumerate(shares_pages):
            yield scrapy.Request(url, callback=self.parse_page,
                                 dont_filter=True, meta=response.meta)

    def parse_page(self, response):
        self.logger.info(response.url)
        tr_list = response.xpath('//table/tbody/tr')
        nation_name_mkt_href_ticker_list = []
        for i in tr_list:
            name = i.xpath('td[2]/a/text()').extract()[0].strip()
            mkt = i.xpath('td[2]/span/text()').extract()[0].strip().strip('(').strip(')')
            href = self.website_url + i.xpath('td[2]/a/@href').extract()[0].strip()
            ticker = i.xpath('td[3]/text()').extract()[0].strip()
            nation_name_mkt_href_ticker_list.append({
                'nation': response.meta['country'],
                "name": name,
                "mkt": mkt,
                "href": href,
                "ticker": ticker,
                "region": response.meta['region'],
                'page_url': response.url
            })

        return {'nation_name_mkt_href_ticker_list': nation_name_mkt_href_ticker_list}

