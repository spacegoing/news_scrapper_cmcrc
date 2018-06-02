# Installation #

## Dependencies ##

- anaconda 5.1.0
- scrapy 1.5.0
- mongodb v3.6.4
- pymongo 3.6.1
- dateparser-0.7.0

## Usage ##

### Stockopedia Account Info ###

- Update stockopedia account info `login()` method in every
  spider

### Start MongoDB ###

1. execute `mongod &` first
2. (optional) run `mongo` to start a mongo shell

### Run Spider ###

1. `cd [spider directory]`
2. `scrapy crawl [spider_name]`
   - `stock_url` for scraping all markets' all stocks' urls from
     dropdown list. Store data in db: `stockopedia` collection:
     `global_urls`
   - `news` for scraping global stocks' all historical news.
     Store data in db: `stockopedia_news` collection:
     `[ExchangeName_News]` (for example, all ASX news will be
     stored in collection `ASX_News`)
   
   - Example:
     - `cd /home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/news`
     - `scrapy crawl news`

## Inspecting Scraped Results ##

Execute `mongo` to start a mongo shell instance.

### Inspecting Global Market URLs ###

``` javascript
use stockopedia
// count how many records are stored in this collection
db.global_urls.count()
// show some results
db.global_urls.find()
```

### Inspecting Market News Titles ###

``` javascript
use stockopedia_news
// count how many ASX news are scraped
db.ASX_News.count()
// show some results
db.ASX_News.find()
```



