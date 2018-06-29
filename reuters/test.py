import time
import subprocess
import pandas as pd
import arrow


def filter_date():

  def dateRange(range_str):
    range_dict = {
        'all': 'all',
        'day': 'pastDay',
        'week': 'pastWeek',
        'month': 'pastMonth',
        'year': 'pastYear'
    }

    range_str = range_dict.get(range_str.lower(), 'all')

    def url_daterange_fn(string):
      return string.replace('dateRange=all', 'dateRange=%s' % range_str)

    return url_daterange_fn

  raw_df = pd.read_csv('final_mqd_nodata.csv')
  raw_df.columns = ['mkt', 'ric', 'compname', 'kw', 'url1', 'url2']

  filter_fn = dateRange('day')
  raw_df['day'] = raw_df['url1'].apply(filter_fn)
  filter_fn = dateRange('week')
  raw_df['week'] = raw_df['url1'].apply(filter_fn)

  raw_mat = raw_df.as_matrix()


if __name__ == "__main__":
  old = arrow.now()
  while (True):
    time.sleep(6)
    subprocess.Popen('scrapy crawl reuters_spider', shell=True)
    print(arrow.now() - old)
