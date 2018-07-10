import time
import subprocess
import arrow


if __name__ == "__main__":
  while (True):
    print('start %s' % arrow.now())
    subprocess.Popen('scrapy crawl reuters_spider', shell=True)
    time.sleep(60*60*24)
