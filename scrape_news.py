import csv
import datetime
import pytz
import re
import sys
import urllib.request

from dateutil.tz import gettz
from pytz import utc
from dateutil import parser

from bs4 import BeautifulSoup

LOAD_RESULTS_BACK_TO = datetime.date(2017, 11, 1)
LOAD_RESULTS_BACK_UNTIL = datetime.date(2017, 11, 30)

PAGE_URL = 'https://www.reuters.com/search/news?sortBy=date&dateRange=pastYear&blob=%s'
MORE_RESULTS_URL = 'https://www.reuters.com/assets/searchArticleLoadMoreJson?blob=%s&bigOrSmall=big&articleWithBlog=true&sortBy=date&dateRange=pastYear&numResultsToShow=10&pn=%s&callback=addMoreNewsResults'

# Taken from https://stackoverflow.com/questions/1703546/parsing-date-time-string-with-timezone-abbreviated-name-in-python
def gen_tzinfos():
    for zone in pytz.common_timezones:
        try:
            tzdate = pytz.timezone(zone).localize(datetime.datetime.utcnow(), is_dst=None)
        except pytz.NonExistentTimeError:
            pass
        else:
            tzinfo = gettz(zone)

            if tzinfo:
                yield tzdate.tzname(), tzinfo
TZINFOS = dict(gen_tzinfos())


def parse_timestamp(timestamp_string):
    timestamp = parser.parse(timestamp_string, tzinfos=TZINFOS)
    return timestamp.astimezone(utc)


def get_more_results(ric, page_number):
    with urllib.request.urlopen(MORE_RESULTS_URL % (ric, page_number)) as response:
        html = response.read()
        js_text = str(BeautifulSoup(html, 'html.parser'))
        data = {}
        for line in js_text.split('\n'):
            line = line.strip()
            if line == '{':
                data = {}
            elif line == '}' or line == '},':
                if data:
                    yield data
            else:
                match = re.match(r'(headline|date): "(.*)",', line)
                if match:
                    data[match.group(1)] = match.group(2)


def scrape(ric):
    print('Processing %s' % ric)
    with urllib.request.urlopen(PAGE_URL % ric) as response:
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        for result_entry in soup.find_all('div', {'class': 'search-result-content'}):
            headline_entry = result_entry.find('h3', {'class': 'search-result-title'}).find('a')
            timestamp_entry = result_entry.find('h5', {'class': 'search-result-timestamp'})
            headline = headline_entry.text.strip()
            timestamp = timestamp_entry.text.strip()
            yield headline, timestamp
        # search additional pages if necessary
        timestamps = soup.find_all('h5', {'class': 'search-result-timestamp'})
        print('timestamps:', timestamps)
        if timestamps:
            last_timestamp = parse_timestamp(timestamps[-1].text.strip())
            page_number = 1
            print('last_timestamp:', last_timestamp.date(), 'LOAD_RESULTS_BACK_TO:', LOAD_RESULTS_BACK_TO, 'LOAD_RESULTS_BACK_UNTIL:', LOAD_RESULTS_BACK_UNTIL)
            while last_timestamp.date() >= LOAD_RESULTS_BACK_TO and last_timestamp.date() <= LOAD_RESULTS_BACK_UNTIL:
                page_number += 1
                print('Loading extra results from %s back to %s for %s, page %s' %
                      (last_timestamp.date(), LOAD_RESULTS_BACK_TO, ric, page_number))
                result_count = 0
                for extra_result in get_more_results(ric, page_number):
                    result_count += 1
                    yield extra_result['headline'], extra_result['date']
                    row_timestamp = parse_timestamp(extra_result['date'])
                    if row_timestamp < last_timestamp:
                        last_timestamp = row_timestamp
                if result_count == 0:
                    print('No extra results on page number %s' % page_number)
                    break


def main(argv):
    # read previous results
    processed = set()
    results = []
    print ('argv:', argv)
    try:
        with open(argv[1]) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                results.append({
                    'ric': row['ric'],
                    'market': row['market'],
                    'Timestamp': row['Timestamp'],
                    'TimestampUTC': parse_timestamp(row['Timestamp']),
                    'Headline': row['Headline'],
                })
                processed.add((row['ric'], row['market']))
    except FileNotFoundError:
        pass

    # scrape new results from input file passed as first argument
    try:
        with open(argv[0]) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                key = (row['ric'], row['market'])
                if key not in processed:
                    for headline, timestamp in scrape(row['ric']):
                        results.append({
                            'ric': row['ric'],
                            'market': row['market'],
                            'Timestamp': timestamp,
                            'TimestampUTC': parse_timestamp(timestamp),
                            'Headline': headline,
                        })
                    processed.add(key)
    finally:
        # write out results
        with open(argv[1], 'w') as fh:
            writer = csv.DictWriter(fh, fieldnames=['ric', 'market', 'Timestamp', 'TimestampUTC', 'Headline'])
            writer.writeheader()
            for row in results:
                # only save results for completed ric requests, incomplete ones must be retried from the beginning
                if (row['ric'], row['market']) in processed:
                    writer.writerow(row)

if __name__ == "__main__":
    main(sys.argv[1:])
