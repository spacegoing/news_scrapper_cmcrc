import numpy as np
import pandas as pd
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup
import re
import requests

# mqd no data
# mkt_str=''

def generate_RIC_search_name_dict_using_input_file(input_file):
  df = pd.read_csv(input_file)
  # for db_utils.py csv
  df_ric_name = df[['mkt', '#RIC', 'Directory Company Name']]
  # for trth data
  # df_ric_name = df[['#RIC', 'Directory Company Name']]
  df_ric_name['Name_words'] = df_ric_name['Directory Company Name'].apply(
      lambda x: str(x).split(' '))

  #find the most frequent words
  word_dict = defaultdict(int)
  for word_list in df_ric_name['Name_words']:
    for word in word_list:
      word_dict[word] += 1
  word_dict = sorted(word_dict.items(), key=lambda item: item[1], reverse=True)
  high_freq_word = [pair[0] for pair in word_dict[:6]]
  del high_freq_word[0]

  #Generate search_word
  def generate_search_word(Name_words):
    if len(Name_words) <= 2:
      search_word = ' '.join(Name_words)
    elif len(Name_words) <= 4:
      if 'Investment' in Name_words:
        search_word = ' '.join(Name_words[:3])
      else:
        search_word = ' '.join(Name_words[:2])
    elif len(Name_words) > 4:
      for word in Name_words:
        if word in high_freq_word:
          search_word = ' '.join(Name_words[:3])
        else:
          search_word = ' '.join(Name_words[:2])
    return search_word

  df_ric_name['search_word'] = df_ric_name['Name_words'].apply(
      generate_search_word)

  # for trth_data
  # df_ric_name['mkt'] = mkt_str

  return df_ric_name[['mkt', '#RIC', 'search_word']]

def tmp_mqd_nodata():
  # mqd database without johannesburg, sao_paulo, sgx
  input_file = 'ric_cmpname.csv'
  asx_nas_lse_ist_df = generate_RIC_search_name_dict_using_input_file(input_file)
  input_file = 'sao_ric_name.csv'
  mkt_str = 'sao_paulo'
  sao_df = generate_RIC_search_name_dict_using_input_file(input_file)
  input_file = 'sgx_ric_name.csv'
  mkt_str = 'sgx'
  sgx_df = generate_RIC_search_name_dict_using_input_file(input_file)
  input_file = 'jse_ric_name.csv'
  mkt_str = 'johannesburg'
  jse_df = generate_RIC_search_name_dict_using_input_file(input_file)

  final_df = pd.concat([asx_nas_lse_ist_df, sao_df, sgx_df, jse_df])
  final_df['query_str'] = final_df['search_word'].apply(lambda x: '+'.join(x.split(' ')))
  final_df.to_csv('final_mqd_nodata.csv', index=False)

def obtain_news_by_RIC(RIC):

  #search by RIC
  print('Processing ' + RIC)
  locate_word = RIC
  news_title_time_pair = []
  news_titles = []
  news_timestamps = []
  news_timestamps_map = []

  target = 'https://www.reuters.com/search/news?sortBy=date&dateRange=all&blob=' + RIC
  page = requests.get(url=target)
  html = page.text
  soup = BeautifulSoup(html, features='html5lib')

  brief_contents = soup.find_all('div', {'class': "search-result-excerpt"})
  news_titles_map = soup.find_all('div', {'class': 'search-result-indiv'})

  for each in news_titles_map:
    title = each.find('h3', {'class': "search-result-title"}).find('a').text
    news_titles.append(title)
    timestamp = each.find('h5').text
    news_timestamps_map.append(timestamp)

  for i in range(len(brief_contents)):
    if re.search(locate_word, brief_contents[i].text):
      news_title_time_pair.append(news_titles[i] + '      ' +
                                  news_timestamps_map[i])

  #search by search_word
  search_word = RIC_name_dcit[RIC]
  print('Processing ' + search_word)
  words = search_word.split(' ')
  match_words = []
  search_word = '+'.join(list(search_word.split()))
  for word in words:
    match_words.append(word.capitalize())
  locate_word = ' '.join(match_words)

  news_title_time_pair_by_search_name = []
  news_titles = []
  news_timestamps = []
  news_timestamps_map = []

  target = 'https://www.reuters.com/search/news?sortBy=date&dateRange=all&blob=' + search_word
  page = requests.get(url=target)
  html = page.text
  soup = BeautifulSoup(html, features='html5lib')

  brief_contents = soup.find_all('div', {'class': "search-result-excerpt"})
  news_titles_map = soup.find_all('div', {'class': 'search-result-indiv'})

  for each in news_titles_map:
    title = each.find('a').text
    news_titles.append(title)
    timestamp = each.find('h5').text
    news_timestamps_map.append(timestamp)

  for i in range(len(brief_contents)):
    if re.search(locate_word, brief_contents[i].text):
      news_title_time_pair_by_search_name.append(news_titles[i] + '      ' +
                                                 news_timestamps_map[i])
    news_title_time_pair = list(
        set(news_title_time_pair + news_title_time_pair_by_search_name))
  return news_title_time_pair


RIC_name_dcit = generate_RIC_search_name_dict_using_input_file(
    'ric_cmpname.csv')
