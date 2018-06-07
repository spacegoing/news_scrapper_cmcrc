# -*- coding: utf-8 -*-
import os
import subprocess


def unzip_files():
  market_strs = [
      'johannesburg', 'sgx', 'lse', 'sao_paulo', 'nasdaq', 'istanbul', 'asx'
  ]

  for m in market_strs:
    date_market_dirs = os.listdir(
        "/home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/s3_data/%s" % m)
    for d in date_market_dirs:
      cur_dir = "/home/ubuntu/mqdCodeLab/news_scrapper_cmcrc/s3_data/%s/%s" % (
          m, d)
      f_list = os.listdir(cur_dir)
      f = cur_dir + '/' + f_list[0]
      subprocess.Popen("gunzip -k %s" % f, shell=True)
