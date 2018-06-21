# -*- coding: utf-8 -*-
import psycopg2 as pg
import pandas as pd

mkt_dbconfig = {
    'dbname': 'mqdashboard',
    'user': 'dbreader',
    'password': 'cmcrc2018!',
    'host': 'mqdashboarddb-metrics.czm2hxmcygx4.us-east-1.rds.amazonaws.com'
}
mkt_conn = pg.connect(**mkt_dbconfig)
mkt_cur = mkt_conn.cursor()

ref_dbconfig = {
    'dbname': 'refdata',
    'host': 'reference-data.czm2hxmcygx4.us-east-1.rds.amazonaws.com',
    'port': '5432',
    'user': 'reader',
    'password': 'refdatareader2017!'
}
ref_conn = pg.connect(**ref_dbconfig)
ref_cur = ref_conn.cursor()


def get_isin_ric_map(mkt):
  """
    read data from mqd aws sql
    write parsed data to mongodb mqd_refdata isin_map
  """

  def parse_isin(s):
    return s.split(':')[1]

  def parse_ric(s):
    return s.split('.')[0]

  def parse_date(d):
    import datetime
    return datetime.datetime.combine(d, datetime.time())

  ref_cur.execute(
      "select * from refdata_tradablesymbolmap where stream_name = '%s'" % mkt)
  map_list = ref_cur.fetchall()
  map_df = pd.DataFrame(
      map_list,
      columns=['id', 'market', 'tradable', 'date', 'mkt', 'ric_suffix'])
  map_df['isin'] = map_df['tradable'].apply(parse_isin)
  map_df['ric'] = map_df['ric_suffix'].apply(parse_ric)
  map_df['date'] = map_df['date'].apply(parse_date)
  map_dict = map_df.to_dict('records')

  from pymongo import MongoClient
  client = MongoClient('mongodb://localhost:27017/')
  mqd_agg_db = client['mqd_refdata']
  col = mqd_agg_db['isin_map']
  col.insert_many(map_dict)

get_isin_ric_map('IST')
get_isin_ric_map('LSE')
