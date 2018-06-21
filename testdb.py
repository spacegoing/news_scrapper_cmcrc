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

    ref_cur.execute("select * from refdata_tradablesymbolmap where stream_name = '%s'" % mkt)
    map_list = ref_cur.fetchall()
    map_df = pd.DataFrame(map_list, columns=['id', 'market', 'tradable', 'date', 'mkt', 'ric_suffix'])
    map_df['isin'] = map_df['tradable'].apply(parse_isin)
    map_df['ric'] = map_df['ric_suffix'].apply(parse_ric)
    map_df['date'] = map_df['date'].apply(parse_date)
    map_dict = map_df.to_dict('records')

    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017/')
    mqd_agg_db = client['mqd_refdata']
    col = mqd_agg_db['isin_map']
    col.insert_many(map_dict)



def get_mkt_info(mkt_uptick_name_list, cur):
    """
    Test market id query
    mkt_uptick_name_list = ['asx','sgx','johannesburg','istanbul','sao_paulo', 'lse', 'nasdaq']
    asx ASX
    sgx SES
    istanbul IST
    lse LSE
    nasdaq NAQ NMQ NSQ (NAS NMS NSM)
    """
    mkt_info_query = "select * from api_market a where a.uptick_name = ANY(%s);"
    cur.execute(mkt_info_query, [mkt_uptick_name_list])
    mkt_info_list = cur.fetchall()
    return mkt_info_list


def get_mkt_id(mkt_info_list):
    mkt_id_list = [i[0] for i in mkt_info_list]
    return mkt_id_list


def get_mkt_uptick_name(mkt_info_list):
    mkt_uptickname_list = [i[-1] for i in mkt_info_list]
    return mkt_uptickname_list


def get_mkt_sec_cur(mkt_id_list, be_date, en_date, cur):
    """
    mkt_id_list = [1, 3, 33, 38, 208]
    be_date = '2017-11-07'
    en_date = '2017-11-07'

    Test Query
    mkt_sec_cur_query = "select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds join api_tradable t on (ds.tradable_id = t.id) join api_market m on (ds.market_id = m.id) where ds.market_id in (1, 3, 33, 38, 208) and ds.date between '2017-11-07' and '2017-11-07' limit 10;"
    cur.execute(mkt_sec_cur_query)

    cur.mogrify(mkt_sec_cur_query, [mkt_id_list, be_date, en_date])
    """
    mkt_sec_cur_query = "select distinct m.uptick_name, ds.security, t.currency_id from api_dailystats ds join api_tradable t on (ds.tradable_id = t.id) join api_market m on (ds.market_id = m.id) where ds.market_id = ANY(%s) and ds.date between %s and %s;"
    cur.execute(mkt_sec_cur_query, [mkt_id_list, be_date, en_date])
    mkt_sec_cur_list = cur.fetchall()

    return mkt_sec_cur_list


def get_tradable_list(mkt_sec_cur_list):
    tradable_list = list({":".join(i) for i in mkt_sec_cur_list})
    return tradable_list


def get_mkt_rics(mkt_uptick_name_list, tradable_list, be_date, en_date, cur):
    """

    Test Query
    mkt_rics_query = "select distinct trading_market as Market, symbol as RIC from refdata_tradablesymbolmap where trading_market in ('asx', 'sgx', 'istanbul', 'johannesburg', 'sao_paulo') and tradable in ('sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD', 'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL', 'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD', 'asx:BEL:AUD', 'sgx:MDRT:SGD') and date between '2018-04-06' and '2018-05-03' limit 10;"
    tradable_list = ['sgx:RESH:SGD', 'sao_paulo:NVHO11:BRL', 'asx:BSX:AUD', 'johannesburg:SOHJ:ZAC', 'sgx:TECK:SGD', 'sao_paulo:DWDP34:BRL', 'sao_paulo:MSCD34:BRL', 'asx:AYI:AUD', 'asx:PMY:AUD', 'asx:BEL:AUD', 'sgx:MDRT:SGD']
    """
    mkt_rics_query = "select distinct trading_market as Market, symbol as RIC from refdata_tradablesymbolmap where trading_market = ANY(%s) and tradable = ANY(%s) and date between %s and %s;"
    cur.execute(mkt_rics_query,
                [mkt_uptick_name_list, tradable_list, be_date, en_date])
    mkt_rics_list = ref_cur.fetchall()
    return mkt_rics_list


mkt_list = ['asx', 'sgx', 'johannesburg', 'istanbul', 'sao_paulo', 'nasdaq']
be_date = '2017-11-07'
en_date = '2017-11-07'
mkt_info_list = get_mkt_info(mkt_list, mkt_cur)
mkt_id_list = get_mkt_id(mkt_info_list)
mkt_uptick_name_list = get_mkt_uptick_name(mkt_info_list)
mkt_sec_cur_list = get_mkt_sec_cur(mkt_id_list, be_date, en_date, mkt_cur)
tradable_list = get_tradable_list(mkt_sec_cur_list)
mkt_rics_list = get_mkt_rics(mkt_uptick_name_list, tradable_list, be_date,
                             en_date, ref_cur)

# save rics list to csv file
mkt_rics_df = pd.DataFrame(mkt_rics_list)
asx_rics = mkt_rics_df.groupby(0).get_group('asx')
asx_rics.to_csv('rics_list.csv', columns=[1], header=False, index=False)

# Debug missing tradables in refdata database (not_rics)
# / Missing rics (not_mkt) in mqdashboard database

# mkt_sec_cur_df = pd.DataFrame(mkt_sec_cur_list)
# mkt_rics_df = pd.DataFrame(mkt_rics_list)
# mkt_sec_cur_df.to_csv("mkt_sec_cur.csv")
# mkt_rics_df.to_csv("mkt_rics.csv")

# not_rics = list()
# for i in mkt_sec_cur_list:
#     flag = False
#     for j in mkt_rics_list:
#         if i[1] in j[1]:
#             flag = True

#     if not flag:
#         not_rics.append(i)

# not_mkt = list()
# for i in mkt_rics_list:
#     flag = False
#     for j in mkt_sec_cur_list:
#         if j[1] in i[1]:
#             flag = True

#     if not flag:
#         not_mkt.append(i)

# t = list({':'.join(i) for i in mkt_rics_list})

mkt_cur.close()
mkt_conn.close()
