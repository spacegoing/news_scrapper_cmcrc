from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
mqd_db = client['mqd_daily_info']
opedia_agg_db = client['stockopedia_news_agg']

mqd_uptick_name_list = [
    'johannesburg', 'sgx', 'lse', 'sao_paulo', 'nasdaq', 'istanbul', 'asx'
]

# dbname in mqd_daily_info : dbname in stockopedia_news
mqd_opedia_dict = {
    "asx": ["ASX_News"],
    "sgx": ["SES_News"],
    "istanbul": ["IST_News"],
    'lse': ["LSE_News"],
    'nasdaq': ["NAQ_News", "NMQ_News", "NSQ_News"]
}

