import requests
import time
import re
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

coins = ['Bitcoin','Ethereum', 'Ripple', 'BitcoinCash', '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 'Litecoin',
         'Stellar', 'Cardano', 'IOTA','0xf230b790e05390fc8295f4d3f60332c93bed42e2', 'Tether', '0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b.neo',
         'Dash', 'Monero', '0xb8c77482e45f1f44de1745f52c74426c631bdd52', 'NEM','0xd850942ef8811f2a866692a623011bde52a462c1',
         'EthereumClassic','Ontology','0xd26114cd6ee289accf82350c8d8487fedb8a0c07']


BC_MONGODB = "192.168.4.158"
PORT_MONGODB = 27017

connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf


fetch_twitters_amount = 10

out_file = "coins_with_tweets.csv"

def get_coin_code_name():
    d_code_name = {}
    for code in coins:
        token = db_bcf.tokens.find_one({"code": code})
        name = token.get("name")
        d_code_name.update({code: name})
    return d_code_name


def between_days(start_date, end_date, step):
    while start_date < end_date:
        yield start_date
        start_date += step


def get_twitters_by_requests(coin_code, timestamp):
    js_ts = int(timestamp * 1000)
    url = "https://api.acnoncw.cn/api/v1/message/twitter/{}".format(coin_code)
    params =  {"lang":"cn", "time": js_ts, "limit": fetch_twitters_amount}
    # print(requests.get(url, params=params).url)
    return requests.get(url, params=params).json()


def sort_by_day(start_date):
    if type(start_date) != tuple :
        raise ValueError("The format of start_time should be (year, month, day), like (2018, 1, 1).")
    if len(start_date) != 3:
        raise ValueError("The format of start_time should be (year, month, day), like (2018, 1, 1).")

    start_date = datetime(start_date[0], start_date[1], start_date[2], tzinfo=timezone.utc)
    now  = datetime.now()
    now = now.replace(tzinfo=timezone.utc)

    d_code_name = get_coin_code_name()

    with open(out_file, 'at', encoding='utf-8') as f:

        for day in between_days(start_date, now, timedelta(days=1)):
            print("#" + day.strftime("%Y-%m-%d"))
            f.write(day.strftime("%Y-%m-%d"))
            f.write('\n')


            until_timestamp = trans_datetime_to_timestamp(day + timedelta(days=1))

            for coin_code in coins:
                coin_name = d_code_name.get(coin_code)
                print(coin_name)
                l_coin_tweets = []
                tweets = get_twitters_by_requests(coin_code, until_timestamp)
                for tweet in tweets:
                    source_time = tweet.get("source_time")
                    source_time = re.search(r"(.*?)T", source_time)[1]
                    if [day.year, day.month, day.day] == [int(i) for i in source_time.split('-')]:
                        text = tweet.get("text")
                        if not (text.startswith("http") or text.startswith("#") or text.startswith("@")):
                            text = text.replace(',', '，').replace('\n', '    ')
                            print(text[:100] + "... ...")
                            l_coin_tweets.append(coin_name + "," + text)

                f.write('\n'.join(l_coin_tweets))
            f.write('\n')


def trans_datetime_to_timestamp(date_time):
    bj_time = date_time + timedelta(hours=8)
    return  time.mktime(bj_time.timetuple())


def run():
    start_date = (2018,5,1)
    sort_by_day(start_date)


if __name__ == "__main__":
    run()