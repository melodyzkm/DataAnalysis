# coding=utf-8

import json
import re
import datetime
import hashlib
from pymongo import MongoClient

# 配置Mongo
BC_MONGODB = '192.168.4.158'
PORT_MONGODB = 27017
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf

# 需要抓取的关键词
d_keywords = {'kw': ["airdrop","airswap","burn","list","exchange","swap","halve","halving","partnership",
                     "cooperate","redesign","rebrand","launch","release","announce","Roadmap","plan", "planning",
                     "planned", "feature","update","upgrade","available","version","wallet","quater","report","bonus",
                     "giveaway","distribute","dividend","payout","ico","audit"]}

# coins = ['Bitcoin','Ethereum', 'Ripple', 'BitcoinCash', '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 'Litecoin',
#          'Stellar', 'Cardano', 'IOTA','0xf230b790e05390fc8295f4d3f60332c93bed42e2', 'Tether', '0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b.neo',
#          'Dash', 'Monero', '0xb8c77482e45f1f44de1745f52c74426c631bdd52', 'NEM','0xd850942ef8811f2a866692a623011bde52a462c1',
#          'EthereumClassic','Ontology','0xd26114cd6ee289accf82350c8d8487fedb8a0c07']

coins = ["0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0"]

def get_md5(str):
    return hashlib.md5(str.encode()).hexdigest()

def get_tw_tweets(start_date):
    d_results = {}

    top_coins = get_coin_code_name_abbr()
    d_top_coins = {}
    for code, name, abbr in top_coins:
        d_top_coins.update({name: code})

    # d_tw_coins = {name: code for name, code in  d_top_coins.items() if name in coins}
    # print(d_top_coins)
    d_coins_with_twitter_names = get_coin_twitter_names(d_top_coins)
    print(d_coins_with_twitter_names)

    for coin_name in d_coins_with_twitter_names:
        # print(coin_name)
        coin_code = d_top_coins.get(coin_name)
        d_tweets = {}
        twitters = db_bcf.twitter_tweets.find({'code': coin_code, "data.datetime":{"$gt": start_date, "$lt":start_date + datetime.timedelta(days=1)}})
        for twitter in twitters:
            tw_name = d_coins_with_twitter_names.get(coin_code)
            print(tw_name)
            if twitter.get('data').get('usernameTweet') in tw_name:
                tweet = twitter.get('data').get('text').strip()
                print(tweet)
                url = 'https://twitter.com' + twitter.get('data').get('url')

                filtered = filter_tweet(tweet)
                if filtered:
                    d_tweets.update({url: filtered})
                    d_results.update({coin_name:d_tweets})

    return  d_results


def filter_tweet(tweet):
    # if re.match(r'@|http|RT', tweet):
    #     return None
    # elif 'news and updates' in tweet:
    #     return  None
    # else:
    #     l_tweet = tweet.lower().split(' ')
    #     if 'report' in l_tweet:
    #         for i in ["monthly", "quarterly", "annual", "full", "trading", "weekly", "new", "fake"]:
    #             if i in l_tweet:
    #                 return tweet
    #     elif 'fork' in l_tweet and 'hard' in l_tweet:
    #         return  tweet
    #     elif 'feature' in l_tweet and 'new' in l_tweet:
    #         return tweet
    #     else:
    #         for i in d_keywords.get('kw'):
    #             if i in l_tweet:
    #                 return tweet

    if re.match(r'@|http|RT', tweet):
        return None
    else:
        return tweet

def get_coin_code_name_abbr():
    coins_with_name_abbr = []
    for code in coins:
        print(code)
        token = db_bcf.tokens.find_one({"code": code})
        name = token.get("name")
        abbr = token.get("abbr")
        coins_with_name_abbr.append((code, name, abbr))
    print(coins_with_name_abbr)
    return coins_with_name_abbr


def get_coin_twitter_names(d_coins):
    d_coins_with_twitter_names = {}
    for coin_name in d_coins:
        coin_code  = d_coins.get(coin_name)
        l_twitter = []
        item = db_bcf.tokens.find_one({'code': coin_code})
        if item.get('links').get('twitter'):
            for url in item.get('links').get('twitter'):
                # print(url)
                try:
                    tw_name = re.search(r'https://twitter.com/(.+)', url)[1]
                    l_twitter.append(tw_name)
                except Exception as e:
                    pass
        d_coins_with_twitter_names.update({coin_code: l_twitter})

    print(d_coins_with_twitter_names)
    return d_coins_with_twitter_names


def between_days(start_date, end_date, step):
    while start_date < end_date:
        yield start_date
        start_date += step


def run(start_date = None):
    # out_file = "D:\\FangCloudV2\\LaplaceTech\\数据接入\\区块链数据\\coins_with_tweets.csv"
    out_file = "coins_with_tweets.csv"
    now = datetime.datetime.now()
    today = datetime.datetime(now.year, now.month, now.day, 0, 0, 0)
    yesterday = today - datetime.timedelta(days=1)

    if start_date:
        print(start_date)
        for date in between_days(start_date, today, datetime.timedelta(days=1)):
            d_result = get_tw_tweets(date)
            date = str(date.date())
            with open(out_file, "at", encoding='utf-8') as f:
                f.write(date + '\n')
                for coin in d_result:
                    d_tweets = d_result.get(coin)
                    for url in d_tweets:
                        content = d_tweets.get(url).strip().replace('\n', '').replace(',', '，')
                        f.write("{},{},{}".format(coin, content, url))
                        f.write('\n')

    else:
        with open(out_file, "at", encoding='utf-8') as f:
            f.write(str(yesterday.date()) + '\n')
            d_result = get_tw_tweets(yesterday)
            for coin in d_result:
                d_tweets = d_result.get(coin)
                for url in d_tweets:
                    content = d_tweets.get(url).strip().replace('\n', '').replace(',', '，')
                    f.write("{},{},{}".format(coin, content, url))
                    f.write('\n')


if __name__ == "__main__":
    # print(get_coin_code_name_abbr)
    #
    #
    #从哪天开始获取数据
    start_date=datetime.datetime(2018,7,1)
    #执行
    run(start_date=start_date)
    #
    # d_coins = {'Bitcoin': 'Bitcoin', 'Ethereum': 'Ethereum', 'Ripple': 'Ripple', 'Bitcoin Cash': 'BitcoinCash', 'EOS': '0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0', 'Litecoin': 'Litecoin', 'Stellar': 'Stellar', 'Cardano': 'Cardano', 'IOTA': 'IOTA', 'TRON': '0xf230b790e05390fc8295f4d3f60332c93bed42e2', 'Tether': 'Tether', 'NEO': '0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b.neo', 'Dash': 'Dash', 'Monero': 'Monero', 'Binance Coin': '0xb8c77482e45f1f44de1745f52c74426c631bdd52', 'NEM': 'NEM', 'VeChain': '0xd850942ef8811f2a866692a623011bde52a462c1', 'Ethereum Classic': 'EthereumClassic', 'Ontology': 'Ontology', 'OmiseGO': '0xd26114cd6ee289accf82350c8d8487fedb8a0c07'}
    # print(get_coin_twitter_names(d_coins))

