import re
import sys
sys.path.append("..")
from common.common import *
from pymongo import MongoClient

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf


def get_coin_with_feixiaohao_or_cmc_link():
    tokens = []
    for token in db_bcf.tokens.find({"$or":[{"links.feixiaohao":{"$size":1}}, {"links.coinmarketcap":{"$size":1}}]}):
        code = token.get("code")
        name = token.get("name")
        name_abbr = token.get("abbr")
        tokens.append((code, name, name_abbr))

    return tokens


def get_top_coin_code_name_abbr(top_no):
    top = []
    for token in db_bcf['token_indicator_slices'].find().sort([('indicators.cmc_market_cap_usd', -1)]).limit(top_no):
        match_coin = re.match(r'(.*?)\s\((.*?)\)', token['name'])
        if match_coin:
            name = match_coin[1]
            name_abbr = match_coin[2]
            top.append((token['code'], name, name_abbr))
        else:
            print("***" + token['code'])
    return top

def get_coins_from_3rd_website(spider_file):
    # feixiaohao
    # D:\Python\Spider\feixiaohao\feixiaohao.txt
    coins_top2000 = get_top_coin_code_name_abbr(3000)
    d_coins_top2000 = {i[2]:i[1] for i in coins_top2000}
    print(d_coins_top2000)

    no_such_coins_in_our_website = []
    abbrs_not_the_same = {}

    with open(spider_file, 'rt', encoding='utf-8') as f:
        contents = f.read().split(',')
        # "0#bitcoin#Bitcoin#比特币#BTC#/coin/7033f2f2c2a16094bbb3bafc47205ba8_small.png",
        for content in contents:
            type_code, lower_name, name, cn_name, abbr_name, logo_url = content.split("#")
            if abbr_name in d_coins_top2000:
                if  not name == d_coins_top2000.get(abbr_name):
                    abbrs_not_the_same.update({abbr_name: "{}/{}".format(name, d_coins_top2000.get(abbr_name))})
            else:
                no_such_coins_in_our_website.append((name, abbr_name))

    return no_such_coins_in_our_website, abbrs_not_the_same

if __name__ == "__main__":
    # coins =  get_coin_with_feixiaohao_or_cmc_link()
    markets = []
    for i in db_bcf.market_symbol.find({}):
        markets.append(i.get('market'))

    markets = list(set(markets))

    old_markets = []
    for i in db_bcf.market_infos.find():
        old_markets.append(i.get('market'))

    print([i for i in old_markets if i not in markets])





