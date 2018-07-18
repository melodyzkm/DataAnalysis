import sys
sys.path.append("..")
from common.common import *
from pymongo import MongoClient

#constructure
'''
{
    "_id" : ObjectId("5b29ecafd8892b68b9ab544b"),
    "market" : "binance",
    "symbol" : "data-eth",
    "take_code" : "Bitcoin",
    "take_type" : "token",
    "pay_code" : "Tether",
    "pay_type" : "token"
}

'''

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf

#法币
currencies = ["usd", "eur", "jpy", "krw", "cad", "gbp"]

def get_code_according_to_abbr(abbr):
    if db_bcf.tokens.find({"abbr":{"$regex": "^{}$".format(abbr), "$options":"i"},
                           "links.coinmarketcap":{"$size":1}}).count() == 1:
        token = db_bcf.tokens.find_one({"abbr":{"$regex": "^{}$".format(abbr), "$options":"i"},
                                    "links.coinmarketcap":{"$size":1}})
        code = token.get("code")
        return code

    elif db_bcf.tokens.find({"abbr": {"$regex": "^{}$".format(abbr), "$options": "i"},
                           "links.feixiaohao": {"$size": 1}}).count() == 1:
        token = db_bcf.tokens.find_one(
            {"abbr": {"$regex": "^{}$".format(abbr), "$options": "i"}, "links.feixiaohao": {"$size": 1}})
        code = token.get("code")
        return code

    else:
        return None


def update_market_symbols():
    for symbol_info in db_bcf.market_symbol.find({}):
        market = symbol_info.get("market")
        symbol = symbol_info.get("symbol")
        aim_abbr = symbol.split("-")[0]
        pay_abbr = symbol.split("-")[1]
        if pay_abbr in currencies:
            pay_code = pay_abbr
            pay_type = "currency"
        else:
            pay_code = get_code_according_to_abbr(pay_abbr)
            pay_type = "token"

        take_code = get_code_according_to_abbr(aim_abbr)
        take_type = "token"
        db_bcf.market_symbol.update_one({"market":market, "symbol":symbol}, {"$set":{"pay_code": pay_code, "pay_type": pay_type,
                                                                      "take_code": take_code, "take_type": take_type}})

        # l_m_and_s = []
        # for i in [market, symbol, take_code, take_type, pay_code, pay_type]:
        #     if i == None:
        #         l_m_and_s.append("")
        #     else:
        #         l_m_and_s.append(i)
        #
        # with open("market_symbol_okex_bitz.csv", "at") as f:
        # # with open("d:\\a\\market_symbol.csv", "at") as f:
        #     f.write("," .join(l_m_and_s))
        #     f.write("\n")


    # print("\n".join(sorted(list(set(aim_abbrs)))))

def update_market_name():
    for symbol_info in db_bcf.market_symbol.find({}):

        market = symbol_info.get("market")
        symbol = symbol_info.get("symbol")

        d_market_cn_name = {"binance": "币安", "huobipro": "火币Pro", "kraken": "K网" }
        d_market_en_name = {"bcex": "BCEX", "bitflyer": "bitFlyer","bit-z":"Bit-Z", "coinbene": "CoinBene", "exx": "EXX",
                            "gateio":"Gate.io","gdax":"CoinbasePro", "hitbtc":"HitBTC", "huobipro": "Huobi",
                            "lbank": "LBank", "okex": "OKEx", }

        if market in d_market_en_name:
            market_name_en = d_market_en_name.get(market)
        else:
            market_name_en = market.capitalize()

        if market in d_market_cn_name:
            market_name_cn = d_market_cn_name.get(market)
        else:
            market_name_cn = market_name_en

        db_bcf.market_symbol.update_one({"market":market, "symbol":symbol}, {"$set":{"market_name_cn": market_name_cn,
                                                                                     "market_name_en": market_name_en}})


def update_infos_from_file(file):

    with open(file, 'rt', encoding='utf-8') as f:
        for line in f.readlines():
            infos = line.strip().split(",")
            market, symbol, take_code,take_type, pay_code, pay_type = infos
            if len(take_code) == 0:
                take_code = None
            if len(pay_code) == 0:
                pay_code = None
            print(market, symbol, take_code,take_type, pay_code, pay_type)
            db_bcf.market_symbol.update_one({"market": market, "symbol": symbol},
                                            {"$set": {"pay_code": pay_code, "pay_type": pay_type,
                                                      "take_code": take_code, "take_type": take_type}})


def update_all_symbol_infos():
    # clear all codes and types
    for symbol_info in db_bcf.market_symbol.find({}):
        market  = symbol_info.get("market")
        symbol  = symbol_info.get("symbol")
        db_bcf.market_symbol.update_one({"market": market, "symbol": symbol}, {"$set": {"pay_code": None, "pay_type": None,
                                                                                        "take_code": None, "take_type": None}})


    # get the token code if abbr is unique
    update_market_symbols()

    # update some other code if
    update_infos_from_file("0626_market_symbol_manually.csv")

    # update market name
    update_market_name()


if __name__ == "__main__":
    update_all_symbol_infos()
