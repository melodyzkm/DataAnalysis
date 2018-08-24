import re
import os
import json
import xlrd
import requests
import sys
sys.path.append("..")
from common.common import *
from pymongo import MongoClient, DESCENDING
from bs4 import BeautifulSoup

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf


def get_top_coin_code_name_abbr(top_no):
    connection = MongoClient(BC_MONGODB, PORT_MONGODB)
    db = connection.bcf
    top = []
    for token in db['token_indicator_slices'].find().sort([('indicators.cmc_market_cap_usd', -1)]).limit(top_no):
        print(token)
        match_coin = re.match(r'(.*?)\s\((.*?)\)', token['name'])
        if match_coin:
            name = match_coin.group(1)
            name_abbr = match_coin.group(2)
            top.append((token['code'], name, name_abbr))
        else:
            print("***" + token['code'])
    return top


def sync():
    for coin_info in db_bcf.tokens.find({"links.coinmarketcap": {"$size": 1}, "logo_url": {"$exists": True}}):
        code = coin_info.get("code")
        name = coin_info.get("name")
        url = coin_info.get("logo_url")
        if url != None:
            sub_url = url.replace("http://bcfdata.oss-us-west-1.aliyuncs.com/logos/", "")
            if len(sub_url)  == 44:
                print(code, name, url)


def insert_logo_url(file_path):

    if file_path.startswith("cmc"):
        img_format = "png"
    else:
        img_format = 'jpg'

    with open(file_path, 'rt', encoding='utf-8') as f:
        for line in f.readlines():
            d_coins = json.loads(line)
            code = d_coins.get("code")
            name = d_coins.get("name")
            # abbr = d_coins.get("name_abbr")
            # name_cn = d_coins.get("name_cn")

            if db_bcf.tokens.find_one({"code": code}):

                try:
                    url = "https://bcfdata-cn.oss-cn-hangzhou.aliyuncs.com/logos/{}.{}".format(re.sub('\s','%20', name), img_format)
                    if re.search(r'\s', name):
                        if requests.get("https://bcfdata-cn.oss-cn-hangzhou.aliyuncs.com/logos/{}.{}".format(re.sub('\s','-', name), img_format)).ok:
                            url = "https://bcfdata.oss-us-west-1.aliyuncs.com/logos/{}.png".format(re.sub('\s', '-', name))
                            db_bcf.tokens.update_one({"code": code}, {"$set": {"logo_url": url}})

                        elif requests.get("https://bcfdata-cn.oss-cn-hangzhou.aliyuncs.com/logos/{}.{}".format(re.sub('\s','%20', name), img_format)).ok:
                            url = "https://bcfdata-cn.oss-cn-hangzhou.aliyuncs.com/logos/{}.{}".format(re.sub('\s','%20', name), img_format)
                            db_bcf.tokens.update_one({"code": code}, {"$set": {"logo_url": url}})

                        else:
                            print("no url: {}, {}".format(code, name))

                    else:
                        db_bcf.tokens.update_one({"code": code}, {"$set": {"logo_url": url}})

                except Exception as e:
                    print(e)


def insert_abbr(file_path):
    with open(file_path, 'rt', encoding='utf-8') as f:
        for line in f.readlines():
            d_coins =  json.loads(line)
            code = d_coins.get("code")
            name = d_coins.get("name")
            abbr = d_coins.get("name_abbr")

            # print(code)
            if db_bcf.tokens.find_one({"code": code}):
                coin = db_bcf.tokens.find_one({"code": code})
                coin_abbr = coin.get("abbr")
                if coin_abbr != abbr:
                    print(code, name, abbr, coin_abbr)
                    db_bcf.tokens.update_one({"code": code}, {"$set": {"abbr": abbr}})


def insert_cn_name(file_path):
    # flush all name_cn
    for token in db_bcf.tokens.find({}):
        code = token.get("code")
        db_bcf.tokens.update_one({"code": code}, {"$set": {"name_cn": None}})

    with open(file_path, 'rt', encoding='utf-8') as f:
        for line in f.readlines():
            d_coins =  json.loads(line)
            code = d_coins.get("code")
            name = d_coins.get("name")
            # abbr = d_coins.get("name_abbr")

            if 'name_cn' in d_coins:
                name_cn = d_coins.get("name_cn")
                pattern_c = re.compile(u"([\u4e00-\u9fa5]+)")
                if re.search(pattern_c, name_cn):
                    print(code, name, name_cn)
                    db_bcf.tokens.update_one({"code": code}, {"$set": {"name_cn": name_cn}})


def write_name_en():
    for token in db_bcf.tokens.find({"name":{"$exists": True}}):
        name = token.get("name")
        code = token.get("code")
        if name != None:
            db_bcf.tokens.update_one({"code": code}, {"$set":{"name_en": name}})


def get_cmc_coins(logo_path):
    names = []
    for root, dir, files in os.walk(logo_path):
        for file in files:
            names.append(file.replace('.png',''))
    return names



if __name__ == "__main__":
    # insert_cn_name("feixiaohao_coin_infos.jl")
    # insert_logo_url("feixiaohao_coin_infos.jl")
    insert_logo_url("cmc_coin_infos.jl")
    # write_name_en()?
    # insert_abbr("cmc_coin_infos.jl")
