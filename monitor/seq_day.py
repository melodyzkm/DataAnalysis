"""
This script is used to monitor the daily sequence data for top 10 coins.
The result is restored in mongo DataMonitor.seq_day_monitor .
The script is executed once a day.
"""
import datetime
import os
from pymongo import MongoClient, DESCENDING
from common.common import get_config, write_log_into_mongodb

config = get_config()
bc_mongodb = config.get("BC_MONGODB")
port_mongodb = config.get("PORT_MONGODB")
connection_bcf = MongoClient(bc_mongodb, port_mongodb)
db_bcf = connection_bcf.bcf

coins_with_chain_data = {"Bitcoin": "Bitcoin", "Ethereum": "Ethereum", "Ripple": "Ripple", "BitcoinCash": "BitcoinCash", "Stellar": "Stellar",
                         "Litecoin": "Litecoin", "Cardano": "Cardano", "IOTA": "IOTA", "Dash": "Dash",
                         "0xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b.neo": "NEO", "0x86fa049857e0209aa7d9e616f7eb3b3b78ecfdb0": "EOS"}


def get_top_coins_with_attributions(top_num : int = 100):
    """
    :return:  {"code": "Bitcoin", "cmc": True, "chain": True, "github": True, "twitter": True, "telegram": True}
    """
    tokens_with_attributions = []

    token_list_file = "token_list.txt"
    if not os.path.exists(token_list_file):
        raise FileExistsError(token_list_file + " does not exist!")

    with open(token_list_file, "rt") as f:
        tokens = f.read().strip().split('\n')

    tokens = tokens[:top_num]
    for token in tokens:
        code = token.split(',')[0]

        token_info = db_bcf.get_collection("tokens").find_one({"code": code})

        token_with_chain_eth = token_info.get("chain")
        chain = True if token_with_chain_eth == "Ethereum" or code in coins_with_chain_data else False

        tokens_links = token_info.get("links")
        cmc = True if "coinmarketcap" in tokens_links and tokens_links.get("coinmarketcap") else False
        github = True if "github" in tokens_links and tokens_links.get("github") else False
        telegram = True if "telegram" in tokens_links and tokens_links.get("telegram") else False
        twitter = True if "twitter" in tokens_links and tokens_links.get("twitter") else False

        tokens_with_attributions.append({"code": code, "cmc": cmc, "chain": chain, "github": github, "twitter": twitter, "telegram": telegram})

    return tokens_with_attributions


def get_all_single_day():
    """
    :return: recent 7 days, type: datetime
    """
    u_now = datetime.datetime.utcnow()
    last_day = datetime.datetime(u_now.year, u_now.month,  u_now.day, 0, 0, 0) - datetime.timedelta(days=1)
    start_day = last_day - datetime.timedelta(days=6)

    t = start_day
    while t <= last_day:
        yield t
        t += datetime.timedelta(days=1)


def check_seq_day(code_with_attribution):
    """
    :param code_with_attribution: {"code": code, "cmc": cmc, "chain": chain, "github": github, "twitter": twitter, "telegram": telegram}
    :return:
    {code: code,
    date_no_data:[],
    data_lost: [{time1:[], time2:[]}]
    }
    """
    sort_type = ("_id", DESCENDING)
    code = code_with_attribution.get("code")
    seq_data = db_bcf.get_collection("seq_day").find({"code": code}).sort([sort_type]).limit(8)

    data_days = [data.get("time") for data in seq_data]
    days = get_all_single_day()

    d_check_result = {}

    data_lost_day = [day for day in days if day not in data_days]
    if data_lost_day:
        d_check_result.update({"date_no_data": data_lost_day})

    # regenerator
    start_date = [d for d in get_all_single_day()][0]
    seq_data = db_bcf.get_collection("seq_day").find({"code": code, "time": {"$gte": start_date}}).sort([sort_type])
    for data in seq_data:
        daily_check_result = {}
        time = data.get("time")
        str_time = time.strftime("%Y-%m-%d")

        if code_with_attribution.get("cmc"):
            check_cmc = [i for i in config.get("cmc") if i not in data]
            if check_cmc:
                daily_check_result.update({"cmc": check_cmc})

        if code_with_attribution.get("chain"):
            now = datetime.datetime.utcnow()
            if now.date() != time.date():
                check_chain = [i for i in config.get("chain") if i not in data]
                if check_chain:
                    daily_check_result.update({"chain": check_chain})

        if code_with_attribution.get("github"):
            check_github = [i for i in config.get("github") if i not in data]
            if check_github:
                daily_check_result.update({"github": check_github})

        if code_with_attribution.get("telegram"):
            check_telegram = [i for i in config.get("telegram") if i not in data]
            if check_telegram:
                daily_check_result.update({"telegram": check_telegram})

        if code_with_attribution.get("twitter"):
            check_twitter = [i for i in config.get("twitter") if i not in data]
            if check_twitter:
                daily_check_result.update({"twitter": check_twitter})

        if daily_check_result:
            d_check_result.update({str_time: daily_check_result})

    if d_check_result:
        d_check_result.update({"code": code, "create_time": datetime.datetime.now()})
        write_log_into_mongodb("seq_day_monitor", d_check_result)


def main_check():
    tokens_with_attributions = get_top_coins_with_attributions()
    for token in tokens_with_attributions:
        check_seq_day(token)


if __name__ == "__main__":
    main_check()