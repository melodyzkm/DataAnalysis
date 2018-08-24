"""
This script is used to keep updating the leading tokens with a txt file -- "top_tokens".
The script is executed once a day.
"""

import re
from common.common import get_config
from pymongo import MongoClient, DESCENDING


def get_top_tokens(top: int = 100):
    config = get_config()
    bc_mongodb = config.get("BC_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(bc_mongodb, port_mongodb)
    db_bcf = connection.bcf

    tokens = []
    sort_type = ("indicators.cmc_market_cap_usd", DESCENDING)
    for t in db_bcf.get_collection("token_indicator_slices").find().sort([sort_type]).limit(top):
        code = t.get("code")
        name, abbr = re.match(r'(.*?)\s\((.*?)\)', t.get("name"))[0], re.match(r'(.*?)\s\((.*?)\)', t.get("name"))[1]
        tokens.append((code, name, abbr))

    with open("token_list.txt", "wt") as f:
        for token in tokens:
            f.write(",".join(token) + '\n')

    return tokens


if __name__ == "__main__":
    get_top_tokens()
