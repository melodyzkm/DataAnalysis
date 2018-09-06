"""
This script is used to monitor if the logo of the coin exists and the logo url is available or not.
The result will be restored in mongodb DataMonitor.logos.
The script is executed once a day by shell command.
"""

import datetime
import requests
import sys
sys.path.append("..")
from common.common import write_log_into_mongodb


def get_invalid_logos():
    invalid_logos = []
    api = "https://www.mybesttoken.cn/api/v3/token/all"
    tokens = requests.get(api).json()
    for token in tokens:
        code = token.get("code")
        name = token.get("name")
        if "logo_url" not in token or not token.get("logo_url") or token.get("logo_url") == "":
            invalid_logos.append({"code": code, "name": name, "logo_ur": None})
        else:
            logo_url = token.get("logo_url")
            status_code = requests.get(logo_url).status_code
            if status_code != 200:
                invalid_logos.append({"code": code, "name": name, "logo_ur": logo_url})
    return invalid_logos


def write_result_into_mongo():
    collection = "logos"
    now = datetime.datetime.now()
    data = get_invalid_logos()
    insertion = {"create_time": now, "invalid_token": data}
    write_log_into_mongodb(collection, insertion)


if __name__ == "__main__":
    write_result_into_mongo()