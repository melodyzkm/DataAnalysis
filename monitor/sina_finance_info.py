"""
This script is used to monitor the sina finance info data.
The result is restored in mongo DataMonitor.cailian_news_monitor.
The script is executed once a day.
"""
import datetime
import time
import sys
sys.path.append("..")
from pymongo import MongoClient, DESCENDING
from common.common import get_config, write_log_into_mongodb


def check_sina_finance_info():
    config = get_config()
    spider_mongodb = config.get("SPIDER_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(spider_mongodb, port_mongodb)
    db = connection.QuantEye

    current_ts = int(time.time())
    start_ts = current_ts - 24 * 60 * 60
    # print(start_ts)

    amount = 1000
    sort_type = ("_id", DESCENDING)
    recent_news_time = [int(new.get("created_at")) for new in db.get_collection("sina_finance_info").find({}).sort([sort_type]).limit(amount)]
    count = len([i for i in recent_news_time if i > start_ts])
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": count}
    write_log_into_mongodb("sina_finance_info_monitor", d_check_result)


if __name__ == "__main__":
    check_sina_finance_info()