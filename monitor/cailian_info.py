"""
This script is used to monitor the cailian news data.
The result is restored in mongo DataMonitor.cailian_news_monitor.
The script is executed once a day.
"""
import datetime
import time
from pymongo import MongoClient, DESCENDING
from common.common import get_config, write_log_into_mongodb


def check_cailian_news():
    config = get_config()
    spider_mongodb = config.get("SPIDER_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(spider_mongodb, port_mongodb)
    db = connection.QuantEye

    current_ts = int(time.time())
    start_ts = current_ts - 24 * 60 * 60
    # print(start_ts)

    count = db.get_collection("cailian_info").find({"sort_score": {"$gt": start_ts}}).count()
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": count}
    write_log_into_mongodb("cailian_info_monitor", d_check_result)


if __name__ == "__main__":
    check_cailian_news()