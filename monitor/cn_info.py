"""
This script is used to monitor the cn_info news data.
The result is restored in mongo DataMonitor.cn_info_monitor.
The script is executed once a day.
"""
import datetime
import sys
sys.path.append("..")
from pymongo import MongoClient
from common.common import connect_mongodb, write_log_into_mongodb


def check_cn_info(source):
    db = connect_mongodb("SPIDER_MONGODB", "FinanceInfo")

    current_time = datetime.datetime.utcnow()
    start_time = current_time - datetime.timedelta(days=1)
    # print(start_ts)

    count = db.get_collection(source).find({"time": {"$gt": start_time}}).count()
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": count}
    log_collection = source + "_monitor"
    write_log_into_mongodb(log_collection, d_check_result)


if __name__ == "__main__":
    for source in ["cn_info", "sse_info"]:
        check_cn_info(source)