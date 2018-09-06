"""
This script is used to monitor the cn_info news data.
The result is restored in mongo DataMonitor.cn_info_monitor.
The script is executed once a day.
"""
import datetime
from pymongo import MongoClient
from common.common import get_config, write_log_into_mongodb


def check_cn_info(source):
    config = get_config()
    spider_mongodb = config.get("SPIDER_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(spider_mongodb, port_mongodb)
    db = connection.QuantEye

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