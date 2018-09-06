"""
This script is used to monitor the message_info data.
The result is restored in mongo DataMonitor.message_info_monitor.
The script is executed once a day.
"""
import datetime
import time
from pymongo import MongoClient
from common.common import get_config, write_log_into_mongodb


def check_message_info():
    config = get_config()
    spider_mongodb = config.get("SPIDER_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(spider_mongodb, port_mongodb)
    db = connection.QuantEye

    current_ts = int(time.time())
    start_ts = current_ts - 1 * 60 * 60
    # print(start_ts)

    count = db.get_collection("message_info").find({"sort_score": {"$gt": start_ts}}).count()
    d_check_result = {"create_time": datetime.datetime.now(), "1h_updated_amount": count}
    write_log_into_mongodb("message_info_monitor", d_check_result)


if __name__ == "__main__":
    check_message_info()