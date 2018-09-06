"""
This script is used to monitor the choice_news news data.
The result is restored in mongo DataMonitor.choice_news_monitor.
The script is executed once a day.
"""
import datetime
import sys
sys.path.append("..")
from pymongo import MongoClient
from common.common import get_config, write_log_into_mongodb


def check_choice_news():
    config = get_config()
    spider_mongodb = config.get("SPIDER_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(spider_mongodb, port_mongodb)
    db = connection.QuantEye

    current_time = datetime.datetime.utcnow()
    start_time = current_time - datetime.timedelta(days=1)
    # print(start_ts)

    count = db.get_collection("choice_news").find({"date": {"$gt": start_time}}).count()
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": count}
    write_log_into_mongodb("choice_news_monitor", d_check_result)


if __name__ == "__main__":
    check_choice_news()