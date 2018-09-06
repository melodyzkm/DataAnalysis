"""
This script is used to monitor the 5min sequence data.
The result is restored in mongo DataMonitor.seq_5min.
The script is executed once a day.
"""
import datetime
import os
from pymongo import MongoClient, DESCENDING
from common.common import get_config, write_log_into_mongodb


def get_top_coins():
    token_list_file = "token_list.txt"
    if not os.path.exists(token_list_file):
        raise FileExistsError(token_list_file + " does not exist!")

    with open(token_list_file, "rt") as f:
        tokens = f.read().strip().split('\n')

    return tokens


def get_all_quintuple_time():
    u_now = datetime.datetime.utcnow()
    last_quintuple_time = datetime.datetime(u_now.year, u_now.month,  u_now.day, u_now.hour, (u_now.minute//5)*5, 0)
    start_time = last_quintuple_time - datetime.timedelta(days=1)

    t = start_time
    while t <= last_quintuple_time:
        yield t
        t += datetime.timedelta(minutes=5)


def check_seq_5min(code):
    config = get_config()
    bc_mongodb = config.get("BC_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection_bcf = MongoClient(bc_mongodb, port_mongodb)
    db_bcf = connection_bcf.bcf

    sort_type = ("_id", DESCENDING)
    seq_data = db_bcf.get_collection("seq_5min").find({"code": code}).sort([sort_type]).limit(300)

    data_times = [data.get("time") for data in seq_data]
    quintuple_times = get_all_quintuple_time()
    lost_quintuple_times = [t for t in quintuple_times if t not in data_times]

    if lost_quintuple_times:
        d_check_result = {"code": code, "lost_data_time": lost_quintuple_times, "create_time": datetime.datetime.now()}
        write_log_into_mongodb("seq_5min_monitor", d_check_result)


def main_check():
    tokens = get_top_coins()
    for token in tokens:
        code = token.split(',')[0]
        check_seq_5min(code)


if __name__ == "__main__":
    main_check()