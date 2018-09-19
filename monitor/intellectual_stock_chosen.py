"""
This script is used to check if intellectual stock chosen works normally.
The result is restored in mongo DataMonitor.intellectual_stock_chosen.
The script is executed once a day.
"""

import datetime
import sys
sys.path.append("..")
from common.common import connect_mongodb, write_log_into_mongodb


def intellectual_stock_chosen():
    db = connect_mongodb("QE_GREY_MONGODB", "QuantEye")

    current_time = datetime.datetime.now()
    current_time_with_12_o_clock = datetime.datetime(current_time.year, current_time.month, current_time.day, 0, 0, 0)
    start_time = current_time_with_12_o_clock - datetime.timedelta(days=1,hours=8)

    total_amount = db.get_collection("picking_strategies").find({}).count()
    refreshed_count = 0
    for item in db.get_collection("picking_strategies").find({}):
        print(item.get("state").get("last_date_with_picked_targets"))
        if item.get("state").get("last_date_with_picked_targets") >= start_time:
            refreshed_count += 1
    d_check_result = {"create_time": datetime.datetime.now(), "24h_updated_amount": refreshed_count, "total": total_amount}
    print(d_check_result)
    write_log_into_mongodb("intellectual_stock_chosen_monitor", d_check_result)


if __name__ == "__main__":
    intellectual_stock_chosen()