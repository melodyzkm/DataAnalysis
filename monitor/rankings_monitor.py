"""
This script is used to check if rankings works normally.
The result is restored in mongo DataMonitor.rankings.
The script is executed once an hour.
"""

import datetime
import sys
sys.path.append("..")
from common.common import connect_mongodb, write_log_into_mongodb


def rankings_check():
    db = connect_mongodb("QE_PRODUCT_MONGODB", "QuantEye")

    current_time = datetime.datetime.utcnow()
    start_time = current_time - datetime.timedelta(minutes=10)

    total_amount = 0
    refreshed_count = 0
    for item in db.get_collection("factor_rankings").find({}):
        name = item.get('info').get("name")
        if "农业品" in name or "工业品" in name:
            total_amount += 1
            if item.get("update_info").get("update_time") > start_time:
                refreshed_count += 1
    d_check_result = {"create_time": datetime.datetime.now(), "1h_updated_amount": refreshed_count, "total": total_amount}
    write_log_into_mongodb("rankings", d_check_result)


if __name__ == "__main__":
    rankings_check()