import os
import sys

crontab_path = "/etc/crontab"
monitor_path = "/home/test/DataAnalysis/monitor"
log_path = "/var/log/bc_log/monitor.log"
user = "test"


def insert(monitor_file, cycle):
    with open(crontab_path, "rt", encoding="utf-8") as f:
        content = f.read()
        if monitor_file not in content:
            insertion = "{} {} cd {} && py {} >> {} 2>&1{}".format(cycle, user, monitor_path, monitor_file, log_path, "\n")
            with open(crontab_path, "at", encoding="utf-8") as f:
                f.write(insertion)


# 财联社, 每天10点运行
insert("cailian_info.py", "0 10 * * *")

# 东财新闻, 每天10点运行
insert("choice_news.py", "0 10 * * *")

# 上证互动易，每天10点运行
insert("cn_info.py", "0 10 * * *")

# Elastic Search 的查询, 每天10点运行
insert("fundamental_infos.py", "0 10 * * *")

# 新闻数据， 每1小时运行
insert("message_info.py", "0 */1 * * *")

# 新浪新闻， 每天10点运行
insert("sina_finance_info.py", "0 10 * * *")

# 华尔街中国新闻数据， 每天10点运行
insert("wscn_info.py", "0 10 * * *")