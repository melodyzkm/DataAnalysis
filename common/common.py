# -*- coding: UTF-8 -*-

import logging, time, json, os
import smtplib
import yaml
from email.mime.text import MIMEText
from email.utils import formataddr
from pymongo import MongoClient

abs_path = os.path.dirname(__file__)

# 获取父路径
root_path = os.path.abspath(abs_path + os.path.sep + "..")
# print(parent_path)

if os.name == 'nt':
    log_path = os.path.abspath(os.path.join(root_path, 'log'))
else:
    log_path = '/var/log/bc_log'
print("All results are stored in: ", log_path)

path_news_check_log = os.path.join(log_path, 'info_check.log')


logging.basicConfig(level = logging.DEBUG,
        format = '%(asctime)s  %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        filename = path_news_check_log,
        filemode = 'at')


def get_config():
    with open(os.path.join(abs_path, "config.yml"), "rt", encoding="utf-8") as f:
        contents = f.read()
        configs = yaml.load(contents)
        return configs


def write_log_into_mongodb(collection, data):
    config = get_config()
    test_mongodb = config.get("TEST_MONGODB")
    port_mongodb = config.get("PORT_MONGODB")
    connection = MongoClient(test_mongodb, port_mongodb)
    db_monitor = connection.DataMonitor
    db_monitor.get_collection(collection).insert_one(data)


def time_format_error(source, id, infos):
    logging.error("{} {}: Time format error({})".format(source, id, infos))


def content_error(source, id, infos):
    logging.error("{} {}: Content error({})".format(source, id, infos))


def log_error(col_name, check_item, check_object, details):
    logging.error("[{}] {}: {} {}".format(col_name, check_item, check_object, details))


def log_warning(col_name, check_item, check_object, details):
    logging.warning("[{}] {}: {} {}".format(col_name, check_item, check_object, details))


def log_info(col_name, check_item, check_object, details):
    logging.info("[{}] {}: {} {}".format(col_name, check_item, check_object, details))


def time2ISOString(s):
    # print(os.path.abspath('.'))

    # Convert seconds to ISO time string
    s = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(s)))
    # return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return s


def get_avg(l):
    if len(l) < 1:
        return None
    else:
        return sum(l) / len(l)


def get_median(l):
    if len(l) < 1:
        return None
    else:
        l.sort()
        return l[len(l) // 2]


def get_stdev(l):
    if len(l) < 1:
        return None
    else:
        average = get_avg(l)
        sdsq = sum([(i - average) ** 2 for i in l])
        stdev = (sdsq / (len(l))) ** 0.5
        return stdev


def reverse_dict_to_json_style(dict_to_be_reversed):
    # return json-style's string
    return json.dumps(dict_to_be_reversed)


def mail():
    my_sender = '214522896@qq.com'  # 发件人邮箱账号
    my_pass = 'qinwan'  # 发件人邮箱密码
    my_user = '214522896@qq.com'  # 收件人邮箱账号，我这边发送给自己
    ret = True
    try:
        msg = MIMEText('填写邮件内容', 'plain', 'utf-8')
        msg['From'] = formataddr(["FromRunoob", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(["FK", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "菜鸟教程发送邮件测试"  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
        ret = False
    return ret


if __name__ == "__main__":
    d = get_config()
    print(d)