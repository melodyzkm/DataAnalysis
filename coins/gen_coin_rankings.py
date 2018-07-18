# coding=utf-8

import re
import datetime
import sys
sys.path.append("..")
from common.common import *
from pymongo import MongoClient
import csv
import codecs
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf


def gen_top_rankings():
    top_tokens = []
    for token in db_bcf.tokens.find({"links.coinmarketcap":{"$size":1}}):
            code = token.get('code')
            name = token.get('name')
            code, name = trans_name_from_en_to_cn(code, name)
            abbr = token.get('abbr')
            token_indicators = db_bcf.token_indicator_slices.find_one({"code":code})
            change_ratio_24h = token_indicators.get("indicators").get("cmc_change_ratio_24h_usd")
            volume = token_indicators.get("indicators").get("cmc_volume_usd")
            chain_new_user_rate_day = token_indicators.get("indicators").get("chain_new_user_rate_day")
            chain_new_user_count_day = token_indicators.get("indicators").get("chain_new_user_count_day")
            twitter_follower_count_day = token_indicators.get("indicators").get("twitter_follower_count_day")
            telegram_user_count_day = token_indicators.get("indicators").get("telegram_user_count_day")
            git_commit_count_7day = token_indicators.get("indicators").get("git_commit_count_7day")
            git_commit_count_7day_ratio = token_indicators.get("indicators").get("git_commit_count_7day_ratio")

            top_tokens.append([code, name, abbr, change_ratio_24h, volume, chain_new_user_rate_day, chain_new_user_count_day, twitter_follower_count_day,
                               telegram_user_count_day, git_commit_count_7day, git_commit_count_7day_ratio])

    '''
    1、每天早上9点把24小时成交前200的币筛选出来，然后在这个基础之上用24小时涨跌幅做倒序排列，给出前5名的名称，缩写，涨跌幅
    2、每天早上9点把24小时成交前5的名称，缩写，成交额给出
    '''
    # print(top_tokens)
    top_tokens_trans_none_to_zero = []

    for i in top_tokens:
        l = []
        for ii in i:
            if ii == None:
                ii = 0
                l.append(ii)
            else:
                l.append(ii)

        top_tokens_trans_none_to_zero.append(l)

    top5_volumes = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[4], reverse=True)[:5]
    top200_volumes = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[4], reverse=True)[:200]
    # 涨幅前五
    top5_change_ratios = sorted(top200_volumes, key=lambda x: x[3], reverse=True)[:5]
    # 跌幅前五
    top5_change_ratios_ascending = sorted(top200_volumes, key=lambda x: x[3])[:5]

    l_top5_change_ratios = [[i[0], i[1], i[2], change_decimal_to_percentage(i[3], multiple=1)] for i in top5_change_ratios]
    l_top5_change_ratios_ascending= [[i[0], i[1], i[2], change_decimal_to_percentage(i[3], multiple=1)] for i in top5_change_ratios_ascending]
    l_top5_volumes = [[i[0], i[1], i[2], i[4]] for i in top5_volumes]


    top200_new_user_count_day = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[6], reverse=True)[:200]
    top5_new_user_rate = sorted(top200_new_user_count_day, key=lambda x: x[5], reverse=True)[:5]
    l_top5_new_user_rate = [[i[0], i[1], i[2], change_decimal_to_percentage(i[5]), i[6]] for i in top5_new_user_rate]

    top5_twitter_new_follower = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[7], reverse=True)[:5]
    l_top5_twitter_new_follower = [[i[0], i[1], i[2], i[7]] for i in top5_twitter_new_follower]

    top5_telegram_new_follower = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[8], reverse=True)[:5]
    l_top5_telegram_new_follower = [[i[0], i[1], i[2], i[8]] for i in top5_telegram_new_follower]

    top5_git_commit_count_7day = sorted(top_tokens_trans_none_to_zero, key=lambda x: x[9], reverse=True)[:5]
    l_top5_git_commit_count_7day = [[i[0], i[1], i[2], i[9], change_decimal_to_percentage(i[10], multiple=1)] for i in top5_git_commit_count_7day]




    with open("daily_report.csv", "wt", encoding='utf-8') as f:
        f.write(str(datetime.datetime.now()) + "\n")
        f.write("1.热门币种24小时行情排名\n")
        f.write(",24小时涨幅排名 \n")
        for i in l_top5_change_ratios:
            f.write("{}-{},{}{}".format(i[1], i[2], i[3],"\n"))
        f.write(",24小时跌幅排名 \n")
        for i in l_top5_change_ratios_ascending:
            f.write("{}-{},{}{}".format(i[1], i[2], i[3],"\n"))
        f.write(",24小时成交排名 \n")
        for i in l_top5_volumes:
            f.write("{}-{},{}{}".format(i[1], i[2],i[3],"\n"))
        f.write("\n\n")

        f.write("2.链上增幅排名\n")
        f.write(",新增用户增幅,日新增用户数\n")
        for i in l_top5_new_user_rate:
            f.write("{}-{},{},{}{}".format(i[1], i[2], i[3],i[4],"\n"))
        f.write("\n\n")

        f.write("3.社群增长排名\n")
        f.write(",Twitter新增关注\n")
        for i in l_top5_twitter_new_follower:
            f.write("{}-{},{}{}".format(i[1], i[2], i[3], "\n"))
        f.write(",Telegram新增用户数\n")
        for i in l_top5_telegram_new_follower:
            f.write("{}-{},{}{}".format(i[1], i[2], i[3], "\n"))
        f.write("\n\n")

        f.write("4.7日代码增长排名\n")
        f.write(",7日代码增长数,占总代码比例\n")
        for i in l_top5_git_commit_count_7day:
            f.write("{}-{},{},{}{}".format(i[1], i[2], i[3],i[4],"\n"))



def change_decimal_to_percentage(dec, multiple=100) :
    return "{}%".format(str(round(dec, 4) * multiple))

def trans_name_from_en_to_cn(code, name):
    token = db_bcf.tokens.find_one({"code": code})
    if "name_cn" in token:
        if token.get("name_cn") != None:
            name = token.get("name_cn")
    return code, name


def send_email():
    sender = 'zhangzhangkm@126.com'
    password = 'admin123'
    receivers = ["634730740@qq.com", "chenbin923@163.com", "coolhandxs@126.com"]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱
    # receivers = ["coolhandxs@126.com"]  # 接收邮件，可设置为你的QQ邮箱或者其他邮箱

    # 创建一个带附件的实例
    message = MIMEMultipart()
    message['From'] = 'My best token'
    message['To'] = ','.join(receivers)
    subject = 'MBT日报'
    message['Subject'] = Header(subject, 'utf-8')

    # 邮件正文内容
    message.attach(MIMEText('货币热度详情', 'plain', 'utf-8'))

    # 构造附件1，传送当前目录下的 test.txt 文件
    att1 = MIMEText(open('daily_report.csv', 'rt', encoding='utf-8').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    att1["Content-Disposition"] = 'attachment; filename="report.csv"'
    message.attach(att1)

    # # 构造附件2，传送当前目录下的 runoob.txt 文件
    # att2 = MIMEText(open('runoob.txt', 'rb').read(), 'base64', 'utf-8')
    # att2["Content-Type"] = 'application/octet-stream'
    # att2["Content-Disposition"] = 'attachment; filename="runoob.txt"'
    # message.attach(att2)

    # try:
    #
    #     server = smtplib.SMTP_SSL("smtp.126.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
    #     server.login(sender, password)  # 括号中对应的是发件人邮箱账号、邮箱密码
    #     server.sendmail(sender, receivers, message.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
    #     print("邮件发送成功")
    #     server.quit()  # 关闭连接
    # except smtplib.SMTPException as e:
    #     print(e)
    #     print("Error: 无法发送邮件")



    smtp = smtplib.SMTP_SSL()                         # 创建一个连接
    smtp.connect("smtp.126.com", port=465)                      # 连接发送邮件的服务器
    smtp.login(sender, password)                # 登录服务器
    smtp.sendmail(sender, receivers, message.as_string())  # 填入邮件的相关信息并发送
    print("邮件发送成功！！！")
    smtp.quit()


def run():
    gen_top_rankings()
    send_email()


if __name__ == "__main__":
    run()
