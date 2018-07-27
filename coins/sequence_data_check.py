# -*- coding: UTF-8 -*-
'''
Check sequence data in MongoDB
'''
import datetime, sys
from pymongo import MongoClient
sys.path.append("..")
from common.common import *

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf

connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db = connection.bcf


# Transfer code to name
def coin_transfer_code_to_name(code):
    name = db.tokens.find({"code": code})[0].get("name")
    return name


# Define the coins to be checked
def get_coins_to_be_checked():
    sys.path.append("..")
    with open("cmc_coins.txt", "rt") as f:
        coins = f.read().strip().split('\n')

    # get coins start with stars
    coins = [coin.replace('*', "") for coin in coins if coin.startswith('*')]
    return coins

# Check if time is valid(time equals zero)
def get_valid_invalid_times(times):
    valid = []
    invalid = []
    for i in times:
        if i.time() == datetime.time(0,0):
            valid.append(i)
        else:
            invalid.append(i)
    return valid, invalid


# Check if time is consecutive, if consecutive = 0, get the lost time
# if consecutive = 1 , format the consecutive times and single times
# time range: [start_time, end_time]
def if_time_consecutive(l_times, consecutive = 0, start_time = datetime.datetime(1970,1,1),
                        end_time = datetime.datetime(1970,1,1)):
    valid_times, invalid_times = get_valid_invalid_times(l_times)
    lost_times = []

    if start_time == datetime.datetime(1970,1,1) and end_time == datetime.datetime(1970,1,1):
        times = valid_times
    elif start_time != datetime.datetime(1970,1,1) and end_time == datetime.datetime(1970,1,1):
        times = [t for t in valid_times if t >= start_time]
    elif start_time == datetime.datetime(1970,1,1) and end_time != datetime.datetime(1970,1,1):
        times = [t for t in valid_times if t <= end_time]
    else:
        times = [t for t in valid_times if t >= start_time and t <= end_time]

    times.sort()

    if consecutive == 0:
        t = times[0]

        while t < times[-1]:
            if t not in times:
                lost_times.append(t)
            t += datetime.timedelta(days = 1)
        return [t.strftime("%Y.%m.%d") for t in lost_times]

    else:
        if len(times) > 0:
            j = 0
            str1 = ''
            for i, item in enumerate(times):
                if i > 0:
                    if times[i] != times[i - 1] + datetime.timedelta(days=1):
                        tmp = times[j:i]
                        if len(tmp) == 1:
                            str1 += str(tmp[0].date().strftime("%Y.%m.%d")) + ','
                        else:
                            str1 += str(tmp[0].date().strftime("%Y.%m.%d")) + "-" + str(
                                tmp[-1].date().strftime("%Y.%m.%d")) + ','
                        j = i
            tmp2 = times[j:]
            if len(tmp2) == 1:
                str1 += str(tmp2[0].date().strftime("%Y.%m.%d")) + ','
            else:
                str1 += str(tmp2[0].date().strftime("%Y.%m.%d")) + "-" + str(tmp2[-1].date().strftime("%Y.%m.%d")) + ','

            return str1[:-1]


def get_today():
    now = datetime.datetime.now()
    today = datetime.datetime(now.year, now.month, now.day)
    return today

def check_seq_data():
    # Some important items
    important_check_items = ['code', 'time', 'cmc_candle_open','chain_active_user_count_day','git_commit_count',
                             'telegram_post_count','twitter_tweet_count']

    # Total items need to be checked
    all_check_items = ['_id', 'code', 'time', 'chain_active_user_count_5day', 'chain_active_user_count_day', 'chain_balance_avg',
                       'chain_balance_sum', 'chain_new_user_count_5day', 'chain_new_user_count_day', 'chain_new_user_rate_day',
                       'chain_trans_amount_avg_day', 'chain_trans_amount_ratio_day', 'chain_trans_amount_sum_day', 'cmc_candle_close',
                       'cmc_candle_high', 'cmc_candle_low', 'cmc_candle_market_cap', 'cmc_candle_open', 'cmc_candle_volume',
                       'git_commit_count', 'git_commit_count_30day', 'git_commit_count_7day', 'git_commit_count_day', 'git_event_count',
                       'git_fork_count', 'git_fork_count_30day', 'git_fork_count_7day', 'git_fork_count_day', 'git_star_count',
                       'git_star_count_30day', 'git_star_count_7day', 'git_star_count_day', 'telegram_post_count',
                       'telegram_post_count_30day', 'telegram_post_count_7day', 'telegram_post_count_day', 'telegram_user_count',
                       'telegram_user_count_30day', 'telegram_user_count_7day', 'telegram_user_count_day', 'twitter_favorite_count',
                       'twitter_favorite_count_30day', 'twitter_favorite_count_7day', 'twitter_favorite_count_day',
                       'twitter_follower_count', 'twitter_follower_count_30day', 'twitter_follower_count_7day',
                       'twitter_follower_count_day','twitter_media_count', 'twitter_tweet_count', 'twitter_tweet_count_30day',
                       'twitter_tweet_count_7day', 'twitter_tweet_count_day']


    coins = get_coins_to_be_checked()

    # coins = get_coins()
    d_results = {}
    for c in coins:
        # get name
         name = coin_transfer_code_to_name(c) if c.startswith('0x') else c

        # get all data from database
         seq_data = {}
         data = db.seq_day.find({"code": c})
         for d in data:
             seq_data.update({d.get('time'):[list(d.keys()), list(d.values())]})

         c_results = {}
         # check if time lost
         lost_times = if_time_consecutive(list(seq_data.keys()))
         # {coin: {item1: times, item2: times}}
         if lost_times:
            c_results.update({'No data at these time': lost_times})
         # for check_item in important_check_items:
         for check_item in all_check_items:
             get_times = []
             for time, keys_and_values in seq_data.items():
                 keys = keys_and_values[0]
                 if check_item not in keys:
                     get_times.append(time)


             # different check items have different start check time
             ## git, telegram, twitter check data until today
             start_check_date = datetime.datetime(2018,7,1)
             start_check_date_tw = datetime.datetime(2018,7,1)
             end_chech_date = get_today() - datetime.timedelta(days=1)

             if check_item.startswith('tw') or check_item.startswith('tel'):
                 check_result = if_time_consecutive(get_times, consecutive=1, start_time = start_check_date_tw, end_time=end_chech_date)
             else:
                 check_result = if_time_consecutive(get_times, consecutive=1, start_time = start_check_date, end_time=end_chech_date)

             if check_result:
                c_results.update({check_item:check_result})

         if c_results:
            d_results.update({name:c_results})

    time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log = os.path.join(log_path, "{}_sequence_check".format(time))

    with open(log, 'wt') as f:
        f.write(json.dumps(d_results))
        f.write("\n")

    return log

def gen_standard_report(seq_log_file):
    seq_check_standard_report = "{}.html".format(seq_log_file)
    with open(seq_log_file, 'rt') as f_in:
        d = json.loads(f_in.read())
        with open(seq_check_standard_report, 'wt', encoding='utf-8') as f_out:
            update_time = datetime.datetime.now()
            f_out.write("数据质量检查")
            f_out.write("\n")
            f_out.write("更新时间: {}".format(update_time))
            f_out.write("\n")

            cmc, chain, git, twitter, telegram = [], [], [], [], []
            for name, item in d.items():
                for check_item, times in item.items():
                    if check_item.startswith('cmc'):
                        cmc.append((name, times))
                    elif check_item.startswith('chain'):
                        chain.append((name, times))
                    elif check_item.startswith('git'):
                        git.append((name, times))
                    elif check_item.startswith('twitter'):
                        twitter.append((name, times))
                    elif check_item.startswith('telegram'):
                        telegram.append((name, times))

            if cmc:
                f_out.write("\n")
                f_out.write("行情数据：\n")
                for i, v in enumerate(cmc):
                    f_out.write("{}. {}: 缺少 {} 数据\n".format(i+1, v[0], v[1]))

            if chain:
                f_out.write("\n")
                f_out.write("链上数据：\n")
                for i, v in enumerate(chain):
                    f_out.write("{}. {}: 缺少 {} 数据\n".format(i+1, v[0], v[1]))

            if git:
                f_out.write("\n")
                f_out.write("Git数据：\n")
                for i, v in enumerate(git):
                    f_out.write("{}. {}: 缺少 {} 数据\n".format(i+1, v[0], v[1]))

            if twitter:
                f_out.write("\n")
                f_out.write("Twitter数据：\n")
                for i, v in enumerate(twitter):
                    f_out.write("{}. {}: 缺少 {} 数据\n".format(i+1, v[0], v[1]))

            if telegram:
                f_out.write("\n")
                f_out.write("Telegram：\n")
                for i, v in enumerate(telegram):
                    f_out.write("{}. {}: 缺少 {} 数据\n".format(i+1, v[0], v[1]))

    # if gen_html == 1:
    #     with open(seq_check_standard_report, 'rt', encoding='utf-8') as f:
    #         content = f.read().replace('\n', '<br/>')
    #
    #     with open(Seq_check_html_report, 'wt') as f:
    #         f.write(Seq_report_head)
    #         f.write(content)
    #         f.write(Seq_report_tail)


if __name__ == "__main__":
    check_seq_data()