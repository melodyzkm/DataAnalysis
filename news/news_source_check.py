# coding=utf-8
import re, datetime, time, logging

from bson.objectid import ObjectId
from pymongo import MongoClient, DESCENDING
import sys
sys.path.append("..")
from common.common import *


config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf

connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db = connection.bcf

# Chinese character
pattern_c = re.compile(u"([\u4e00-\u9fa5]+)")


def time2ISOString(time_stamp):
    """
    Convert seconds to ISO time string.
    :param s:
    float(time_stamp)
    :return:
    str(%Y-%m-%d %H:%M:%S)
    """
    time_stamp = int(time_stamp) if isinstance(time_stamp, str) else time_stamp
    time_stamp = time_stamp/1000 if len(str(time_stamp)) > 10 else time_stamp

    s = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(float(time_stamp)))
    return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def check_messages_update(messages, info_source):
    ''' Check if update regularly.
    :param s:
    messages: [str]
    info_source: str
    :return:
    logs
    '''

    messages = list(set(messages))

    now = datetime.datetime.utcnow()
    start_checkpoint = now- datetime.timedelta(hours=2)
    base_checkpoint = now - datetime.timedelta(days=1)

    times = []
    for message in messages:
        time = message[1]
        times.append(time)

    updated_times = [d for d in times if d >= start_checkpoint]
    if len(updated_times) > 0:
        logging.info("[{}] {} - {} Updated news number: {}". format(info_source, start_checkpoint, now, len(updated_times)))
    else:
        logging.error("[{}] {} - {} Updated news number: {}".format(info_source, start_checkpoint, now, len(updated_times)))

    # check out-of-date times
    out_of_date = [d for d in times if d < base_checkpoint]

    if len(out_of_date) > 0:
        out_of_date = list(tuple(out_of_date))
        logging.warning("[{}] {} - {} Out-of-date news number:  {}". format(info_source, now, base_checkpoint, len(out_of_date)))


def save_id(storage_file, last_id):
    # to save id
    with open(storage_file, 'rt') as f:
        id = f.read()

    # if any id in storage file
    if len(id) > 0:
        start_id = id
    # take last id as id
    else:
        start_id = last_id

    # write the id into storage file
    with open(storage_file, 'wt') as f:
        f.write(last_id)

    return start_id


def get_bishijie_news():
    ## Raw Data Format
    # {
    #     "_id": "xxx", // 文档标识id
    #     "data": {
    #         "buttom": [
    #             {
    #             "content":"Telegram计划在ICO第二轮募资17亿美元。", // 新闻内容
    #             "issue_time": 1519798637, // 发表时间
    #             }
    #         ],
    #     }
    # }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.bishijie_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/bishijie', last_id)

    create_times_to_ISO = []
    contents = []
    object_ids = []

    infos = [a for a in db.bishijie_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        for date, value in info.get('data').items():
            bottoms = value.get('buttom')
            #get create times and contents
            for i in bottoms:
                # check if the time format is right
                try:
                    # get id
                    object_ids.append(info.get('_id'))
                    create_times_to_ISO.append(time2ISOString(i.get('issue_time')))
                    # check if content contains any valid character
                    content = i.get('content')
                    if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
                        content_error('[bishijie_news]', info.get('_id'), content)
                    contents.append(i.get('content'))

                except ValueError as e:
                    time_format_error('[bishijie_news]', info.get('_id'), i.get('issue_time'))


    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[bishijie_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, create_times_to_ISO, contents)]

    # check messages
    check_messages_update(messages, 'bishijie_news')


def get_jinse_news():
    ## Raw Data Format
    # {
    #     "_id": ObjectId("5b4de3301fdc6f78a448b190"),
    #     "list": [
    #         {
    #             "lives": [
    #                 {
    #                     "content": "【动态 | 前Overstock高管现担任巴巴多斯区块链公司Bitt总裁】,
    #                     "created_at": 1531831042
    #                 }
    #             ],
    #             "date": "2018-07-17"
    #         }
    #     ]
    # }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.jinse_news.find().sort("_id", -1).limit(1)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/jinse', last_id)

    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.jinse_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        # items = info.get("list")
        lives = info.get("list")[0].get("lives")
        if lives:
            for live in lives:
                    # get create times and contents
                    try:
                        object_ids.append(info.get('_id'))
                        create_time = time2ISOString(live.get('created_at'))
                        create_times.append(create_time)

                        # check if content contains any valid character
                        content = live.get('content')
                        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
                            content_error('[jinse_news]', info.get('_id'), content)
                        contents.append(content)

                    except ValueError as e:
                        time_format_error('[jinse_news]', info.get('_id'), time2ISOString(live.get('created_at')))

    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[jinse_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, create_times, contents)]

    # check messages
    check_messages_update(messages, 'jinse_news')


def get_cryptopanic_news():
    ## Raw Data Format
    # {
    #     "_id": ObjectId("5ac1c36e1fdc6f1be77a252a"),
    #     "crawl_time": ISODate("2018-04-02T13:45:18.131Z"),
    #     "date": ISODate("2018-04-02T09:05:00.000Z"),
    #     "content": ""
    #  }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.cryptopanic_news_new.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/cryptopanic', last_id)

    # get crawl_time
    crawl_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.cryptopanic_news_new.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        # get create times and contents
        # get id
        object_ids.append(info.get('_id'))
        # check if content contains any valid character
        title = info.get('title')
        if not (re.search(r'[a-zA-Z]', title) or re.search(pattern_c, title)):
            content_error('[cryptopanic_news_new]', info.get('_id'), title)

        body = info.get('body')
        if not (re.search(r'[a-zA-Z]', title) or re.search(pattern_c, body)):
            content_error('[cryptopanic_news_new]', info.get('_id'), body)

        contents.append(title + body)

        crawl_time = info.get('crawl_time')
        if not isinstance(crawl_time, datetime.datetime):
            time_format_error('[cryptopanic_news_new]', info.get('_id'), crawl_time)
        crawl_times.append(crawl_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[cryptopanic_news_new] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i, k, v in zip(object_ids, crawl_times, contents)]

    # check messages
    check_messages_update(messages, 'cryptopanic_news_new')


def get_gongxiangcj_news():
    ## Raw Data Format
    # {
    #     "_id": ObjectId("5ac1c36e1fdc6f1be77a252a"),
    #     "crawl_time": ISODate("2018-04-02T13:45:18.131Z"),
    #     "date": ISODate("2018-04-02T09:05:00.000Z"),
    #     "content": ""
    #  }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.gongxiangcj_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/gongxiangcj', last_id)

    # get crawl_time
    crawl_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.gongxiangcj_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        #get create times and contents
        # get id
        object_ids.append(info.get('_id'))
        # check if content contains any valid character
        content = info.get('content')
        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
            content_error('[gongxiangcj_news]', info.get('_id'), content)
        contents.append(content)

        crawl_time = info.get('crawl_time')
        if not isinstance(crawl_time, datetime.datetime):
            time_format_error('[gongxiangcj_news]', info.get('_id'), crawl_time)
        crawl_times.append(crawl_time)

    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[gongxiangcj_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, crawl_times, contents)]

    # check messages
    check_messages_update(messages, 'gongxiangcj_news')


def get_jinniu_news():
    # {
    #     "_id": ObjectId("5ab24d2d1d41c841f6ec9f44"),
    #     "data": {
    #         "list": [
    #             {
    #                 "created_at": NumberLong(1531718511000),
    #                 "content": "content",
    #             },
    #         ]
    #     }
    # }
    # get start_id
    last_message = db.jinniu_news.find().sort("_id", -1).limit(1)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/jinniu', last_id)

    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.jinniu_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        #get create times and contents
        for i in info.get("data").get("list"):
            try:
                if i :
                    object_ids.append(info.get('_id'))
                    create_time = i.get('createdAt')
                    create_time = time2ISOString(create_time)
                    create_times.append(create_time)

                    # check if content contains any valid character
                    content = i.get('content')
                    if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
                        content_error('[jinniu_news]', info.get('_id'), content)
                    contents.append(content)

            except ValueError as e:
                time_format_error('[jinniu_news]', info.get('_id'), i.get('created_at'))

    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[jinniu_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, create_times, contents)]

    # check messages
    check_messages_update(messages, 'jinniu_news')


def get_jgy_news():
    ## Raw Data Format
    # {
    #     "_id": ObjectId("5ac1c36e1fdc6f1be77a252a"),
    #     "crawl_time": ISODate("2018-04-02T13:45:18.131Z"),
    #     "date": ISODate("2018-04-02T09:05:00.000Z"),
    #     "content": ""
    #  }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.jgy_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/jgy', last_id)

    # get crawl_time
    crawl_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.jgy_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        #get create times and contents
        # get id
        object_ids.append(info.get('_id'))
        # check if content contains any valid character
        content = info.get('content')
        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
            content_error('[jgy_news]', info.get('_id'), content)
        contents.append(content)

        crawl_time = info.get('crawl_time')
        if not isinstance(crawl_time, datetime.datetime):
            time_format_error('[jgy_news]', info.get('_id'), crawl_time)
        crawl_times.append(crawl_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[jgy_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, crawl_times, contents)]

    # check messages
    check_messages_update(messages, 'jgy_news')


def get_tuoniaox_news():
    ## Raw Data Format
    # {
    #     "_id": ObjectId("5ac1c36e1fdc6f1be77a252a"),
    #     "crawl_time": ISODate("2018-04-02T13:45:18.131Z"),
    #     "date": ISODate("2018-04-02T09:05:00.000Z"),
    #     "content": ""
    #  }

    ## Return
    # [{datetime: content}, {datetime: content} ...]

    # get start_id
    last_message = db.tuoniaox_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/tuoniaox', last_id)

    # get crawl_time
    crawl_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.tuoniaox_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        #get create times and contents
        # get id
        object_ids.append(info.get('_id'))
        # check if content contains any valid character
        content = info.get('content')
        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
            content_error('[tuoniaox_news]', info.get('_id'), content)
        contents.append(content)

        crawl_time = info.get('crawl_time')
        if not isinstance(crawl_time, datetime.datetime):
            time_format_error('[tuoniaox_news]', info.get('_id'), crawl_time)
        crawl_times.append(crawl_time)


    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[tuoniaox_news] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i,k,v in zip(object_ids, crawl_times, contents)]

    # check messages
    check_messages_update(messages, 'tuoniaox_news')


def get_cointime_news():
    # Raw Data Format
    # {
    #     "_id": ObjectId("5b5036f4ac1f2f7af51a0ba3"),
    #     "data": {
    #         "2018-07-18": [
    #             {
    #                 "content": "The closing value of the Garyscale’s Zcsah investment trust’s share on July 18th was of $21.16, 9.18% higher than the previous day. The market value has rose 9.18% in the last 24 hours, has dropped 14.69% in a month time, has dropped 9.84% in three months. Since the start of the Zcsah investment trust’s share has decreased 2.22%.",
    #                 "updated_at_yuan": "23:58",
    #             }
    #         ]
    #     },
    # }
    # get start_id
    last_message = db.cointime_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/cointime', last_id)

    # get crawl_time
    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.cointime_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    for info in infos:
        # get create times and contents
        print(info)

        for data in info.get("data").values():
                for d in data:
                    content = d.get("content")
                    if content:
                        object_ids.append(info.get('_id'))
                        contents.append(content)

                        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
                            content_error('[cointime]', info.get('_id'), content)


                        create_time = d.get('updated_at_yuan')
                        # timezoe to UTC
                        create_time = datetime.datetime.strptime(create_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=7)
                        create_times.append(create_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[cointime] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i, k, v in zip(object_ids, create_times, contents)]

    # check messages
    check_messages_update(messages, 'cointime')


def get_coinhills_news():
    # Raw Data Format
    # {
    #     "_id": ObjectId("5b1b843014432c706c4657c9"),
    #     "data": [
    #         {
    #             "content": "Leading crypto research firm, Chainalysis, revealed staggering wealth transfer numbers, as reported by legacy news outlet Financial Times, concerning bitcoin core (BTC). From its price high in December of last year through April of 2018, BTC hodlers (supposed longer term investors) dumped $30 billion onto the market, shedding about half their collective positions just in December.",
    #             "pubdate": 1528525941
    #         },
    #     ]
    # }
    # get start_id
    last_message = db.coinhills_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/coinhills', last_id)

    # get crawl_time
    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.coinhills_news.find({'_id': {'$gt': ObjectId(start_id)}})]
    print(infos)
    for info in infos:
        # get create times and contents
        print(info)

        for data in info.get("data"):
            content = data.get("content")
            if content:
                object_ids.append(info.get('_id'))
                contents.append(content)

                if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
                    content_error('[coinhills]', info.get('_id'), content)

                pub_timestamp = data.get('pubdate')
                # timezoe to UTC
                create_time =time2ISOString(pub_timestamp)
                create_times.append(create_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[coinhills] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i, k, v in zip(object_ids, create_times, contents)]
    print(messages)

    # check messages
    check_messages_update(messages, 'coinhills')


def get_coinspeaker_news():
    # Raw Data Format
    # {
    #     "_id": ObjectId("5b1b8dc614432c6560e96962"),
    #     "title": "Crypto Startup Circle in Talks With U.S. to Become Federally Licensed Bank",
    #     "description": "One of the most valuable cryptocurrency platforms is ready to become a ‘Guinea Pig’ for the U.S. regulators and break the ice between it and the crypto sphere.",
    #     "push_date": ISODate("2018-06-06T00:00:00.000Z")
    # }
    # get start_id
    last_message = db.coinspeaker_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/coinspeaker', last_id)

    # get crawl_time
    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.coinspeaker_news.find({'_id': {'$gt': ObjectId(start_id)}})]

    for info in infos:
        # get create times and contents
        print(info)
        object_ids.append(info.get('_id'))
        content = info.get("description")

        contents.append(content)

        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
            content_error('[coinspeaker]', info.get('_id'), content)

        create_time = info.get('push_date')
        create_times.append(create_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[coinspeaker] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i, k, v in zip(object_ids, create_times, contents)]
    print(messages)

    # check messages
    check_messages_update(messages, 'coinspeaker')



def get_coindesk_news():
    # Raw Data Format
    # {
    #     "_id": ObjectId("5b1b8dc614432c6560e9695a"),
    #     "title": "Chinese Search Giant Baidu Launches Blockchain-based Space Game Du Yuzhou",
    #     "description": "After announcing its “super chain”, the Chinese search giant Baidu decided to launch a new blockchain-based game called Du Yuzhou.",
    #     "push_date": "2018-07-19T09:30:25+00:00",
    # }
    # get start_id
    last_message = db.coindesk_news.find().sort('_id', DESCENDING)
    last_id = str(last_message[0].get("_id"))
    start_id = save_id('id/coindesk', last_id)

    # get crawl_time
    create_times = []
    contents = []
    object_ids = []

    infos = [a for a in db.coindesk_news.find({'_id': {'$gt': ObjectId(start_id)}})]

    for info in infos:
        # get create times and contents
        print(info)
        object_ids.append(info.get('_id'))
        content = info.get("description")

        contents.append(content)

        if not (re.search(r'[a-zA-Z]', content) or re.search(pattern_c, content)):
            content_error('[coindesk]', info.get('_id'), content)

        create_time = info.get('push_date')
        create_time = datetime.datetime.strptime(create_time, "%Y-%m-%dT%H:%M:%S+00:00")
        create_times.append(create_time)

    # print log
    if len(object_ids) > 0:
        first = sorted(object_ids)[0]
        last = sorted(object_ids)[-1]
        logging.info("[coindesk] check from {} to {}".format(str(first), str(last)))

    # combine datetime and content
    messages = [(i, k, v) for i, k, v in zip(object_ids, create_times, contents)]
    print(messages)

    # check messages
    check_messages_update(messages, 'coindesk')


def main_check_cn():
    # Chinese news source
    get_bishijie_news()
    get_jinse_news()
    get_gongxiangcj_news()
    get_jinniu_news()
    get_jgy_news()
    get_tuoniaox_news()



def main_check_en():
    # En
    get_cryptopanic_news()
    get_cointime_news()
    get_coinhills_news()
    get_coinspeaker_news()
    get_coindesk_news()

if __name__  == "__main__":
    main_check_cn()
    main_check_en()
    connection.close()
