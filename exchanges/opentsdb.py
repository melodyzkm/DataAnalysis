import random
import sys

import requests
from exchanges import Binance

sys.path.append("..")
from coins.common import *

config = get_config()
BC_MONGODB = config.get("BC_MONGODB")
PORT_MONGODB= config.get("PORT_MONGODB")
connection = MongoClient(BC_MONGODB, PORT_MONGODB)
db_bcf = connection.bcf

markets = ['binance','huobipro', 'okex', 'lbank', 'zb', 'bitstamp']
cycles = ['1m', '5m', '15m', '30m', '1h', '4h', '12h','1d']
props = ['open', 'high', 'low', 'close', 'volume']


# 从tsdb中取最近的数据数量
SAMPLE_AMOUNT = 300
# 取随机的交易对验证的数量
COUNT_RANDOM_SAMPLE = 15


def get_markets_symbols():
    d_markets_symbols = {}
    for market in markets:
        market_infos = db_bcf.market_infos.find_one({"market": market})
        d_markets_symbols.update({market: market_infos.get('symbols')})

    with open('markets_symbols.json', 'wt', encoding='utf-8') as f:
        f.write(json.dumps(d_markets_symbols))

    return d_markets_symbols


def get_result_by_post(query):
    api = 'http://192.168.4.168:4242/api/query'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36r)'
    headers_fake = {'User-Agent': user_agent}
    req = requests.post(api, query, headers=headers_fake)
    resp = req.content.decode('utf-8').strip()

    if resp.startswith('[') and resp.endswith(']') and len(resp) > 2:
        return json.loads(resp[1:-1])
    else:
        return {}

def concat_query(market, symbol, cycle, prop, start):
    '''
    :param market:
    :param symbol:
    :param cycle:
    :param prop:
    :param start:
    :param end:  endtime is not necessary
    :return:
    '''
    query = {
	"start": start,
	"queries": [{
		"aggregator": "avg",
		"metric": "market.candle",
		"tags": {
			"market": market,
			"symbol": symbol,
			"cycle": cycle,
			"prop": prop
		}
	}]
}
    # type: "byte"
    # print(query)
    return json.dumps(query).encode()


def check_market_data_update(market, cycle, log_path):

    symbols_result = {}

    periodic_timestamps = get_periodic_timestamp(cycle, SAMPLE_AMOUNT)
    start = periodic_timestamps[0]
    end = periodic_timestamps[-1]

    markets_symbols = get_markets_symbols()
    # markets_symbols = {'huobipro':['btc-usdt', 'eth-usd']}

    symbols = markets_symbols.get(market)
    len_symbols = len(symbols)
    symbols_without_data = []
    symbols_error = []
    symbols_other_error = []
    symbols_not_update_timely = []
    for count, symbol in enumerate(symbols):
        query_close = concat_query(market, symbol, cycle, props[3],start)
        response_close = get_result_by_post(query_close)

        # check response data
        if response_close:
            d_response = response_close
            if 'error' in response_close:
                symbols_error.append(symbol)
            # 'dps' is the list name of the returned data
            elif 'dps' in d_response:
                dps = d_response.get('dps')
                if len(dps) < 1:
                    symbols_other_error.append(symbol)
                # if update timely
                else:
                    last_time = int(sorted(list(dps.keys()))[-1])
                    if last_time < end:
                        symbols_not_update_timely.append(symbol)
            else:
                symbols_other_error.append(symbol)
                print(d_response)
        else:
            symbols_without_data.append(symbol)

        print("{}/{} {} {} update check done".format(count+1,len_symbols, market, symbol))

    symbols_result.update({"symbols_not_update_timely": symbols_not_update_timely,"error": symbols_error,
                           "without_data": symbols_without_data, "other error": symbols_other_error})


    with open(log_path, 'wt') as f:
        f.write(reverse_dict_to_json_style(symbols_result))

    return  symbols_result


def combine_dicts_with_one_group_of_key(l_common_keys, *args):
    d_result = {}
    for common_key in l_common_keys:
        common_values = []
        for d in args:
            if common_key in d:
                common_values.append(d[common_key])
            else:
                # if no such time
                common_values.append(-1)
        d_result.update({int(common_key):common_values})
    return d_result


def round_lists(l):
    ll = []
    for i in l:
        if i >= 0.01:
            ll.append(round(i, 2))
        else:
            ll.append(round(i, 8))
    return  ll

def check_post_values(market, cycle, log_path):
    '''
    Format : symbols_result = {market: ['btc-usdt']}
    :param market:
    :param cycle:
    :param log_path:
    :return:
    '''

    market_symbol_file = 'markets_symbols.json'
    if not os.path.exists(market_symbol_file):
        raise ValueError("no ", market_symbol_file)

    with open(market_symbol_file, 'rt', encoding='utf-8') as f:
        d_markets_symbols = json.loads(f.read())
    split_times = get_periodic_timestamp(cycle, SAMPLE_AMOUNT)
    split_times_str = [str(i) for i in split_times]

    start = split_times[0]
    end = split_times[-1]

    d_position_ohlcvs = {0: 'open', 1: 'high', 2: 'low', 3: 'close', 4: 'volume'}

    d_wrong_prices = {}
    d_items_lost = {}
    d_some_item_lost = {}
    d_diff_from_binance = {}
    d_no_update = {}

    #random 10 symbols
    symbols = d_markets_symbols.get(market)
    symbols_random = random.sample(symbols, COUNT_RANDOM_SAMPLE)

    for count, symbol in enumerate(symbols_random):
        print("{}/{} {} value check started.".format(count + 1, COUNT_RANDOM_SAMPLE, symbol))

        # get binance kline data
        if market == 'binance':
            binance = Binance(symbol)
            binance_ohlcvs = binance.get_kline_data(cycle)

        l_wrong_prices = []
        l_items_lost = []
        l_some_item_lost = []
        l_diff_from_binance = []

        query_open = concat_query(market, symbol, cycle, props[0], start)
        query_high = concat_query(market, symbol, cycle, props[1], start)
        query_low = concat_query(market, symbol, cycle, props[2], start)
        query_close = concat_query(market, symbol, cycle, props[3], start)
        query_volume = concat_query(market, symbol, cycle, props[4], start)

        response_open = get_result_by_post(query_open).get("dps")
        response_high = get_result_by_post(query_high).get("dps")
        response_low = get_result_by_post(query_low).get("dps")
        response_close = get_result_by_post(query_close).get("dps")
        response_volume = get_result_by_post(query_volume).get("dps")

        # print(response_open)
        # print(response_high)
        # print(response_low)
        # print(response_close)
        # print(response_volume)

        if response_open and response_high and response_low and response_close and response_volume:
            d_ohlcvs = combine_dicts_with_one_group_of_key\
                (split_times_str, response_open, response_high,response_low, response_close, response_volume)
            # print(d_ohlcvs)
            for key_time in d_ohlcvs:
                open_, high, low, close, volume = d_ohlcvs.get(key_time)

                # price is not right
                if max(open_, high, low, close) != high or min(open_, high, low, close) != low:
                    l_wrong_prices.append({key_time: [open_, high, low, close]})

                # no data in this time
                if max(open_, high, low, close, volume) == -1:
                    l_items_lost.append(key_time)

                # some item is lost
                if max(open_, high, low, close, volume) != -1 and min(open_, high, low, close, volume) == -1:
                    l_some_item_lost.append(l_some_item_lost)

                # compared with binance
                if min(open_, high, low, close, volume) != -1:
                    if "market" == "binance":
                        for key_time in binance_ohlcvs:
                            l_diff = []
                            round_ohlcvs = round_lists(d_ohlcvs.get(key_time))
                            round_binance_ohlcvs = round_lists(binance_ohlcvs.get(key_time))

                            if round_ohlcvs != round_binance_ohlcvs:
                                for flag, values in enumerate(zip(round_ohlcvs, round_binance_ohlcvs)):
                                    if values[0] != values[1]:
                                        l_diff.append({d_position_ohlcvs.get(flag): (values[0], values[1])})
                            if l_diff:
                                l_diff_from_binance.append({key_time: l_diff})
        else:
            d_no_update.update({"{}.{}".format(market, symbol) : "{} - {}".format(start, end)})


        if l_wrong_prices:
            d_wrong_prices.update({symbol: l_wrong_prices})
        # if l_items_lost:
        #     d_items_lost.update({symbol: l_items_lost})
        if l_some_item_lost:
            d_some_item_lost.update({symbol: l_some_item_lost})

        if market == 'binance':
            if l_diff_from_binance:
                d_diff_from_binance.update({symbol: l_diff_from_binance})



    if market == 'binance':
        d_result = {"wrong prices": d_wrong_prices, "items_lost": d_items_lost,
                            "some item lost": d_some_item_lost,"different from binance": d_diff_from_binance,
                            "no response": d_no_update}
    else:
        d_result = {"wrong prices": d_wrong_prices, "items_lost": d_items_lost,
                            "some item lost": d_some_item_lost, "no response": d_no_update}

    with open(log_path, 'wt') as f:
        f.write(json.dumps(d_result))


def cal_avg_in_triangle_stdev(values):
    values_suspicous = []
    avg = get_avg(values)
    stdev = get_stdev(values)
    # minimum = avg - 5 * stdev
    minimum = 0
    maximum = avg + 3 * stdev
    # print("min:{}, max: {}".format(minimum, maximum))

    for value in values:
        # if value <= minimum or value > maximum:
        if value <= minimum :
            values_suspicous.append(value)
    return values_suspicous


def get_periodic_timestamp(cycle, sample_amount):

    d_cycles = {'1m': 60, '5m': 300, '15m': 600, '30m': 1800, '1h': 3600, '4h': 14400, '12h':43200, '1d': 86400}
    if cycle not in d_cycles:
        raise ValueError("No such cycle", d_cycles.keys())

    # calculated with seconds
    time_delta = sample_amount * d_cycles.get(cycle)

    # 5 seconds earlier
    end_time = int(time.time()) - 5
    start_time = int(end_time/60) * 60  - time_delta

    return [time_minute for time_minute in range(start_time, end_time, d_cycles.get(cycle))]


def update_check():
    time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    d_markets_symbols = get_markets_symbols()

    for cycle in cycles:
        for market in d_markets_symbols:
            log = "{}_{}_{}_symbols_update".format(market, cycle, time)
            check_market_data_update(market, cycle, os.path.join(log_path, log))


def value_check():
    time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    with open('markets_symbols.json', 'rt') as f:
        d_markets_symbols = json.loads(f.read())

        for market in d_markets_symbols:
            for cycle in cycles:
                log = "{}_{}_{}_symbols_values".format(market, cycle, time)
                check_post_values(market, cycle, os.path.join(log_path, log))


def debug():
    time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    markets = ['binance']
    cycles = ['4h', '12h']
    check_market_data_update('1m', 'symbols_result.json')
    # for market in markets:
    #     for cycle in cycles:
    #         log_path = "{}_{}_{}_symbols_values".format(time, market,cycle)
    #         check_post_values(market, cycle, log_path)
    pass



if __name__ == "__main__":

    # for market in markets:
    #     check_post_values(market, '1m')

    # check_market_data_1m_update()
    # debug()
    # print(round_lists([1.11111, 0.00002999999]))

    update_check()
