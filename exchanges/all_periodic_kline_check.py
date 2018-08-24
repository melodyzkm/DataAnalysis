import time
import datetime
import requests

'''
start_time:  从何时开始
sample_amount: 采样数量
'''
# d_cycles = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '12h': 43200, '1d': 86400}
d_cycles = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400}
BASE_URL = "http://192.168.4.188:8787/api/candle"

def trans_timestamp_to_local_time(time_stamp):
    return datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d %H:%M:%S")


def get_periodic_timestamp(cycle, start_time = None, sample_amount = None):
    if cycle not in d_cycles:
        raise ValueError("No such cycle", d_cycles.keys())

    cycle_seconds = d_cycles.get(cycle)

    # several seconds earlier
    end_time = int(time.time()) - cycle_seconds
    if start_time == None and sample_amount == None:
        raise  ValueError("Must specify one parameter")

    if start_time:
        # calculated with seconds
        start_time = int(start_time / cycle_seconds) * cycle_seconds
        return [time_minute for time_minute in range(start_time, end_time, cycle_seconds)]

    elif sample_amount:
        time_delta = sample_amount * cycle_seconds
        start_time = int(end_time / 60) * 60 - time_delta
        return [time_minute for time_minute in range(start_time, end_time, cycle_seconds)]


def get_kline_data(cycle, market, symbol):
    params = {"cycle": cycle, "market": market, "symbol": symbol}
    response = requests.get(BASE_URL, params=params).json()
    if not response or not response.get(market).get(symbol):
        print("No data with market:{} symbol:{}".format(market, symbol))
        return None
        # raise  ValueError("No data with market:{} symbol:{}".format(market, symbol))
    ohlcvs = response.get(market).get(symbol)

    l_d_timestamp_ohlcv_1m = []

    for single_kline_data in ohlcvs:
        ts = single_kline_data.get("time")/1000
        open = single_kline_data.get("open")
        close = single_kline_data.get("close")
        high = single_kline_data.get("high")
        low = single_kline_data.get("low")
        volume = single_kline_data.get("volume")
        ohlcv = [open, high, low, close, volume]
        l_d_timestamp_ohlcv_1m.append({ts:ohlcv})

    return l_d_timestamp_ohlcv_1m


def gen_kline_from_1m_lines(cycle, kline_data_1m):

    # [{timestamp1: [open, high, low, close, volume]}, {timestamp2: [open, high, low, close, volume]}]
    l_d_timestamp_ohlcv_1m = kline_data_1m
    print(l_d_timestamp_ohlcv_1m)
    start_time = list(l_d_timestamp_ohlcv_1m[0].keys())[0]

    periodic_timestamps = get_periodic_timestamp(cycle, start_time=start_time)
    l_d_timestamp_ohlcv_cycle = []

    for i in range(1, len(periodic_timestamps)-1):
        start = periodic_timestamps[i]
        end = periodic_timestamps[i+1]
        print(start_time, end)

        kline_duration_data = [i for i in l_d_timestamp_ohlcv_1m if i >= start and i < end]
        print(kline_duration_data)

        timestamp = list(kline_duration_data[0].keys())[0]
        open = list(kline_duration_data[0].values())[0][0]
        close = list(kline_duration_data[-1].values())[0][1]
        high = max([list(i.values())[0][3] for i in kline_duration_data])
        low = min([list(i.values())[0][2] for i in kline_duration_data])
        volume = sum([list(i.values())[0][4] for i in kline_duration_data])

        l_d_timestamp_ohlcv_cycle.append({timestamp: [open, close, low, high, volume]})

    return l_d_timestamp_ohlcv_cycle


def compare(kline_data_from_api, kline_data_from_merge):
    # print("kline_data_from_api: {}".format(kline_data_from_api))
    # print("kline_data_from_merge: {}".format(kline_data_from_merge))

    common_start_time = max(list(kline_data_from_api[0].keys())[0], list(kline_data_from_merge[0].keys())[0])
    common_end_time = min(list(kline_data_from_api[-1].keys())[0], list(kline_data_from_merge[-1].keys())[0])

    common_api_data = [i for i in kline_data_from_api if list(i.keys())[0] >= common_start_time and list(i.keys())[0] < common_end_time]
    common_merge_data = [i for i in kline_data_from_merge if list(i.keys())[0] >= common_start_time and list(i.keys())[0] < common_end_time]

    print("start")
    print(common_api_data[0])
    print(common_merge_data[0])

    print("end")
    print(common_api_data[-1])
    print(common_merge_data[-1])

    if len(common_api_data) != len(common_merge_data):
        timestamps_api = [list(i.keys())[0] for i in common_api_data]
        timestamps_merge = [list(i.keys())[0] for i in common_merge_data]

        merge_api = [trans_timestamp_to_local_time(i) for i in timestamps_merge if i not in timestamps_api]
        api_merge = [trans_timestamp_to_local_time(i) for i in timestamps_api if i not in timestamps_merge]

        merge_api_str = ','.join([str(i) for i in merge_api])
        api_merge_str = ','.join([str(i) for i in api_merge])


        print("These time should have k line: {} {}These time should not have k line:{}".format(merge_api_str, '\n',api_merge_str ))
        return None


    prices_volume_names = {0: "open", 1:"close", 2:"low", 3:"high", 4:"volume"}
    for index in range(len(common_api_data)):
        if common_api_data[index] != common_merge_data[index]:
            timestamp = list(common_api_data[index].keys())[0]
            l_ohlcv_api = list(common_api_data[index].values())[0]
            l_ohlcv_merge = list(common_merge_data[index].values())[0]
            for i in range(len(l_ohlcv_api)-1):
                if l_ohlcv_api[i] != l_ohlcv_merge[i]:
                    print("{} : wrong {}! Currently: {}, should be :{}".format(trans_timestamp_to_local_time(timestamp), prices_volume_names.get(i), l_ohlcv_api[i],l_ohlcv_merge[i]))

            index_volume = len(l_ohlcv_api) - 1
            if l_ohlcv_api[index_volume] < 0.98 * l_ohlcv_merge[index_volume] or l_ohlcv_api[index_volume] > 1.02 * l_ohlcv_merge[index_volume]:
                print("{} : wrong {}! Currently: {}, should be :{}".format(trans_timestamp_to_local_time(timestamp),
                                                                           prices_volume_names.get(index_volume), l_ohlcv_api[index_volume],
                                                                           l_ohlcv_merge[index_volume]))
            else:
                print("{} OK".format(list(common_api_data[index].keys())[0]) )

        else:
            print("{} OK".format(list(common_api_data[index].keys())[0]))


def check_kline_data(cycle, market, symbol):
    params = {"cycle": cycle, "market": market, "symbol": symbol}
    response = requests.get(BASE_URL, params=params)
    print(response.url)
    response = response.json()

    if not response:
        return False
    elif not response.get(market):
        return  False
    elif not response.get(market).get(symbol):
        return False
    else:
        return True


if __name__ == "__main__":
    # print(get_periodic_timestamp("1h",  sample_amount= 100))
    # huobipro_btc_usdt_1m = get_kline_data('1m', 'huobipro', 'btc-usdt')
    # huobipro_btc_usdt_5m = get_kline_data('5m', 'huobipro', 'btc-usdt')
    # huobipro_btc_usdt_5m_merger = gen_kline_from_1m_lines('5m', huobipro_btc_usdt_1m)
    # compare(huobipro_btc_usdt_5m, huobipro_btc_usdt_5m_merger)

    #
    # market = "binance"
    # symbol = "btc-usdt"
    # for i in d_cycles:
    #     print(i)
    #     compare(get_kline_data(i, market, symbol), gen_kline_from_1m_lines(i, get_kline_data('1m', market, symbol)))


    # 交易所数据
    d = {"binance": "btc-usdt", "huobipro": "btc-usdt", "okex": "btc-usdt", "bitfinex": "btc-usd", "bibox": "btc-usdt",
         "bitbank": "btc-jpy", "lbank": "btc-usdt", "zb": "btc-qc", "hitbtc": "btc-usd", "bitz": "eth-dkkt",
         "gateio": "btc-usdt", "gdax": "btc-usd", "poloniex": "btc-usdt", "kraken": "btc-eur", "bcex": "eth-ckusd",
         "upbit": "btc-usdt", "bitflyer": "btc-jpy", "bithumb": "btc-krw", "bitstamp": "btc-usd", "bittrex": "btc-usdt",
         "coinbene": "btc-usdt", "exx": "btc-usdt"}

    markets = ["bcex", "bibox","binance", "bitfinex", "bittrex", "gdax", "hitbtc", "huobipro", "kraken", "lbank", "okex", "upbit"]

    markets_with_data = []
    markets_without_data = []
    for market in markets:
        symbol = d.get(market)
        if check_kline_data('1m', market, symbol):
            markets_with_data.append(market)
        else:
            markets_without_data.append(market)

    print(markets_with_data)
    print(markets_without_data)