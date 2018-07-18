import re
from urllib.request import urlopen, Request

def aaa():
    pass

class Binance():
    def __init__(self, symbols):
        self.symbols = symbols.replace('-', '').upper()

    def get_kline_data(self, cycle):
        d_ohlcvs = {}

        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36r)'
        headers_fake = {'User-Agent': user_agent}
        if cycle in ['1m', '5m', '15m', '30m', '1h', '4h', '12h','1d']:
            api_kline = 'https://www.binance.com/api/v1/klines?symbol={}&interval={}'.format(self.symbols, cycle)
            print(api_kline)
            req = Request(api_kline, headers=headers_fake)
            content = urlopen(req).read().decode('utf-8')
            # print(content)
            # print(type(content))

            if content:
                ohlcvs = re.findall(r'\[(.*?)\]', content[1:-1])

                for ohlcv in ohlcvs:
                    time, open, high, low, close, volume = ohlcv.split(',')[:6]
                    time = int(time[:-3])
                    open = float(open.replace('"', ''))
                    high = float(high.replace('"', ''))
                    low = float(low.replace('"', ''))
                    close = float(close.replace('"', ''))
                    volume = float(volume.replace('"', ''))

                    d_ohlcvs.update({time: [open, high, low, close, volume]})
                # print(d_ohlcvs)
                return d_ohlcvs

            else:
                return None
        else:
            raise ValueError("No such cycle")

class Okex():
    def __init__(self, symbol):
        self.symbol = symbol.replace('-', '_')

    def get_k_line_data(self, cycle):
        d_ohlcv = {}

        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36r)'
        headers_fake = {'User-Agent': user_agent}

        if cycle in ['1m', '5m', '15m', '30m', '1h', '4h', '12h', '1d']:
            api_kline = "https://www.okex.com/v2/spot/markets/kline?since=0&symbol={}&marketFrom=zil_btc&type=1min" \
                        "&limit=1000&coinVol=0"

if __name__ == '__main__':
    b = Binance("btc-usdt")
    b.get_kline_data('1m')

