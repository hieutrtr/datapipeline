import requests as rq
import pandas as pd
from sys import argv
import time
import _thread
binance_host = 'https://api.binance.com'
binance_api_price = '/api/v3/ticker/price'
binance_api_trades = '/api/v3/trades'
binance_path_root = '/root/data/pipeline/binance'
binance_path_symbols_price = '/symbols/price'
binance_path_symbols_trades = '/symbols/trades'
file_record_limit = 1000
def store_trades_as_parquet(data, symbol):
    partition_cols = ['symbol', 'year', 'month', 'day', 'hour']
    df = pd.json_normalize(data)
    df['symbol'] = symbol
    df['date'] = pd.to_datetime(df['time'],unit='ms')
    df[['day', 'month', 'year', 'hour']] = df.date.apply(lambda x: (x.day, x.month, x.year, x.hour)).tolist()
    print(df.head())
    print(df.dtypes)
    df.to_parquet("{}{}".format(binance_path_root, binance_path_symbols_trades), partition_cols=partition_cols)

def get_trade(symbol):
    largest_trade_ids = 0
    trade_data = []
    while True:
        time.sleep(60)
        try:
            res = rq.get("{}{}?symbol={}".format(binance_host, binance_api_trades, symbol))
            trades = res.json()
            if largest_trade_ids != 0:
                for trade in trades[::-1]:
                    if trade['id'] == largest_trade_ids:
                        break
                    trade_data.append(trade)
                if trades[-1]['id'] > largest_trade_ids:
                    largest_trade_ids = trades[-1]['id']
            else:
                trade_data = trades
                largest_trade_ids = trades[-1]['id']
        except Exception as e:
            print('Exception: {}'.format(e))

        if len(trade_data) >= file_record_limit:
            store_trades_as_parquet(trade_data, symbol)
            trade_data = []

def crawl_trades():
    prices = rq.get("{}{}".format(binance_host, binance_api_price))
    print('number of symbols: {}'.format(len(prices.json())))
    for price in prices.json():
        time.sleep(2)
        _thread.start_new_thread(get_trade, (price['symbol'],))
    while(True):
        time.sleep(1000)

def crawl_symbols():
    res = rq.get("{}{}".format(binance_host, binance_api_price))
    # print("crawl_symbols {}".format(res.json()))
    df = pd.json_normalize(res.json())
    ts = pd.Timestamp.utcnow()
    df['timestamp'] = ts
    df['day'] = ts.day
    df['month'] = ts.month
    df['year'] = ts.year
    df['hour'] = ts.hour
    df.to_parquet("{}{}".format(binance_path_root, binance_path_symbols_price), partition_cols=['year', 'month', 'day', 'hour'])

if __name__ == '__main__':
    type = argv[1].strip()
    print("crawling {}".format(type))
    res = {
        "symbols": crawl_symbols,
        "trades": crawl_trades
    }[type]()
    print("result: {}".format(res))