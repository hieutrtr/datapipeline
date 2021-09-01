import requests as rq
import pandas as pd
from sys import argv
import time
binance_host = 'https://api.binance.com'
binance_api_price = '/api/v3/ticker/price'
binance_api_trades = '/api/v3/trades'
binance_path_root = '/root/data/pipeline/binance'
binance_path_symbols_price = '/symbols/price'
binance_path_symbols_trades = '/symbols/trades'
file_record_limit = 500
def store_trades_as_parquet(data, symbol):
    partition_cols = ['symbol', 'year', 'month', 'day', 'hour']
    df = pd.json_normalize(data)
    df['symbol'] = symbol
    df['date'] = pd.to_datetime(df['time'],unit='ms')
    df[['day', 'month', 'year', 'hour']] = df.date.apply(lambda x: (x.day, x.month, x.year, x.hour)).tolist()
    print(df.head())
    print(df.dtypes)
    df.to_parquet("{}{}".format(binance_path_root, binance_path_symbols_trades), partition_cols=partition_cols)


def crawl_trades():
    prices = rq.get("{}{}".format(binance_host, binance_api_price))
    largest_trade_ids = {}
    trades_data = {}
    while(True):
        time.sleep(10)
        for price in prices.json():
            try:
                symbol = price['symbol']
                res = rq.get("{}{}?symbol={}".format(binance_host, binance_api_trades, symbol))
                trades = res.json()
                if largest_trade_ids.get(symbol, 0) != 0:
                    for trade in trades[::-1]:
                        if trade['id'] == largest_trade_ids.get(symbol, 0):
                            break
                        trades_data.get(symbol, []).append(trade)
                    if trades[-1]['id'] > largest_trade_ids.get(symbol, 0):
                        largest_trade_ids[symbol] = trades[-1]['id']
                else:
                    trades_data[symbol] = trades
                    largest_trade_ids[symbol] = trades[-1]['id']
            except Exception as e:
                print('Exception: {}'.format(e))
        for symbol in trades_data.keys():
            if len(trades_data[symbol]) >= file_record_limit:
                store_trades_as_parquet(trades_data[symbol], symbol)

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