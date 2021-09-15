import pandas as pd
import os
from io import BytesIO
import minio


STORAGE_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
STORAGE_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
STORAGE_DOMAIN = os.getenv('STORAGE_DOMAIN')
binance_trade_dir = 'binance/trades'
file_size = 1000

def store_trades_as_parquet(data, symbol, root_path):
    if len(data) < file_size:
        return pd.DataFrame({'A' : []})
    df = pd.json_normalize(data)
    df['symbol'] = symbol
    df['date'] = pd.to_datetime(df['time'],unit='ms')
    df[['day', 'month', 'year', 'hour']] = df.date.apply(lambda x: (x.day, x.month, x.year, x.hour)).tolist()
    print(df.head())
    print(df.dtypes)
    df.to_parquet(path="{}/{}".format(root_path, binance_trade_dir), partition_cols=['symbol', 'year', 'month', 'day'])
    return df