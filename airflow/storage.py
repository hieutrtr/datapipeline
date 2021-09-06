import pandas as pd
import os
from io import BytesIO
from minio import Minio


STORAGE_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
STORAGE_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
STORAGE_DOMAIN = os.getenv('STORAGE_DOMAIN')
binance_trade_dir = 'binance/trades'

minioClient = Minio(STORAGE_DOMAIN,
                    access_key=STORAGE_ACCESS_KEY_ID,
                    secret_key=STORAGE_SECRET_ACCESS_KEY,
                    secure=False)

def store_trades_as_parquet(data, symbol, bucket):
    partition_cols = ['symbol', 'year', 'month', 'day', 'hour']
    df = pd.json_normalize(data)
    df['symbol'] = symbol
    df['date'] = pd.to_datetime(df['time'],unit='ms')
    df[['day', 'month', 'year', 'hour']] = df.date.apply(lambda x: (x.day, x.month, x.year, x.hour)).tolist()
    print(df.head())
    print(df.dtypes)
    df_bytes = df.to_parquet(partition_cols=partition_cols).encode('utf-8')
    df_buffer = BytesIO(df_bytes)
    result = minioClient.put_object(bucket,
                           binance_trade_dir,
                           data=df_buffer,
                           length=len(df_bytes))
    if result.object_name:
        return True
    return False