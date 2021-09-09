from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
import _thread, time
import binance_utils, storage

tmp_dir = os.getenv('PATH')
bucket = 'crypto'

def get_trade(symbol):
    checkpoint = 0
    trades = []
    while True:
        checkpoint, data = binance_utils.get_trade(symbol, checkpoint=checkpoint)
        trades.append(data)
        result, _ = storage.store_trades_as_parquet(trades, symbol, bucket)
        if result.object_name:
            trades = []

def get_binance_trades(symbols):
    for symbol in symbols:
        time.sleep(2)
        _thread.start_new_thread(get_trade, (symbol,))
    while(True):
        time.sleep(1000)

args = {
    'owner': 'airflow'
}

with DAG(
    dag_id='binance_trades_crawler',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['datapipeline', 'binance', 'trades']
) as dag:
    symbol_bags = binance_utils.get_splitted_symbols(workers=10)
    for i, symbol_bag in enumerate(symbol_bags):
        task_get_binance_trades = PythonVirtualenvOperator(
            task_id="get_binance_trades_{}".format(i),
            python_callable=get_binance_trades,
            requirements=["requests==2.21.0", "pandas==1.3.2", "minio==7.1.0"],
            system_site_packages=False,
            op_kwargs={'symbols': symbol_bags},
        )