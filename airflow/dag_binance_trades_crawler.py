from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
import binance_utils
import json

WORKER=5
binance_api_price = '/api/v3/ticker/price'
binance_api_trades = '/api/v3/trades'

args = {
    'owner': 'airflow'
}

dag = DAG(
    dag_id='binance_trades_crawler',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['datapipeline', 'binance', 'trades']
)

get_symbols = SimpleHttpOperator(
    task_id='get_binance_symbols',
    http_conn_id='binance_api',
    endpoint=binance_api_price,
    method='GET',
    response_filter=lambda response, worker=WORKER: binance_utils.get_splitted_symbols(json.loads(response.text), worker),
    dag=dag
)

def get_binance_trades(**kwargs):
    import binance_utils
    from airflow.models import Variable
    import redis, json
    r = redis.Redis(
        host=Variable.get('redis_host'),
        port=int(Variable.get('redis_port')),
        password=Variable.get('redis_pass'),
        db=int(Variable.get('redis_db')),
        decode_responses=True
    )
    symbols = kwargs['symbols']

    print('get trades by symbols {}'.format(symbols))
    for symbol in json.loads(symbols.replace("'","\"")):
        checkpoint = int(r.get(symbol)) if r.get(symbol) is not None else 0
        data, checkpoint = binance_utils.get_trade(symbol.strip(), checkpoint)
        r.set(symbol, checkpoint)




for i in range(WORKER+1):
    _task = PythonVirtualenvOperator(
            task_id='get_binance_trades_{}'.format(i),
            python_callable=get_binance_trades,
            requirements=["requests==2.21.0", "redis==3.5.3"],
            system_site_packages=False,
            provide_context=True,
            op_kwargs={'symbols': "{{ ti.xcom_pull(task_ids='get_binance_symbols')[" + str(i) + "] }}"},
            dag=dag
        )
    get_symbols >> _task
