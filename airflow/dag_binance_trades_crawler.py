from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.utils.dates import days_ago
import binance_utils
import json
from datetime import datetime, timedelta

WORKER=5
binance_api_price = '/api/v3/ticker/price'
binance_api_trades = '/api/v3/trades'

args = {
    'owner': 'airflow'
}

dag = DAG(
    dag_id='binance_trades_crawler_v10',
    default_args=args,
    schedule_interval=timedelta(minutes=5),
    start_date=datetime.utcnow() - timedelta(minutes=5),
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
    from kafka_utils import KafkaRest
    from airflow.models import Variable
    import redis, json

    kafka_conf = {
        'rest_host': Variable.get('kafka_rest_host'),
        'schemaregistry_host': Variable.get('kafka_schemaregistry_host'),
        'schemaregistry_user': Variable.get('kafka_schemaregistry_user'),
        'schemaregistry_pass': Variable.get('kafka_schemaregistry_pass'),
        'binance_trade_schema_id': Variable.get('kafka_binance_trade_schema_id'),
        'binance_trade_key_schema_id': Variable.get('kafka_binance_trade_key_schema_id'),
    }

    # separate schema module
    binance_trade_schema = {
        "type": "record",
        "namespace": "io.hieu.crypto",
        "name": "binance_trade",
        "fields":
        [
            {
                "type": "int",
                "name": "id"
            },
            {
                "type": "string",
                "name": "time"
            },
            {
                "type": "string",
                "name": "price"
            },
            {
                "type": "string",
                "name": "qty"
            },
            {
                "type": "string",
                "name": "quoteQty"
            },
            {
                "type": "boolean",
                "name": "isBuyerMaker"
            },
            {
                "type": "boolean",
                "name": "isBestMatch"
            },
            {
                "type": "string",
                "name": "symbol"
            }
        ]
    }

    binance_trade_key_schema = {
        "type": "record",
        "namespace": "io.hieu.crypto",
        "name": "binance_trade",
        "fields":
        [
            {
                "type": "string",
                "name": "symbol"
            }
        ]
    }

    r = redis.Redis(
        host=Variable.get('redis_host'),
        port=int(Variable.get('redis_port')),
        password=Variable.get('redis_pass'),
        db=int(Variable.get('redis_db')),
        decode_responses=True
    )
    symbols = kwargs['symbols']
    kafka_rest = KafkaRest(kafka_conf)
    print('get trades by symbols {}'.format(symbols))
    for symbol in json.loads(symbols.replace("'","\"")):
        checkpoint = int(r.get(symbol)) if r.get(symbol) is not None else 0
        trades, checkpoint = binance_utils.get_trade(symbol.strip(), checkpoint)
        if len(trades) > 0:
            kafka_rest.produce('binance-trade', symbol, trades)
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
