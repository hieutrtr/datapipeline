import requests as rq
import utils
binance_host = 'https://api.binance.com'
binance_api_price = '/api/v3/ticker/price'
binance_api_trades = '/api/v3/trades'

def _check_error(res):
    if res != dict or res['code'] is None:
        pass
    elif res['code'] in range(1000,1100):
        raise Exception({'error': 'Binance Server/Network Error', 'type': 'server'})
    elif res['code'] in range(1100,3000):
        raise Exception({'error': 'Request error', 'type': 'client'})


# calling binance REST api to get trades by symbol
# return trades with id that greater than checkpoint
# return largest/latest id as new checkpoint
# handle error of status 400 from binance get trades endpoint

def get_trade(symbol, check_point):
    res = rq.get("{}{}?symbol={}".format(binance_host, binance_api_trades, symbol))
    res = res.json()
    utils.log(res)
    _check_error(res)

    if len(res) == 0:
        return res, 0
    elif check_point == 0:
        return res, res[-1]['id']
    else:
        return res[:check_point:-1], res[-1]['id']


# fetch all symbols.
# separate symbols for each worker
# return array of symbols bag
def get_splitted_symbols(prices, worker):
    n = len(prices)
    size = int(n/worker)
    bags = []
    bag = []
    for p in prices:
        if len(bag) == size:
            bags.append(bag)
            bag = []
        bag.append(p['symbol'])

    bags.append(bag)
    return bags
