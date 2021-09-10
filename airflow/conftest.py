import pytest
import requests
import minio
import binance_utils
import pandas as pd

class MockTradesResponse:
    def __init__(self, url):
        self.url = url

    def json(self):
        if self.url.endswith('NODATA'):
            return trades_no_data
        elif self.url.endswith('10DATA0CP'):
            return trades_10_data
        elif self.url.endswith('10DATA5CP'):
            return trades_10_data


class MockPricesResponse:
    def __init__(self, url):
        self.url = url

    def json(self):
        return symbols_data


@pytest.fixture(autouse=True)
def mock_binance_api(monkeypatch):
    def mock_get_trade(url):
        if binance_utils.binance_api_price in url:
            return MockPricesResponse(url)
        elif binance_utils.binance_api_trades in url:
            return MockTradesResponse(url)
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: mock_get_trade(args[0]))

trades_no_data = []
trades_10_data = [
    {
        "id": 0,
        "price": "0.00976300",
        "qty": "0.29000000",
        "quoteQty": "0.00283127",
        "time": 1630923495816,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 1,
        "price": "0.00976500",
        "qty": "0.42000000",
        "quoteQty": "0.00410130",
        "time": 1630923495820,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 2,
        "price": "0.00976500",
        "qty": "0.32100000",
        "quoteQty": "0.00313456",
        "time": 1630923495820,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 3,
        "price": "0.00976500",
        "qty": "3.00000000",
        "quoteQty": "0.02929500",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 4,
        "price": "0.00976500",
        "qty": "1.32700000",
        "quoteQty": "0.01295815",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 5,
        "price": "0.00976500",
        "qty": "0.47400000",
        "quoteQty": "0.00462861",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 6,
        "price": "0.00976500",
        "qty": "0.06200000",
        "quoteQty": "0.00060543",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 7,
        "price": "0.00976500",
        "qty": "0.46400000",
        "quoteQty": "0.00453096",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 8,
        "price": "0.00976500",
        "qty": "2.67500000",
        "quoteQty": "0.02612137",
        "time": 1630923495824,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 9,
        "price": "0.00976500",
        "qty": "1.98300000",
        "quoteQty": "0.01936399",
        "time": 1630923495825,
        "isBuyerMaker": False,
        "isBestMatch": True
    }
]
trades_5_data = [
    {
        "id": 9,
        "price": "0.00976500",
        "qty": "1.98300000",
        "quoteQty": "0.01936399",
        "time": 1630923495825,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 8,
        "price": "0.00976500",
        "qty": "2.67500000",
        "quoteQty": "0.02612137",
        "time": 1630923495824,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 7,
        "price": "0.00976500",
        "qty": "0.46400000",
        "quoteQty": "0.00453096",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 6,
        "price": "0.00976500",
        "qty": "0.06200000",
        "quoteQty": "0.00060543",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    },
    {
        "id": 5,
        "price": "0.00976500",
        "qty": "0.47400000",
        "quoteQty": "0.00462861",
        "time": 1630923495823,
        "isBuyerMaker": False,
        "isBestMatch": True
    }
]
symbols_data = [
    {'symbol': 'S{}'.format(str(symbol))} for symbol in range(108)
]

