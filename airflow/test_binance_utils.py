import binance_utils
import conftest as cft
import pytest

@pytest.mark.parametrize("get_trade_symbol, checkpoint, get_trade_result", [
    ("NODATA", 0, cft.trades_no_data),
    ("10DATA0CP", 0, cft.trades_10_data),
    ("10DATA5CP", 4, cft.trades_5_data),
])
def test_get_trade(get_trade_symbol, checkpoint, get_trade_result, mock_binance_api):
    res, cp = binance_utils.get_trade(get_trade_symbol, checkpoint)
    assert res == get_trade_result
    assert cp in range(10)

@pytest.mark.parametrize("worker, bags_len, bag_len, last_bag_len", [
    (10, 11, 10, 8),
    (5, 6, 21, 3),
    (4, 4, 27, 27)
])
def test_get_splitted_symbols(worker, bags_len, bag_len, last_bag_len):
    bags = binance_utils.get_splitted_symbols(cft.symbols_data, worker)
    assert len(bags) == bags_len
    for bag in bags[:-1]:
        assert len(bag) == bag_len
    assert len(bags[-1]) == last_bag_len

