import pytest
import storage
import conftest as cft
storage.file_size = 7

@pytest.mark.parametrize('df_input, df_issubset', [
    (cft.trades_10_data, True),
    (cft.trades_5_data, False)
])
def test_store_trades_as_parquet(df_input, df_issubset, mock_minio_storage):
    _, df = storage.store_trades_as_parquet(df_input, 'TEST', 'bucket')
    assert set(['symbol', 'day', 'month', 'year', 'hour']).issubset(df.columns) == df_issubset