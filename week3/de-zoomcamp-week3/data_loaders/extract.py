import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    dfs = []
    for df in download_data():
        print(len(df), 'rows are loaded')
        dfs.append(df)
    return pd.concat(dfs)


def download_data():
    for i in range(1, 13):
        month = str(i) if i > 9 else '0' + str(i)
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{month}.parquet'
        print(f'download started for month {i}')
        df = pd.read_parquet(url)
        print(f'download finished for month {i}')
        yield df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
