import pandas as pd
from prefect import flow, task

from utils.os_helpers import download_if_not_exists, unzip


@flow(name='ingest-github-archive')
def etl(year: int, month: int, day: int, hour: int):
    data = extract(year, month, day, hour)
    data = transform(data)
    load(data)


@task(retries=1, cache_result_in_memory=True, log_prints=True)
def extract(year: int, month: int, day: int, hour: int) -> pd.DataFrame:
    download_path = get_download_path(year, month, day, hour)

    print('start downloading data')
    file_path = download_if_not_exists(download_path)
    print('download finished')

    print('start unzipping data')
    file_path = unzip(file_path)
    print('unzipping finished')

    print('start converting to dataframe')
    df = pd.read_json(file_path, lines=True)
    print('dataframe created')

    return df


@task()
def transform(data: pd.DataFrame) -> pd.DataFrame:
    return data


@task(retries=3)
def load(data: pd.DataFrame):
    print(data.head())


def get_download_path(year: int, month: int, day: int, hour: int):
    month = get_str_from_int(month)
    day = get_str_from_int(day)
    download_path = f"https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"
    return download_path


def get_str_from_int(number: int):
    if int(number) < 10:
        return '0' + str(number)

    return str(number)
