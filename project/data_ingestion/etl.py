import argparse
from typing import Iterator
import pandas as pd
from prefect import flow, task

from project.utils.os_helpers import download_if_not_exists, unzip
from project.utils.loggers import print_logger

from project.database.PostgresDB import PostgresDB


@flow(name='ingest-github-archive')
@print_logger
def etl(year: int, month: int, day: int, hour: int):
    file_path = extract(year, month, day, hour)

    chunk_size = 5000
    data_iter = pd.read_json(file_path, lines=True, chunksize=chunk_size)

    try:
        data = next(data_iter)
        data = transform(data)
        load(data)
    except StopIteration:
        return


@task(retries=1, cache_result_in_memory=True, log_prints=True)
@print_logger
def extract(year: int, month: int, day: int, hour: int) -> str:
    download_path = get_download_path(year, month, day, hour)

    print('start downloading data')
    file_path = download_if_not_exists(download_path)
    print('download finished')

    print('start unzipping data')
    file_path = unzip(file_path)
    print('unzipping finished')

    return file_path


@task(log_prints=True)
@print_logger
def transform(data: pd.DataFrame) -> pd.DataFrame:
    return data[['id', 'type', 'public', 'created_at']]


@task(retries=1, log_prints=True)
@print_logger
def load(data: pd.DataFrame):
    PostgresDB().insert(data, 'github_archives')


def get_download_path(year: int, month: int, day: int, hour: int):
    month = get_str_from_int(month)
    day = get_str_from_int(day)
    download_path = f"https://data.gharchive.org/{year}-{month}-{day}-{hour}.json.gz"
    return download_path


def get_str_from_int(number: int):
    if int(number) < 10:
        return '0' + str(number)

    return str(number)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--year')
    parser.add_argument('--month')
    parser.add_argument('--day')
    parser.add_argument('--hour')

    args = parser.parse_args()

    year = args.year
    month = args.month
    day = args.day
    hour = args.hour

    assert year is not None

    etl(year, month, day, hour)
