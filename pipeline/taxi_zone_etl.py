import pandas as pd
from prefect import flow, task

from PostgresDB import PostgresDB
from helpers import download_if_not_exists


@task()
def extract(source: str) -> pd.DataFrame:
    file_name = download_if_not_exists(source)
    return pd.read_csv(file_name)


@task()
def transform(df: pd.DataFrame) -> pd.DataFrame:
    return df


@task()
def load(df: pd.DataFrame):
    table_name = 'taxi_zones'
    PostgresDB().insert(df, table_name)


@flow(name="ingest taxi zones data")
def main():
    url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
    data = extract(url)
    data = transform(data)
    load(data)


if __name__ == '__main__':
    main()
