import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url: str = params.url

    csv_name = download_and_prepare_file(url)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()

            df = next(df_iter)

            # df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            # df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break


def download_and_prepare_file(url: str):
    name = url.split('/')[-1]
    csv_name = name.split('.')[0] + '.csv'

    if os.system(f"test -f {csv_name}") == 0:
        return csv_name

    if url.endswith('.csv.gz'):
        os.system(f"wget {url} -O {csv_name}.gz")
        os.system(f"gunzip {csv_name}.gz")
    elif url.endswith('.csv'):
        os.system(f"wget {url} -O {csv_name}")
    else:
        raise Exception('unhandled format to download')

    return csv_name


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
