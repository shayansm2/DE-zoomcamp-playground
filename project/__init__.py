import argparse

from data_ingestion.etl import etl

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