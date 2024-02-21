import dlt
import requests
import json
import os
import gzip
import io
import csv


def extract_load(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your custom logic here

    urls = []
    tblname = 'yellow_tripdata'
    for year in range(2019, 2021):
        for month in range(1, 2):
            urls.append(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02d}.csv.gz')

    print('should download these urls', urls)
    load_from_urls(urls, tblname)
    return

    urls = []
    tblname = 'fhv_tripdata'
    for year in range(2019, 2020):
        for month in range(1, 13):
            urls.append(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02d}.csv.gz')

    load_from_urls(urls, tblname)

    urls = []
    tblname = 'green_tripdata'
    for year in range(2019, 2021):
        for month in range(1, 13):
            urls.append(f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{year}-{month:02d}.csv.gz')

    load_from_urls(urls, tblname)


def load_from_urls(urls: list, tbl_name: str):
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    domain = os.getenv('POSTGRES_HOST')
    dbname = os.getenv('POSTGRES_DBNAME')

    
    pipeline = dlt.pipeline(
        destination=dlt.destinations.postgres(
            credentials=f"postgresql://{user}:{password}@{domain}/{dbname}"
        ),
        dataset_name='public'
    )
    
    for url in urls:
        pipeline.run(
            download_and_yield_rows(url),
            table_name=tbl_name,
            write_disposition="replace"
        )


def download_and_yield_rows(url):
    print(f'downloading {url}')
    response = requests.get(url, stream=True)
    response.raise_for_status()
    print(f'downloading finished')

    gzip_file = gzip.GzipFile(fileobj=io.BytesIO(response.content))
    reader = csv.reader(io.TextIOWrapper(gzip_file))

    for row in reader:
        yield row


extract_load()