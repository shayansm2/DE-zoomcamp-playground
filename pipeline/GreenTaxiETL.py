from abc import ABC
import pandas as pd
from helpers import download_if_not_exists, unzip
from ETL import ETL
from PostgresDBConnector import PostgresDBConnector


class GreenTaxiETL(ETL, ABC):
    def get_flow_name(self) -> str:
        return "Ingest Green Taxi Data"

    @classmethod
    def get_db_connector(cls):
        PostgresDBConnector().get()

    @classmethod
    def get_table_name(cls) -> str:
        return 'green_taxi_trips'

    @classmethod
    def get_source(cls):
        return 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'

    def extract(self, source) -> pd.DataFrame:
        file_name = download_if_not_exists(source)
        file_name = unzip(file_name)
        return pd.read_csv(file_name, chunksize=self.get_chunk_size())

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
        data['lpep_dropoff_datetime'] = pd.to_datetime(data['lpep_dropoff_datetime'])
        data = data[data['passenger_count'] != 0]

        return data
