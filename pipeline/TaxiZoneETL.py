from abc import ABC
from helpers import download_if_not_exists
import pandas as pd
from pipeline.ETL import ETL
from PostgresDBConnector import PostgresDBConnector


class TaxiZoneETL(ETL, ABC):
    @staticmethod
    def get_flow_name() -> str:
        return 'Ingest Taxi Zones Data'

    @classmethod
    def get_db_connector(cls):
        return PostgresDBConnector().get()

    @classmethod
    def get_table_name(cls) -> str:
        return 'taxi_zones'

    def extract(self, source) -> pd.DataFrame:
        file_name = download_if_not_exists(source)
        return pd.read_csv(file_name, chunksize=self.get_chunk_size())
