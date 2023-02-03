from abc import abstractmethod
from typing import Optional
import pandas as pd
from prefect import flow, task


class ETL(object):
    @staticmethod
    @abstractmethod
    def get_flow_name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def get_db_connector():
        pass

    @staticmethod
    @abstractmethod
    def get_table_name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def get_source():
        pass

    @staticmethod
    def get_chunk_size() -> Optional[int]:
        return None

    @staticmethod
    def get_if_exists_config():
        return "append"

    @flow(name=get_flow_name())
    def run(self):
        self.ingest()

    def ingest(self):
        data = self.extract(self.get_source())
        data = self.transform(data)
        self.load(data)

    @task(name="Extract Data")
    def extract(self, source) -> pd.DataFrame:
        return pd.DataFrame()

    @task(name="Transform Data")
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

    @task(name="Load Data")
    def load(self, df: pd.DataFrame):
        df.to_sql(
            name=self.get_table_name(),
            con=self.get_db_connector(),
            if_exists=self.get_if_exists_config()
        )
