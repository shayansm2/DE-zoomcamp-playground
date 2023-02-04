from abc import abstractmethod
import pandas as pd
from project.utils.SingletonMeta import SingletonMeta


class Database(object, metaclass=SingletonMeta):
    @classmethod
    @abstractmethod
    def get_connection(cls):
        pass

    def insert(self, df: pd.DataFrame, table_name: str, method='append'):
        df.to_sql(name=table_name, con=self.get_connection(), if_exists=method)

    def select(self, sql) -> pd.DataFrame:
        return pd.read_sql(sql, self.get_connection())
