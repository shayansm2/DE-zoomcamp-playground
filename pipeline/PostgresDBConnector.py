from prefect_sqlalchemy import SqlAlchemyConnector
from helpers import SingletonMeta


class PostgresDBConnector(object, metaclass=SingletonMeta):
    def get(self):
        connection_block = SqlAlchemyConnector.load('postgres-connector')
        return connection_block.get_connection(begin=False)
