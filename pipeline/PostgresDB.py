from prefect_sqlalchemy import SqlAlchemyConnector
from Database import Database


class PostgresDB(Database):
    @classmethod
    def get_connection(cls):
        connection_block = SqlAlchemyConnector.load('postgres-connector')
        return connection_block.get_connection(begin=False)
