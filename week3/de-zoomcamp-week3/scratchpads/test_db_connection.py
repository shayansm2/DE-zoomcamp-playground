"""
NOTE: Scratchpad blocks are used only for experimentation and testing out code.
The code written here will not be executed as part of the pipeline.
"""
from mage_ai.data_preparation.variable_manager import get_variable
import psycopg2
from psycopg2 import OperationalError

def create_conn():
    conn = None
    try:
        conn = psycopg2.connect(
            database="postgres",
            user="root",
            password="root",
            host="de-zoomcamp-week3-postgres",
            port="5432",
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return conn

connection = create_conn()
