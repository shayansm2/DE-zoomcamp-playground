from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    # kwargs['tbl_name'] = 'yellow_tripdata'
    schema_name = 'public'  # Specify the name of the schema to export data to
    table_name = kwargs['tbl_name']  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    print('number of rows for inserting to db: ' +  str(len(df.index)))

    # batch_size = 500000
    # num_batches = len(df) // batch_size + 1
    # for i in range(num_batches):
    #     start_i = i * batch_size
    #     end_i = (i + 1) * batch_size
    #     df_batch = df.iloc[start_i:end_i]
    #     print(f'batch number {i}')
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='append',  # Specify resolution policy if table name already exists
        )