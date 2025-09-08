from dagster import IOManager, io_manager
from sqlalchemy import create_engine
import pandas as pd

class PostgresPandasIOManager(IOManager):
    def __init__(self, conn_str: str):
        self.engine = create_engine(conn_str)

    def handle_output(self, context, obj: pd.DataFrame):
        table_name = context.asset_key.path[-1]
        obj.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def load_input(self, context):
        table_name = context.asset_key.path[-1]
        return pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

@io_manager(config_schema={"conn_str": str})
def postgres_io_manager(init_context):
    return PostgresPandasIOManager(init_context.resource_config["conn_str"])