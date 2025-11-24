from dagster import IOManager, io_manager
from sqlalchemy import create_engine, text
import pandas as pd

class PostgresPandasIOManager(IOManager):
    def __init__(self, conn_str: str):
        self.engine = create_engine(conn_str)

    def _get_schema_table(self, asset_key):
        path = asset_key.path
        if len(path) > 1:
            return path[0], path[-1]
        return "public", path[-1]

    def handle_output(self, context, obj: pd.DataFrame):
        schema, table_name = self._get_schema_table(context.asset_key)
        
        # Ensure schema exists
        if schema != "public":
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                conn.commit()

        obj.to_sql(table_name, self.engine, if_exists="replace", index=False, schema=schema)

    def load_input(self, context):
        schema, table_name = self._get_schema_table(context.asset_key)
        return pd.read_sql(f"SELECT * FROM {schema}.{table_name}", self.engine)

@io_manager(config_schema={"conn_str": str})
def postgres_io_manager(init_context):
    return PostgresPandasIOManager(init_context.resource_config["conn_str"])