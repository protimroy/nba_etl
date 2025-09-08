from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from dagster import IOManager, io_manager
from superduper import superduper

class MongoSuperDuperIOManager(IOManager):
    def __init__(
        self,
        connection_str: str,
        collection_name: str,
        vector_index_backend: str = "local",
    ):
        """
        Initialize Superduper with MongoDB plugin via URI.

        Args:
            connection_str: Mongo URI including database, e.g. "mongodb://host:27017/db_name".
            collection_name: Name of the MongoDB collection.
            vector_index_backend: Backend for vector indices (e.g., "local", "pinecone").
        """
        # Factory call to instantiate underlying MongoDataBackend automatically
        self.db = superduper(
            connection_str,
            vector_index_backend=vector_index_backend,
        )
        self.collection = self.db.collection(collection_name)

    def handle_output(self, context, obj: pd.DataFrame):
        """
        Write DataFrame rows to MongoDB collection via Superduper.
        """
        # Convert DataFrame to list of dicts
        docs = obj.to_dict(orient="records")
        self.collection.insert_many(docs)

    def load_input(self, context) -> pd.DataFrame:
        """
        Read all documents from MongoDB collection into a DataFrame.
        """
        return pd.DataFrame(self.collection.find())

@io_manager(config_schema={
    "connection_str": str,
    "collection_name": str,
    "vector_index_backend": str,
})
def mongo_superduper_io_manager(init_context):
    """
    Dagster IO manager wrapping Superduper MongoDB plugin.
    Expects 'connection_str', 'collection_name', and optional 'vector_index_backend'.
    """
    config = init_context.resource_config
    return MongoSuperDuperIOManager(
        connection_str=config["connection_str"],
        collection_name=config["collection_name"],
        vector_index_backend=config.get("vector_index_backend", "local"),
    )