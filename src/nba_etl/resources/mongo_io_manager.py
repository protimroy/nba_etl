from __future__ import annotations

from typing import Any, List

import pandas as pd
from dagster import IOManager, io_manager
from pymongo import MongoClient


class MongoPandasIOManager(IOManager):
    def __init__(self, connection_str: str, db_name: str, collection_name: str):
        self._client = MongoClient(connection_str)
        self._db = self._client[db_name]
        self._collection = self._db[collection_name]

    def handle_output(self, context, obj: pd.DataFrame):
        if obj is None or (isinstance(obj, pd.DataFrame) and obj.empty):
            context.log.info("No data to write to MongoDB.")
            return

        def _to_serializable(v: Any) -> Any:
            # Convert numpy types/arrays/lists into JSON-serializable types
            if hasattr(v, "tolist"):
                return v.tolist()
            return v

        docs: List[dict] = []
        for rec in obj.to_dict(orient="records"):
            docs.append({k: _to_serializable(v) for k, v in rec.items()})

        if docs:
            # Upsert by a natural key if present, else insert many
            key = "id" if "id" in docs[0] else None
            if key is None:
                self._collection.insert_many(docs)
            else:
                for d in docs:
                    self._collection.update_one({key: d[key]}, {"$set": d}, upsert=True)

    def load_input(self, context) -> pd.DataFrame:
        cur = self._collection.find()
        data = list(cur)
        if not data:
            return pd.DataFrame()
        # drop Mongo _id for cleanliness
        for d in data:
            d.pop("_id", None)
        return pd.DataFrame(data)


@io_manager(
    config_schema={
        "connection_str": str,
        "db_name": str,
        "collection_name": str,
    }
)
def mongo_io_manager(init_context):
    cfg = init_context.resource_config
    return MongoPandasIOManager(
        connection_str=cfg["connection_str"],
        db_name=cfg["db_name"],
        collection_name=cfg["collection_name"],
    )
