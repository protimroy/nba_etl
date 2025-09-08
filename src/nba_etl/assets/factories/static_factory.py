from dagster import asset
from typing import Callable
import pandas as pd

def make_static_asset(name: str, loader_fn: Callable[[], pd.DataFrame]):
    @asset(name=name)
    def _asset() -> pd.DataFrame:
        return loader_fn()
    return _asset