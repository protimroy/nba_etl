from dagster import Definitions, define_asset_job, load_assets_from_modules;
import importlib
import pkgutil
import nba_etl.assets.endpoints as endpoints_pkg
from . import assets;

all_assets = load_assets_from_modules( [assets] );

# Dynamically import all modules inside nba_etl.assets.endpoints
modules = [
    importlib.import_module(f"{endpoints_pkg.__name__}.{name}")
    for _, name, is_pkg in pkgutil.iter_modules(endpoints_pkg.__path__)
    if not is_pkg
]

defs = Definitions(
    assets=all_assets,
);
