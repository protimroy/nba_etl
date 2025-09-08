import inspect
from dagster import asset
from nba_api.stats import endpoints as nba_endpoints

def _is_endpoint_member(obj):
    return (
        inspect.isclass(obj)
        and hasattr(obj, "get_data_frames")
        and obj.__module__.startswith("nba_api.stats.endpoints")
    )

def _make_endpoint_asset(endpoint_cls):
    name = endpoint_cls.__name__.lower()
    @asset(name=name, io_manager_key="postgres_io_manager")
    def _fetch(context):
        params = context.op_config or {}
        dfs = endpoint_cls(**params).get_data_frames()
        if not dfs:
            context.log.warn(f"No data for {name}")
            return None
        return dfs[0] if len(dfs) == 1 else {f"sheet_{i}": df for i, df in enumerate(dfs)}
    _fetch.config_schema = {"op": endpoint_cls.__init__.__annotations__}
    return _fetch

_endpoint_classes = [
    cls for _, cls in inspect.getmembers(nba_endpoints, _is_endpoint_member)
]

def get_dynamic_assets():
    return [_make_endpoint_asset(c) for c in _endpoint_classes]