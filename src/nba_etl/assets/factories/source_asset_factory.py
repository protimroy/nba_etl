from dagster import asset, AssetsDefinition
from typing import List
from ..lib.abstract_source import BaseApiSource

def build_assets_for_source(
    source_system: BaseApiSource, 
    endpoints: List[str]
) -> List[AssetsDefinition]:
    """
    Generates a list of Dagster assets for a specific source system.
    """
    assets = []

    for endpoint in endpoints:
        # Use the endpoint as the asset name, and source_name as the key prefix (schema)
        # e.g. Asset Key: ["balldontlie", "players"] -> Schema: balldontlie, Table: players
        
        @asset(
            name=endpoint,
            key_prefix=[source_system.source_name],
            group_name=source_system.source_name,
            compute_kind="python"
        )
        def _dynamic_asset(context):
            context.log.info(f"Fetching {endpoint} from {source_system.source_name}")
            
            raw_data = source_system.fetch_data(endpoint)
            df = source_system.normalize_data(raw_data, endpoint)
            
            context.log.info(f"Fetched {len(df)} rows")
            return df

        assets.append(_dynamic_asset)

    return assets
