from nba_etl.lib.static_data_loader import StaticPlayerLoader, StaticTeamLoader
from nba_etl.assets.factories.static_factory import make_static_asset

# These come from the nba_api python package, so we put them in the nba_api schema
all_players = make_static_asset("all_players", StaticPlayerLoader.load, key_prefix=["nba_api"])
all_teams = make_static_asset("all_teams", StaticTeamLoader.load, key_prefix=["nba_api"])

__all__ = ["all_players", "all_teams"]