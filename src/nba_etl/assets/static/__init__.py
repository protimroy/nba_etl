from nba_etl.lib.static_data_loader import StaticPlayerLoader, StaticTeamLoader
from nba_etl.assets.factories.static_factory import make_static_asset

all_players = make_static_asset("all_players", StaticPlayerLoader.load)
all_teams = make_static_asset("all_teams", StaticTeamLoader.load)

__all__ = ["all_players", "all_teams"]