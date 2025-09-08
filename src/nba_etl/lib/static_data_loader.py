import pandas as pd
from nba_api.stats.static import players, teams

class StaticPlayerLoader:
    @staticmethod
    def load() -> pd.DataFrame:
        return pd.DataFrame(players.get_players())

class StaticTeamLoader:
    @staticmethod
    def load() -> pd.DataFrame:
        return pd.DataFrame(teams.get_teams())
