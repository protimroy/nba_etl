from __future__ import annotations

import math
import pandas as pd
from dagster import asset, AssetIn
from nba_api.stats.static import players as static_players
from nba_api.stats.endpoints import playergamelog


@asset(
    io_manager_key="postgres_io_manager",
    name="league_player_stats",
    config_schema={
        "season": str,  # e.g. "2024-25"
        "max_players": int,  # limit to avoid API rate issues in dev
    },
)
def league_player_stats(context) -> pd.DataFrame:
    """
    Pull recent game logs for active NBA players and aggregate core rate stats.
    Configurable season and max_players for safer iteration during development.
    """
    season = context.op_config.get("season") or "2024-25"
    max_players = int(context.op_config.get("max_players") or 200)

    active_players = [p for p in static_players.get_players() if p.get("is_active")]
    if max_players > 0:
        active_players = active_players[:max_players]

    rows = []
    for p in active_players:
        try:
            gid = p["id"]
            gl = playergamelog.PlayerGameLog(player_id=gid, season=season).get_data_frames()[0]
            if gl.empty:
                continue
            # Aggregate simple per-game averages
            keep_cols = [
                "PTS", "REB", "AST", "STL", "BLK", "TOV", "FG3M", "FG3A", "FGM", "FGA", "FTM", "FTA", "PLUS_MINUS",
            ]
            agg = gl[keep_cols].mean(numeric_only=True)
            agg["player_id"] = gid
            agg["player_name"] = p["full_name"]
            rows.append(agg)
        except Exception:
            # Keep going even if some players fail
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Derived features with robust numeric handling
    fga = df["FGA"].astype(float).fillna(0.0)
    fta = df["FTA"].astype(float).fillna(0.0)
    pts = df["PTS"].astype(float).fillna(0.0)
    fg3m = df["FG3M"].astype(float).fillna(0.0)
    fg3a = df["FG3A"].astype(float).fillna(0.0)

    ts_attempts = fga + 0.44 * fta
    # Avoid divide-by-zero; where zero, fallback to 0.5 neutral TS%
    df["TS%"] = pts.divide(2 * ts_attempts, fill_value=0.0)
    df.loc[ts_attempts == 0, "TS%"] = 0.5

    # 3P% with zero/NaN guard
    df["3P%"] = fg3m.divide(fg3a.replace(0.0, math.nan))
    df["3P%"] = df["3P%"].fillna(0.0)

    return df


@asset(
    ins={"league_player_stats": AssetIn("league_player_stats"), "all_players": AssetIn("all_players")},
    io_manager_key="postgres_io_manager",
    name="player_profiles",
)
def player_profiles(league_player_stats: pd.DataFrame, all_players: pd.DataFrame) -> pd.DataFrame:
    """
    Join static player metadata with aggregated stats to form a first-cut player profile table.
    """
    if league_player_stats is None or league_player_stats.empty:
        return pd.DataFrame()
    meta = all_players[["id", "full_name", "team_id"]].rename(columns={"id": "player_id"})
    df = league_player_stats.merge(meta, on="player_id", how="left")
    # Reorder columns
    cols = [
        "player_id", "player_name", "full_name", "team_id",
        "PTS", "REB", "AST", "STL", "BLK", "TOV", "PLUS_MINUS",
        "FGM", "FGA", "FTM", "FTA", "FG3M", "FG3A", "3P%", "TS%",
    ]
    return df[[c for c in cols if c in df.columns]]
