# 1. Standard library
import os

# 2. Third-party
from dotenv import load_dotenv
from dagster import Definitions, define_asset_job, AssetSelection

# 3. Your own code
#   – static assets
from nba_etl.assets.static import all_players, all_teams
#   – core moneyball assets
from nba_etl.assets.nba_analytics_transformations import league_player_stats, player_profiles
#   – embeddings
from nba_etl.assets.embeddings import player_embeddings_openai
#   – dynamic‐endpoint factory (optional, keeps the project agentic/modular)
from nba_etl.assets.factories.dynamic_factory import get_dynamic_assets
#   – new source framework
from nba_etl.sources.balldontlie_source import BallDontLieSource
from nba_etl.assets.factories.source_asset_factory import build_assets_for_source

#   – IO managers
from nba_etl.resources.postgres_io_manager import postgres_io_manager
from nba_etl.resources.mongo_io_manager import mongo_io_manager

# Load environment variables
load_dotenv()

# Build connection strings from env
POSTGRES_CONN_STR = (
    f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB')}"
)

mongo_user = os.getenv('MONGO_USER')
mongo_pass = os.getenv('MONGO_PASSWORD')
mongo_host = os.getenv('MONGO_HOST', 'localhost')
mongo_port = os.getenv('MONGO_PORT', '27017')
mongo_db = os.getenv('MONGO_DB')

auth = f"{mongo_user}:{mongo_pass}@" if mongo_user and mongo_pass else ""
db_suffix = f"/{mongo_db}" if mongo_db else ""
MONGO_CONNECTION_STR = f"mongodb://{auth}{mongo_host}:{mongo_port}{db_suffix}"

# Initialize Sources
bdl_source = BallDontLieSource(api_key=os.getenv("BDL_API_KEY", "demo"))
bdl_assets = build_assets_for_source(bdl_source, endpoints=["players", "teams"])

# Compose asset list
all_assets = [
    # static
    all_players,
    all_teams,
    # moneyball core
    league_player_stats,
    player_profiles,
    # embeddings
    player_embeddings_openai,
    # dynamic endpoints keep it agentic/extendable
    *get_dynamic_assets(),
    # new source assets
    *bdl_assets,
]

# Define a focused job for the Moneyball pipeline (NBA)
moneyball_nba_job = define_asset_job(
    name="moneyball_nba",
    selection=AssetSelection.assets(league_player_stats, player_profiles),
)

# Single Definitions object with consistent resource keys
defs = Definitions(
    assets=all_assets,
    jobs=[moneyball_nba_job],
    resources={
        # Default io_manager maps to Postgres for simple persistence
        "io_manager": postgres_io_manager.configured({"conn_str": POSTGRES_CONN_STR}),
        # Explicit key used by some assets/factories
        "postgres_io_manager": postgres_io_manager.configured({"conn_str": POSTGRES_CONN_STR}),
        # Mongo for embeddings and any doc-style assets
        "mongo_io_manager": mongo_io_manager.configured({
            "connection_str": MONGO_CONNECTION_STR,
            "db_name": mongo_db or "nba",
        }),
    },
)
