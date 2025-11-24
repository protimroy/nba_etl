from dotenv import load_dotenv
import os
import pandas as pd
import openai


from dagster import asset, AssetIn

load_dotenv()


# Initialize API clients
openai.api_key = os.getenv("OPENAI_API_KEY")

@asset(
    ins={"all_players": AssetIn(key=["nba_api", "all_players"])},
    io_manager_key="mongo_io_manager",
    name="player_embeddings_openai",
)
def player_embeddings_openai(all_players: pd.DataFrame) -> pd.DataFrame:
    """
    Embeds player full names using OpenAI’s text-embedding-ada-002.
    """
    texts = all_players["full_name"].astype(str).tolist()

    embeddings = []
    # Batch in increments of 500 to stay within API limits
    for i in range(0, len(texts), 500):
        batch = texts[i : i + 500]
        resp = openai.Embedding.create(
            model="text-embedding-ada-002",
            input=batch,
        )
        embeddings.extend(record["embedding"] for record in resp["data"])

    # Attach embeddings and return
    df = all_players.copy()
    df["embedding"] = embeddings
    return df[["id", "full_name", "embedding"]]