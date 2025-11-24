import requests
import pandas as pd
from typing import Any, Dict, Optional
from ..lib.abstract_source import BaseApiSource

class BallDontLieSource(BaseApiSource):
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.balldontlie.io/v1"

    @property
    def source_name(self) -> str:
        return "balldontlie"

    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        headers = {"Authorization": self.api_key}
        # Handle pagination logic here if needed
        response = requests.get(f"{self.base_url}/{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def normalize_data(self, raw_data: Any, endpoint: str) -> pd.DataFrame:
        # Specific logic for BallDontLie JSON structure
        if "data" in raw_data:
            return pd.DataFrame(raw_data["data"])
        return pd.DataFrame([raw_data])
