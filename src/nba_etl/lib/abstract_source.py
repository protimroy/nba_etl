from abc import ABC, abstractmethod
import pandas as pd
from typing import Any, Dict, Optional

class BaseApiSource(ABC):
    """
    Abstract base class that all data sources must implement.
    """
    
    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique name for the source (e.g., 'nba_api', 'balldontlie')"""
        pass

    @abstractmethod
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Logic to hit the API and return raw data (JSON/Dict)"""
        pass

    @abstractmethod
    def normalize_data(self, raw_data: Any, endpoint: str) -> pd.DataFrame:
        """Logic to convert raw data into a clean DataFrame"""
        pass
