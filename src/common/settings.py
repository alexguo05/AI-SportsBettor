"""Application settings and configuration."""
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

# Load .env file if present
load_dotenv()

# Project root
PROJECT_ROOT = Path(__file__).parent.parent.parent


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database
    postgres_dsn: str = "postgresql://sportsbettor:sportsbettor@localhost:5432/sportsbettor"
    
    # Odds API
    odds_api_key: Optional[str] = None
    
    # Logging
    log_level: str = "INFO"
    
    # Data paths
    data_raw_news_dir: Path = PROJECT_ROOT / "data" / "raw" / "news"
    data_raw_odds_dir: Path = PROJECT_ROOT / "data" / "raw" / "odds"
    data_ref_dir: Path = PROJECT_ROOT / "data" / "ref"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

# Ensure data directories exist
settings.data_raw_news_dir.mkdir(parents=True, exist_ok=True)
settings.data_raw_odds_dir.mkdir(parents=True, exist_ok=True)
settings.data_ref_dir.mkdir(parents=True, exist_ok=True)

