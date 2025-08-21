"""
Configuration management for UK Premises Licence Scraper
"""

import os
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # OpenAI Configuration
    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    openai_model: str = Field("gpt-4o-mini", env="OPENAI_MODEL")
    
    # Scraping Configuration
    request_timeout: int = Field(30, env="REQUEST_TIMEOUT")
    max_concurrent_requests: int = Field(5, env="MAX_CONCURRENT_REQUESTS")
    delay_between_requests: float = Field(1.0, env="DELAY_BETWEEN_REQUESTS")
    
    # Data Storage
    data_dir: str = Field("data", env="DATA_DIR")
    reports_dir: str = Field("reports", env="REPORTS_DIR")
    
    # Selenium Configuration
    headless_browser: bool = Field(True, env="HEADLESS_BROWSER")
    browser_timeout: int = Field(30, env="BROWSER_TIMEOUT")
    
    # Logging
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra environment variables


def get_settings() -> Settings:
    """Get application settings"""
    return Settings()


# Create data directories if they don't exist
def setup_directories():
    """Create necessary directories"""
    settings = get_settings()
    os.makedirs(settings.data_dir, exist_ok=True)
    os.makedirs(settings.reports_dir, exist_ok=True)
    os.makedirs(f"{settings.data_dir}/councils", exist_ok=True)
    os.makedirs(f"{settings.data_dir}/licences", exist_ok=True)
    os.makedirs(f"{settings.data_dir}/cache", exist_ok=True)
