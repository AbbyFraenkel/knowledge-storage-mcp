"""
Configuration module for the Knowledge Storage MCP.

This module handles loading configuration from environment variables and provides
a settings object that can be used throughout the application.
"""

import os
from functools import lru_cache
from pydantic import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Neo4j Configuration
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "password")
    
    # Server Configuration
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8080"))
    debug: bool = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Schema Configuration
    schema_validation_enabled: bool = os.getenv("SCHEMA_VALIDATION_ENABLED", "True").lower() in ("true", "1", "t")
    schema_version: str = os.getenv("SCHEMA_VERSION", "1.0.0")
    
    # Application Configuration
    app_name: str = "Knowledge Storage MCP"
    app_version: str = "0.1.0"
    
    class Config:
        """Pydantic config."""
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    """
    Get application settings.
    
    Returns:
        Settings object with configuration values
    """
    return Settings()
