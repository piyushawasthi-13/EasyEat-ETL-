"""
Configuration management using Pydantic Settings.

All pipeline configuration is driven by environment variables,
validated at startup. No hardcoded values in source code.
"""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Pipeline configuration — loaded from environment variables."""

    # PostgreSQL (source)
    postgres_dsn: str = Field(
        description="PostgreSQL connection string",
        examples=["postgresql://user:pass@localhost:5432/easyeat"],
    )

    # MongoDB (target)
    mongo_uri: str = Field(
        description="MongoDB connection URI",
        examples=["mongodb://user:pass@localhost:27017"],
    )
    mongo_db: str = Field(
        default="easyeat",
        description="MongoDB database name",
    )
    mongo_metrics_collection: str = Field(
        default="daily_metrics",
        description="Target collection for aggregated metrics",
    )
    mongo_state_collection: str = Field(
        default="pipeline_state",
        description="Collection for watermark and pipeline state",
    )

    # Processing
    batch_size: int = Field(
        default=5000,
        ge=100,
        le=50000,
        description="Number of rows to extract per batch",
    )
    cursor_itersize: int = Field(
        default=5000,
        ge=100,
        le=50000,
        description="PostgreSQL server-side cursor fetch chunk size",
    )

    # Observability
    log_level: str = Field(
        default="INFO",
        pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Logging verbosity",
    )

    # Pipeline identity
    pipeline_id: str = Field(
        default="daily_metrics_etl",
        description="Unique identifier for this pipeline instance",
    )

    model_config = {
        "env_prefix": "",
        "case_sensitive": False,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


def get_settings() -> Settings:
    """Load and validate settings from environment variables."""
    return Settings()  # type: ignore[call-arg]
