"""
MongoDB connection management.

Provides MongoClient setup, collection handles, and
index management for the target database.
"""

import logging

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from src.config import Settings

logger = logging.getLogger(__name__)


class MongoDBClient:
    """Manages MongoDB connections and collection access.

    Handles connection lifecycle, collection references, and
    index creation for the target database.
    """

    def __init__(self, settings: Settings) -> None:
        self._uri = settings.mongo_uri
        self._db_name = settings.mongo_db
        self._metrics_collection_name = settings.mongo_metrics_collection
        self._state_collection_name = settings.mongo_state_collection
        self._client: MongoClient | None = None
        self._db: Database | None = None

    def connect(self) -> None:
        """Establish connection to MongoDB."""
        self._client = MongoClient(self._uri)
        self._db = self._client[self._db_name]
        # Verify connectivity
        self._client.admin.command("ping")
        logger.info("Connected to MongoDB (database: %s)", self._db_name)

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    @property
    def database(self) -> Database:
        """Get the active database handle."""
        assert self._db is not None, "Call connect() first"
        return self._db

    @property
    def metrics_collection(self) -> Collection:
        """Handle to the daily_metrics collection."""
        return self.database[self._metrics_collection_name]

    @property
    def state_collection(self) -> Collection:
        """Handle to the pipeline_state collection."""
        return self.database[self._state_collection_name]

    def ensure_indexes(self) -> None:
        """Create required indexes on target collections.

        The compound unique index on daily_metrics enforces one document
        per (restaurant_id, date, country) and enables upsert operations.
        """
        # Unique compound index — enforces the aggregation key constraint
        self.metrics_collection.create_index(
            [
                ("restaurant_id", ASCENDING),
                ("date", ASCENDING),
                ("country", ASCENDING),
            ],
            unique=True,
            name="idx_metrics_group_key",
        )
        logger.info("Ensured unique index on daily_metrics: idx_metrics_group_key")

        # Index on pipeline_state for quick lookups
        self.state_collection.create_index(
            "pipeline_id",
            unique=True,
            name="idx_pipeline_id",
        )
        logger.info("Ensured unique index on pipeline_state: idx_pipeline_id")
