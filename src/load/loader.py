"""
Load stage — writes aggregated metrics to MongoDB.

Uses bulk_write with UpdateOne + upsert=True for idempotent writes.
Each metric document is upserted by its composite key:
(restaurant_id, date, country).

Why $set and not $inc:
- $set is idempotent: running twice produces identical output
- $inc accumulates: running twice doubles the counts
- We recompute full aggregates, so $set reflects the true state
"""

import logging

from pymongo import UpdateOne
from pymongo.results import BulkWriteResult

from src.db.mongo import MongoDBClient
from src.models import DailyMetric

logger = logging.getLogger(__name__)


class Loader:
    """Loads computed metrics into MongoDB using bulk upsert.

    Each DailyMetric is upserted by its composite key. The $set
    operator replaces all metric fields, ensuring idempotency.
    """

    def __init__(self, mongo_client: MongoDBClient) -> None:
        self._mongo = mongo_client

    def bulk_upsert(self, metrics: list[DailyMetric]) -> int:
        """Upsert a batch of metrics into MongoDB.

        Uses bulk_write with ordered=False for parallel execution.
        Each operation is an UpdateOne with upsert=True:
        - If the document exists (matching filter): $set replaces metrics
        - If it doesn't exist: creates a new document

        The unique compound index on (restaurant_id, date, country)
        guarantees at most one document per group.

        Args:
            metrics: List of DailyMetric objects from the Transform stage.

        Returns:
            Number of documents upserted (modified + inserted).
        """
        if not metrics:
            logger.info("No metrics to load — skipping")
            return 0

        operations = [
            UpdateOne(
                filter=metric.to_mongo_filter(),
                update=metric.to_mongo_update(),
                upsert=True,
            )
            for metric in metrics
        ]

        logger.info("Executing bulk_write with %d upsert operations", len(operations))

        result: BulkWriteResult = self._mongo.metrics_collection.bulk_write(
            operations,
            ordered=False,  # Parallel execution — faster, no ordering dependency
        )

        upserted_count = result.upserted_count + result.modified_count
        logger.info(
            "Bulk upsert complete: inserted=%d, modified=%d, total=%d",
            result.upserted_count,
            result.modified_count,
            upserted_count,
        )

        return upserted_count
