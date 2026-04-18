"""
Watermark management for incremental processing.

The watermark is a persisted timestamp representing the last
successfully processed `updated_at` value. It serves as the
single source of truth for pipeline progress.

Storage: MongoDB pipeline_state collection.
Update: Only after successful load (crash-safe checkpoint).
"""

import logging
from datetime import datetime, timedelta

from src.db.mongo import MongoDBClient

logger = logging.getLogger(__name__)

# Epoch timestamp — used when no watermark exists (first run or full rebuild)
EPOCH = datetime(1970, 1, 1)


class WatermarkManager:
    """Read, write, and reset the pipeline watermark.

    The watermark is stored in MongoDB's pipeline_state collection,
    co-located with the target data. This ensures:
    - Survives container restarts (persistent)
    - Atomic update (MongoDB document-level locking)
    - No additional infrastructure dependency (no Redis, no filesystem)
    """

    def __init__(self, mongo_client: MongoDBClient, pipeline_id: str) -> None:
        self._collection = mongo_client.state_collection
        self._pipeline_id = pipeline_id

    def get(self) -> datetime:
        """Read the current watermark.

        Returns:
            The last successfully processed updated_at timestamp.
            Returns EPOCH (1970-01-01) if no watermark exists (first run).
        """
        doc = self._collection.find_one({"pipeline_id": self._pipeline_id})
        if doc and "last_watermark" in doc:
            watermark = doc["last_watermark"]
            logger.info("Current watermark: %s", watermark.isoformat())
            return watermark  # type: ignore[no-any-return]

        logger.info("No watermark found — starting from epoch (first run)")
        return EPOCH

    @staticmethod
    def _ceil_to_millisecond(dt: datetime) -> datetime:
        """Ceil datetime to the next millisecond boundary.

        MongoDB's BSON Date only has millisecond precision. If we save a
        microsecond-precision timestamp (e.g. 764976µs) it gets truncated to
        764ms = 764000µs. The next extract query (WHERE updated_at > 764000µs)
        would then re-extract the same rows forever. Ceiling to the next
        millisecond (765ms) ensures those rows are excluded on the next run.
        """
        sub_ms = dt.microsecond % 1000
        if sub_ms == 0:
            return dt  # already on a millisecond boundary
        dt_ceiled = dt.replace(microsecond=0) + timedelta(milliseconds=dt.microsecond // 1000 + 1)
        return dt_ceiled

    def save(
        self,
        watermark: datetime,
        rows_processed: int = 0,
        groups_affected: int = 0,
    ) -> None:
        """Advance the watermark after a successful batch load.

        This is the critical checkpoint operation. It only runs AFTER
        the load stage succeeds, ensuring crash-safe recovery.

        Args:
            watermark: The max(updated_at) from the current batch.
            rows_processed: Number of rows extracted in this batch.
            groups_affected: Number of unique groups recomputed.
        """
        watermark = self._ceil_to_millisecond(watermark)
        self._collection.update_one(
            {"pipeline_id": self._pipeline_id},
            {
                "$set": {
                    "pipeline_id": self._pipeline_id,
                    "last_watermark": watermark,
                    "last_run_at": datetime.utcnow(),
                    "rows_processed": rows_processed,
                    "groups_affected": groups_affected,
                }
            },
            upsert=True,
        )
        logger.info(
            "Watermark advanced to %s (rows=%d, groups=%d)",
            watermark.isoformat(),
            rows_processed,
            groups_affected,
        )

    def reset(self) -> None:
        """Reset watermark to epoch for full rebuild.

        Used by full rebuild mode to force reprocessing of all records.
        Does NOT delete the state document — just resets the timestamp.
        """
        self._collection.update_one(
            {"pipeline_id": self._pipeline_id},
            {
                "$set": {
                    "last_watermark": EPOCH,
                    "last_run_at": datetime.utcnow(),
                }
            },
            upsert=True,
        )
        logger.info("Watermark reset to epoch for full rebuild")
