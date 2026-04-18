"""
Extract stage — retrieves changed orders from PostgreSQL.

This module handles three extraction modes:
1. Incremental: Fetch orders where updated_at > watermark
2. Backfill: Fetch orders where created_at is within a date range
3. Full: Fetch all orders (watermark reset to epoch)

Key performance decisions:
- Server-side cursors: PostgreSQL holds the result set; Python streams chunks
- Keyset pagination: WHERE updated_at > $last_seen (O(log n) per page)
- No LIMIT/OFFSET: OFFSET is O(n) on large tables — unusable at 50M rows
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime

from src.db.postgres import PostgresClient
from src.models import Order

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Output of the extract stage.

    Contains the extracted orders and the maximum updated_at
    timestamp for watermark advancement.
    """

    orders: list[Order]
    max_updated_at: datetime | None  # None if no rows extracted
    row_count: int


class Extractor:
    """Extracts changed orders from PostgreSQL.

    Uses server-side cursors for memory-efficient streaming.
    Each call extracts one batch of rows, bounded by batch_size.
    """

    # Incremental extract query — uses idx_orders_updated_at index
    INCREMENTAL_QUERY = """
        SELECT order_id, restaurant_id, user_id, status, amount,
               country, created_at, updated_at
        FROM orders
        WHERE updated_at > %(watermark)s
        ORDER BY updated_at ASC
        LIMIT %(batch_size)s
    """

    # Backfill extract query — uses idx_orders_created_at index
    BACKFILL_QUERY = """
        SELECT order_id, restaurant_id, user_id, status, amount,
               country, created_at, updated_at
        FROM orders
        WHERE created_at >= %(start_date)s
          AND created_at < %(end_date)s
        ORDER BY created_at ASC
    """

    def __init__(self, pg_client: PostgresClient, batch_size: int) -> None:
        self._pg = pg_client
        self._batch_size = batch_size

    def extract_incremental(self, watermark: datetime) -> ExtractionResult:
        """Extract orders changed since the given watermark.

        Uses keyset pagination: WHERE updated_at > $watermark ORDER BY updated_at.
        This is O(log n) per batch via the idx_orders_updated_at B-tree index,
        regardless of how many batches have already been processed.

        Args:
            watermark: The last successfully processed updated_at timestamp.

        Returns:
            ExtractionResult with the batch of orders and max updated_at.
        """
        logger.info(
            "Extracting orders updated after %s (batch_size=%d)",
            watermark.isoformat(),
            self._batch_size,
        )

        rows = self._pg.execute(
            self.INCREMENTAL_QUERY,
            {"watermark": watermark, "batch_size": self._batch_size},
        )

        return self._build_result(rows)

    def extract_backfill(self, start_date: date, end_date: date) -> ExtractionResult:
        """Extract all orders within a created_at date range.

        Used for historical reprocessing. Queries by created_at (not updated_at)
        to capture all orders attributed to the reporting dates in the range.

        This mode does NOT affect the incremental watermark — it's an
        independent operation that can run alongside normal incremental processing.

        Args:
            start_date: Start of the date range (inclusive).
            end_date: End of the date range (inclusive).

        Returns:
            ExtractionResult with all orders in the date range.
        """
        # Convert date to datetime for PostgreSQL TIMESTAMP comparison
        start_dt = datetime.combine(start_date, datetime.min.time())
        # end_date is inclusive, so we go to the start of the next day
        from datetime import timedelta

        end_dt = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

        logger.info(
            "Extracting orders for backfill: %s to %s",
            start_date.isoformat(),
            end_date.isoformat(),
        )

        rows = self._pg.execute(
            self.BACKFILL_QUERY,
            {"start_date": start_dt, "end_date": end_dt},
        )

        return self._build_result(rows)

    def _build_result(self, rows: list[dict]) -> ExtractionResult:
        """Convert raw database rows to validated Order objects.

        Pydantic validation catches any data quality issues at the
        extraction boundary — before they propagate to transform/load.
        """
        if not rows:
            logger.info("No rows extracted")
            return ExtractionResult(orders=[], max_updated_at=None, row_count=0)

        orders: list[Order] = []
        max_updated_at: datetime | None = None

        for row in rows:
            order = Order(**row)
            orders.append(order)
            if max_updated_at is None or order.updated_at > max_updated_at:
                max_updated_at = order.updated_at

        logger.info(
            "Extracted %d orders (max_updated_at=%s)",
            len(orders),
            max_updated_at.isoformat() if max_updated_at else "N/A",
        )

        return ExtractionResult(
            orders=orders,
            max_updated_at=max_updated_at,
            row_count=len(orders),
        )
