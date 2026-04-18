"""
Tests for the Extract stage.

Validates:
- Watermark-based filtering (only rows with updated_at > watermark)
- Batch size limits
- Empty result handling
- Backfill mode date range filtering
"""

from datetime import datetime, date
from decimal import Decimal

from src.extract.extractor import Extractor
from src.db.postgres import PostgresClient
from src.watermark import EPOCH
from tests.conftest import insert_order


class TestIncrementalExtract:
    """Tests for incremental (watermark-based) extraction."""

    def test_extracts_rows_after_watermark(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """Only rows with updated_at > watermark should be extracted."""
        # Insert orders at different times
        insert_order(
            pg_client,
            restaurant_id="rest_001",
            created_at=datetime(2024, 1, 10, 12, 0, 0),
            updated_at=datetime(2024, 1, 10, 12, 0, 0),
        )
        insert_order(
            pg_client,
            restaurant_id="rest_002",
            created_at=datetime(2024, 1, 15, 14, 0, 0),
            updated_at=datetime(2024, 1, 15, 14, 0, 0),
        )
        insert_order(
            pg_client,
            restaurant_id="rest_003",
            created_at=datetime(2024, 1, 20, 16, 0, 0),
            updated_at=datetime(2024, 1, 20, 16, 0, 0),
        )

        # Watermark at Jan 12 — should pick up orders from Jan 15 and Jan 20
        watermark = datetime(2024, 1, 12, 0, 0, 0)
        result = extractor.extract_incremental(watermark)

        assert result.row_count == 2
        restaurant_ids = {o.restaurant_id for o in result.orders}
        assert restaurant_ids == {"rest_002", "rest_003"}

    def test_respects_batch_size(
        self, pg_client: PostgresClient, settings
    ):
        """Should not return more rows than batch_size."""
        # Insert 10 orders
        for i in range(10):
            insert_order(
                pg_client,
                restaurant_id=f"rest_{i:03d}",
                created_at=datetime(2024, 1, 15, i, 0, 0),
                updated_at=datetime(2024, 1, 15, i, 0, 0),
            )

        # Extract with batch_size=3
        small_extractor = Extractor(pg_client, batch_size=3)
        result = small_extractor.extract_incremental(EPOCH)

        assert result.row_count == 3

    def test_returns_empty_when_no_changes(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """Should return empty result when no rows match watermark."""
        insert_order(
            pg_client,
            created_at=datetime(2024, 1, 10, 12, 0, 0),
            updated_at=datetime(2024, 1, 10, 12, 0, 0),
        )

        # Watermark is after all data
        watermark = datetime(2024, 12, 31, 23, 59, 59)
        result = extractor.extract_incremental(watermark)

        assert result.row_count == 0
        assert result.orders == []
        assert result.max_updated_at is None

    def test_tracks_max_updated_at(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """max_updated_at should reflect the latest row in the batch."""
        insert_order(
            pg_client,
            restaurant_id="rest_001",
            created_at=datetime(2024, 1, 15, 10, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 0, 0),
        )
        insert_order(
            pg_client,
            restaurant_id="rest_002",
            created_at=datetime(2024, 1, 16, 14, 0, 0),
            updated_at=datetime(2024, 1, 20, 8, 0, 0),  # Updated later
        )

        result = extractor.extract_incremental(EPOCH)

        assert result.max_updated_at == datetime(2024, 1, 20, 8, 0, 0)

    def test_picks_up_updated_orders(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """Orders with bumped updated_at should be re-extracted."""
        order_id = insert_order(
            pg_client,
            created_at=datetime(2024, 1, 15, 10, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 0, 0),
            status="placed",
        )

        # First extract picks it up
        result1 = extractor.extract_incremental(EPOCH)
        assert result1.row_count == 1

        # Simulate status change — updated_at moves forward
        from tests.conftest import update_order_status
        update_order_status(
            pg_client, order_id, "completed",
            updated_at=datetime(2024, 1, 20, 12, 0, 0),
        )

        # Extract with watermark at original time — should pick up again
        result2 = extractor.extract_incremental(
            datetime(2024, 1, 15, 10, 0, 0)
        )
        assert result2.row_count == 1
        assert result2.orders[0].status.value == "completed"


class TestBackfillExtract:
    """Tests for backfill (date range) extraction."""

    def test_extracts_orders_in_date_range(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """Backfill should return orders with created_at in range."""
        insert_order(pg_client, restaurant_id="rest_001",
                     created_at=datetime(2024, 1, 10, 12, 0, 0),
                     updated_at=datetime(2024, 1, 10, 12, 0, 0))
        insert_order(pg_client, restaurant_id="rest_002",
                     created_at=datetime(2024, 1, 15, 14, 0, 0),
                     updated_at=datetime(2024, 1, 15, 14, 0, 0))
        insert_order(pg_client, restaurant_id="rest_003",
                     created_at=datetime(2024, 1, 25, 16, 0, 0),
                     updated_at=datetime(2024, 1, 25, 16, 0, 0))

        result = extractor.extract_backfill(
            start_date=date(2024, 1, 12),
            end_date=date(2024, 1, 20),
        )

        assert result.row_count == 1
        assert result.orders[0].restaurant_id == "rest_002"

    def test_backfill_date_range_is_inclusive(
        self, extractor: Extractor, pg_client: PostgresClient
    ):
        """Both start_date and end_date should be inclusive."""
        insert_order(pg_client, restaurant_id="rest_001",
                     created_at=datetime(2024, 1, 15, 0, 0, 1),
                     updated_at=datetime(2024, 1, 15, 0, 0, 1))
        insert_order(pg_client, restaurant_id="rest_002",
                     created_at=datetime(2024, 1, 15, 23, 59, 59),
                     updated_at=datetime(2024, 1, 15, 23, 59, 59))

        result = extractor.extract_backfill(
            start_date=date(2024, 1, 15),
            end_date=date(2024, 1, 15),
        )

        assert result.row_count == 2
