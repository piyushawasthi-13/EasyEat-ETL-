"""
End-to-end pipeline tests.

These tests exercise the full Extract → Transform → Load pipeline,
including the most critical scenarios:

1. Late status update: An order placed on Jan 15 is cancelled on Jan 20.
   The pipeline must correctly update Jan 15's metrics.

2. Backfill: Reprocessing a date range independently of the watermark.

3. Full rebuild: Resetting and reprocessing all data.
"""

from datetime import date, datetime
from decimal import Decimal

from src.config import Settings
from src.db.mongo import MongoDBClient
from src.db.postgres import PostgresClient
from src.pipeline import ETLPipeline
from tests.conftest import insert_order, update_order_status


class TestLateStatusUpdate:
    """The most critical test — handling orders whose status changes
    after the original reporting date.

    Scenario:
    1. Order ORD-001 placed on Jan 15 (status=placed, amount=500)
    2. Pipeline runs → Jan 15 metrics: total=1, completed=0, revenue=0
    3. On Jan 20, ORD-001 is completed (updated_at=Jan 20)
    4. Pipeline runs → Jan 15 metrics MUST show: completed=1, revenue=500
    """

    def test_late_status_change_updates_original_date(
        self,
        pg_client: PostgresClient,
        mongo_client: MongoDBClient,
        settings: Settings,
    ):
        pipeline = ETLPipeline(settings)
        pipeline._pg = pg_client
        pipeline._mongo = mongo_client
        pipeline.setup()

        # Step 1: Insert order on Jan 15 as "placed"
        order_id = insert_order(
            pg_client,
            restaurant_id="rest_001",
            status="placed",
            amount=Decimal("500.00"),
            country="IN",
            created_at=datetime(2024, 1, 15, 10, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 0, 0),
        )

        # Step 2: First pipeline run
        pipeline.run_incremental()

        # Verify Jan 15 metrics after first run
        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc is not None
        assert doc["total_orders"] == 1
        assert doc["completed_orders"] == 0
        assert doc["cancelled_orders"] == 0
        assert doc["gross_revenue"] == 0.0

        # Step 3: Order completed on Jan 20 (updated_at changes)
        update_order_status(
            pg_client,
            order_id,
            new_status="completed",
            updated_at=datetime(2024, 1, 20, 14, 0, 0),
        )

        # Step 4: Second pipeline run
        pipeline.run_incremental()

        # Verify Jan 15 metrics are CORRECTLY UPDATED
        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc is not None
        assert doc["total_orders"] == 1
        assert doc["completed_orders"] == 1  # NOW completed
        assert doc["cancelled_orders"] == 0
        assert doc["gross_revenue"] == 500.00  # NOW has revenue

    def test_late_cancellation_reduces_revenue(
        self,
        pg_client: PostgresClient,
        mongo_client: MongoDBClient,
        settings: Settings,
    ):
        """An order initially completed then cancelled should reduce revenue."""
        pipeline = ETLPipeline(settings)
        pipeline._pg = pg_client
        pipeline._mongo = mongo_client
        pipeline.setup()

        # Insert as completed
        order_id = insert_order(
            pg_client,
            restaurant_id="rest_001",
            status="completed",
            amount=Decimal("800.00"),
            country="IN",
            created_at=datetime(2024, 1, 15, 10, 0, 0),
            updated_at=datetime(2024, 1, 15, 10, 0, 0),
        )

        # Also insert another completed order that stays
        insert_order(
            pg_client,
            restaurant_id="rest_001",
            status="completed",
            amount=Decimal("200.00"),
            country="IN",
            created_at=datetime(2024, 1, 15, 14, 0, 0),
            updated_at=datetime(2024, 1, 15, 14, 0, 0),
        )

        # First run — revenue = 800 + 200 = 1000
        pipeline.run_incremental()

        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc["gross_revenue"] == 1000.00
        assert doc["completed_orders"] == 2

        # Cancel the first order
        update_order_status(
            pg_client, order_id, "cancelled",
            updated_at=datetime(2024, 1, 20, 8, 0, 0),
        )

        # Second run — revenue should drop to 200
        pipeline.run_incremental()

        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc["gross_revenue"] == 200.00
        assert doc["completed_orders"] == 1
        assert doc["cancelled_orders"] == 1


class TestBackfillMode:
    """Tests for backfill (date range reprocessing)."""

    def test_backfill_does_not_affect_watermark(
        self,
        pg_client: PostgresClient,
        mongo_client: MongoDBClient,
        settings: Settings,
    ):
        """Backfill should NOT advance the incremental watermark."""
        pipeline = ETLPipeline(settings)
        pipeline._pg = pg_client
        pipeline._mongo = mongo_client
        pipeline.setup()

        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=datetime(2024, 1, 15, 10, 0, 0),
                     updated_at=datetime(2024, 1, 15, 10, 0, 0))

        # Run incremental to set the watermark
        pipeline.run_incremental()
        wm_before = pipeline.watermark_mgr.get()

        # Insert new data and run backfill
        insert_order(pg_client, restaurant_id="rest_002", status="completed",
                     amount=Decimal("200.00"), country="US",
                     created_at=datetime(2024, 1, 16, 10, 0, 0),
                     updated_at=datetime(2024, 1, 16, 10, 0, 0))

        pipeline.run_backfill(
            start_date=date(2024, 1, 16),
            end_date=date(2024, 1, 16),
        )

        # Watermark should NOT have changed
        wm_after = pipeline.watermark_mgr.get()
        assert wm_before == wm_after

        # But the metrics should be populated
        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_002", "date": "2024-01-16", "country": "US"}
        )
        assert doc is not None
        assert doc["gross_revenue"] == 200.00


class TestDryRun:
    """Tests for dry run mode (no writes)."""

    def test_dry_run_does_not_write(
        self,
        pg_client: PostgresClient,
        mongo_client: MongoDBClient,
        settings: Settings,
    ):
        """Dry run should extract and transform but not load."""
        pipeline = ETLPipeline(settings)
        pipeline._pg = pg_client
        pipeline._mongo = mongo_client
        pipeline.setup()

        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=datetime(2024, 1, 15, 10, 0, 0),
                     updated_at=datetime(2024, 1, 15, 10, 0, 0))

        pipeline.run_incremental(dry_run=True)

        # MongoDB should be empty — nothing written
        count = mongo_client.metrics_collection.count_documents({})
        assert count == 0
