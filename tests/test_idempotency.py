"""
Idempotency tests — the pipeline is safe to re-run.

These tests verify the most critical property of the pipeline:
running it multiple times on the same data produces identical results.
No duplicates, no accumulated counts, no corrupted state.
"""

from datetime import datetime, date
from decimal import Decimal

from src.db.postgres import PostgresClient
from src.db.mongo import MongoDBClient
from src.extract.extractor import Extractor
from src.transform.transformer import Transformer
from src.load.loader import Loader
from src.watermark import WatermarkManager, EPOCH
from tests.conftest import insert_order


class TestIdempotency:
    """Full pipeline idempotency tests."""

    def test_double_run_identical_output(
        self,
        pg_client: PostgresClient,
        mongo_client: MongoDBClient,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
        watermark_mgr: WatermarkManager,
    ):
        """Running the pipeline twice produces byte-identical metrics.

        This is the acceptance criterion: pipeline re-run safety.
        """
        dt = datetime(2024, 1, 15, 10, 0, 0)
        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("500.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="cancelled",
                     amount=Decimal("200.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="placed",
                     amount=Decimal("300.00"), country="IN",
                     created_at=dt, updated_at=dt)

        # --- First run ---
        result1 = extractor.extract_incremental(EPOCH)
        groups1 = transformer.identify_affected_groups(result1.orders)
        metrics1 = transformer.recompute_metrics(groups1)
        loader.bulk_upsert(metrics1)
        watermark_mgr.save(result1.max_updated_at, result1.row_count, len(groups1))

        # Snapshot after first run
        doc_after_first = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )

        # --- Reset watermark and run again (simulating re-run) ---
        watermark_mgr.reset()

        result2 = extractor.extract_incremental(EPOCH)
        groups2 = transformer.identify_affected_groups(result2.orders)
        metrics2 = transformer.recompute_metrics(groups2)
        loader.bulk_upsert(metrics2)

        # Snapshot after second run
        doc_after_second = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )

        # --- Assertions: identical output ---
        assert doc_after_first["total_orders"] == doc_after_second["total_orders"] == 3
        assert doc_after_first["completed_orders"] == doc_after_second["completed_orders"] == 1
        assert doc_after_first["cancelled_orders"] == doc_after_second["cancelled_orders"] == 1
        assert doc_after_first["gross_revenue"] == doc_after_second["gross_revenue"] == 500.00

        # Only ONE document (no duplicates)
        count = mongo_client.metrics_collection.count_documents(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert count == 1

    def test_watermark_prevents_reprocessing(
        self,
        pg_client: PostgresClient,
        extractor: Extractor,
        transformer: Transformer,
        loader: Loader,
        watermark_mgr: WatermarkManager,
    ):
        """After advancing watermark, same rows are NOT re-extracted."""
        dt = datetime(2024, 1, 15, 10, 0, 0)
        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=dt, updated_at=dt)

        # First run — extracts the order
        result1 = extractor.extract_incremental(EPOCH)
        assert result1.row_count == 1

        # Advance watermark
        watermark_mgr.save(result1.max_updated_at, result1.row_count, 1)

        # Second run with advanced watermark — should find nothing
        current_watermark = watermark_mgr.get()
        result2 = extractor.extract_incremental(current_watermark)
        assert result2.row_count == 0
