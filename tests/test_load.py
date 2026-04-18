"""
Tests for the Load stage.

Validates:
- New groups create documents (insert path)
- Existing groups update in place (update path)
- $set replaces values (not $inc accumulation)
- Bulk operation correctness
"""

from decimal import Decimal

from src.db.mongo import MongoDBClient
from src.load.loader import Loader
from src.models import DailyMetric


class TestBulkUpsert:
    """Tests for the bulk_upsert operation."""

    def test_inserts_new_metrics(
        self, loader: Loader, mongo_client: MongoDBClient
    ):
        """New groups should create documents in MongoDB."""
        metrics = [
            DailyMetric(
                restaurant_id="rest_001",
                date="2024-01-15",
                country="IN",
                total_orders=10,
                completed_orders=8,
                cancelled_orders=1,
                gross_revenue=Decimal("5000.00"),
            )
        ]

        count = loader.bulk_upsert(metrics)

        assert count == 1
        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc is not None
        assert doc["total_orders"] == 10
        assert doc["completed_orders"] == 8
        assert doc["cancelled_orders"] == 1
        assert doc["gross_revenue"] == 5000.00

    def test_updates_existing_metrics(
        self, loader: Loader, mongo_client: MongoDBClient
    ):
        """Existing groups should be updated (not duplicated)."""
        # First insert
        metrics_v1 = [
            DailyMetric(
                restaurant_id="rest_001",
                date="2024-01-15",
                country="IN",
                total_orders=10,
                completed_orders=8,
                cancelled_orders=1,
                gross_revenue=Decimal("5000.00"),
            )
        ]
        loader.bulk_upsert(metrics_v1)

        # Update with new values (e.g., status changed)
        metrics_v2 = [
            DailyMetric(
                restaurant_id="rest_001",
                date="2024-01-15",
                country="IN",
                total_orders=10,
                completed_orders=7,  # One completion reversed
                cancelled_orders=2,  # One more cancellation
                gross_revenue=Decimal("4200.00"),
            )
        ]
        loader.bulk_upsert(metrics_v2)

        # Should have exactly one document (updated, not duplicated)
        count = mongo_client.metrics_collection.count_documents(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert count == 1

        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )
        assert doc["completed_orders"] == 7
        assert doc["cancelled_orders"] == 2
        assert doc["gross_revenue"] == 4200.00

    def test_handles_empty_metrics(self, loader: Loader):
        """Should handle empty list gracefully."""
        count = loader.bulk_upsert([])
        assert count == 0

    def test_multiple_groups_in_one_batch(
        self, loader: Loader, mongo_client: MongoDBClient
    ):
        """Bulk upsert should handle multiple groups efficiently."""
        metrics = [
            DailyMetric(
                restaurant_id=f"rest_{i:03d}",
                date="2024-01-15",
                country="IN",
                total_orders=i * 10,
                completed_orders=i * 7,
                cancelled_orders=i,
                gross_revenue=Decimal(str(i * 1000)),
            )
            for i in range(1, 6)
        ]

        count = loader.bulk_upsert(metrics)

        assert count == 5
        total = mongo_client.metrics_collection.count_documents({})
        assert total == 5

    def test_set_not_inc_behavior(
        self, loader: Loader, mongo_client: MongoDBClient
    ):
        """$set must replace values, not accumulate.

        This is THE critical idempotency test. If we used $inc,
        running the same batch twice would double the counts.
        With $set, the second run produces identical results.
        """
        metric = DailyMetric(
            restaurant_id="rest_001",
            date="2024-01-15",
            country="IN",
            total_orders=10,
            completed_orders=8,
            cancelled_orders=1,
            gross_revenue=Decimal("5000.00"),
        )

        # Run twice with identical data
        loader.bulk_upsert([metric])
        loader.bulk_upsert([metric])

        doc = mongo_client.metrics_collection.find_one(
            {"restaurant_id": "rest_001", "date": "2024-01-15", "country": "IN"}
        )

        # Values should be from the metric, not doubled
        assert doc["total_orders"] == 10  # NOT 20
        assert doc["completed_orders"] == 8  # NOT 16
        assert doc["gross_revenue"] == 5000.00  # NOT 10000.00
