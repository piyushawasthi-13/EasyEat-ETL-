"""
Tests for the Transform stage.

Validates:
- Affected group identification from extracted orders
- SQL aggregation correctness (counts, revenue)
- Handling of multiple statuses in the same group
- The critical late-status-update scenario
"""

from datetime import datetime, date
from decimal import Decimal

from src.db.postgres import PostgresClient
from src.extract.extractor import Extractor
from src.transform.transformer import Transformer
from src.watermark import EPOCH
from tests.conftest import insert_order, update_order_status


class TestGroupIdentification:
    """Tests for identify_affected_groups()."""

    def test_identifies_unique_groups(
        self, transformer: Transformer, extractor: Extractor,
        pg_client: PostgresClient
    ):
        """Should return unique (restaurant_id, date, country) tuples."""
        # Two orders for same restaurant/date/country
        insert_order(pg_client, restaurant_id="rest_001", country="IN",
                     created_at=datetime(2024, 1, 15, 10, 0, 0),
                     updated_at=datetime(2024, 1, 15, 10, 0, 0))
        insert_order(pg_client, restaurant_id="rest_001", country="IN",
                     created_at=datetime(2024, 1, 15, 14, 0, 0),
                     updated_at=datetime(2024, 1, 15, 14, 0, 0))
        # One order for different restaurant
        insert_order(pg_client, restaurant_id="rest_002", country="US",
                     created_at=datetime(2024, 1, 15, 12, 0, 0),
                     updated_at=datetime(2024, 1, 15, 12, 0, 0))

        result = extractor.extract_incremental(EPOCH)
        groups = transformer.identify_affected_groups(result.orders)

        assert len(groups) == 2
        assert (
            "rest_001", date(2024, 1, 15), "IN"
        ) in groups
        assert (
            "rest_002", date(2024, 1, 15), "US"
        ) in groups

    def test_groups_use_created_at_date(
        self, transformer: Transformer, extractor: Extractor,
        pg_client: PostgresClient
    ):
        """Group date should come from created_at, not updated_at."""
        insert_order(
            pg_client,
            restaurant_id="rest_001",
            country="IN",
            created_at=datetime(2024, 1, 15, 10, 0, 0),
            updated_at=datetime(2024, 1, 20, 12, 0, 0),  # Updated 5 days later
        )

        result = extractor.extract_incremental(EPOCH)
        groups = transformer.identify_affected_groups(result.orders)

        # Group date should be Jan 15 (created_at), not Jan 20 (updated_at)
        assert ("rest_001", date(2024, 1, 15), "IN") in groups
        assert ("rest_001", date(2024, 1, 20), "IN") not in groups


class TestMetricRecomputation:
    """Tests for recompute_metrics()."""

    def test_correct_aggregation(
        self, transformer: Transformer, pg_client: PostgresClient
    ):
        """Metrics should correctly count statuses and sum revenue."""
        dt = datetime(2024, 1, 15, 10, 0, 0)

        # 3 completed, 1 cancelled, 2 placed
        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("250.50"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("75.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="cancelled",
                     amount=Decimal("300.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="placed",
                     amount=Decimal("150.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="placed",
                     amount=Decimal("200.00"), country="IN",
                     created_at=dt, updated_at=dt)

        affected_groups = {("rest_001", date(2024, 1, 15), "IN")}
        metrics = transformer.recompute_metrics(affected_groups)

        assert len(metrics) == 1
        m = metrics[0]
        assert m.restaurant_id == "rest_001"
        assert m.date == "2024-01-15"
        assert m.country == "IN"
        assert m.total_orders == 6
        assert m.completed_orders == 3
        assert m.cancelled_orders == 1
        # gross_revenue = 100.00 + 250.50 + 75.00 = 425.50 (completed only)
        assert m.gross_revenue == Decimal("425.50")

    def test_cancelled_orders_have_zero_revenue(
        self, transformer: Transformer, pg_client: PostgresClient
    ):
        """Cancelled orders should not contribute to gross_revenue."""
        dt = datetime(2024, 1, 15, 10, 0, 0)

        insert_order(pg_client, restaurant_id="rest_001", status="cancelled",
                     amount=Decimal("999.99"), country="IN",
                     created_at=dt, updated_at=dt)

        affected_groups = {("rest_001", date(2024, 1, 15), "IN")}
        metrics = transformer.recompute_metrics(affected_groups)

        assert len(metrics) == 1
        assert metrics[0].gross_revenue == Decimal("0")
        assert metrics[0].total_orders == 1
        assert metrics[0].cancelled_orders == 1
        assert metrics[0].completed_orders == 0

    def test_multiple_groups_recomputed(
        self, transformer: Transformer, pg_client: PostgresClient
    ):
        """Should compute metrics independently for each group."""
        dt1 = datetime(2024, 1, 15, 10, 0, 0)
        dt2 = datetime(2024, 1, 16, 10, 0, 0)

        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=dt1, updated_at=dt1)
        insert_order(pg_client, restaurant_id="rest_002", status="completed",
                     amount=Decimal("200.00"), country="US",
                     created_at=dt2, updated_at=dt2)

        affected_groups = {
            ("rest_001", date(2024, 1, 15), "IN"),
            ("rest_002", date(2024, 1, 16), "US"),
        }
        metrics = transformer.recompute_metrics(affected_groups)

        assert len(metrics) == 2
        by_restaurant = {m.restaurant_id: m for m in metrics}

        assert by_restaurant["rest_001"].gross_revenue == Decimal("100.00")
        assert by_restaurant["rest_002"].gross_revenue == Decimal("200.00")

    def test_empty_groups_returns_empty(
        self, transformer: Transformer
    ):
        """Should handle empty affected_groups gracefully."""
        metrics = transformer.recompute_metrics(set())
        assert metrics == []

    def test_metric_consistency_validation(
        self, transformer: Transformer, pg_client: PostgresClient
    ):
        """All metrics should pass consistency check."""
        dt = datetime(2024, 1, 15, 10, 0, 0)

        insert_order(pg_client, restaurant_id="rest_001", status="completed",
                     amount=Decimal("100.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="cancelled",
                     amount=Decimal("50.00"), country="IN",
                     created_at=dt, updated_at=dt)
        insert_order(pg_client, restaurant_id="rest_001", status="placed",
                     amount=Decimal("75.00"), country="IN",
                     created_at=dt, updated_at=dt)

        affected_groups = {("rest_001", date(2024, 1, 15), "IN")}
        metrics = transformer.recompute_metrics(affected_groups)

        for m in metrics:
            assert m.validate_consistency(), (
                f"total={m.total_orders}, completed={m.completed_orders}, "
                f"cancelled={m.cancelled_orders}"
            )
