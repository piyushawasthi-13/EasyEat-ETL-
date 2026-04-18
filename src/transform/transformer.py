"""
Transform stage — identifies affected groups and computes aggregates.

This is the most critical stage in the pipeline. It implements the
"affected groups" recomputation pattern:

1. From extracted rows, identify unique (restaurant_id, date, country) groups
2. For each affected group, query PostgreSQL for ALL orders in that group
3. Compute the full aggregate using SQL GROUP BY

Why recompute instead of increment:
- Idempotent: running twice produces identical results
- Correct: handles status changes on historical orders
- $set replaces the document; $inc would accumulate errors on re-runs

The aggregation is pushed to PostgreSQL (SQL GROUP BY with FILTER)
rather than computed in Python. PostgreSQL's query executor is orders
of magnitude faster at aggregation, especially with proper indexes.
"""

import logging
from datetime import date

from src.db.postgres import PostgresClient
from src.models import Order, DailyMetric

logger = logging.getLogger(__name__)


class Transformer:
    """Transforms extracted orders into aggregated daily metrics.

    The transform is a two-step process:
    1. identify_affected_groups(): Pure Python — O(n) scan of extracted rows
    2. recompute_metrics(): SQL GROUP BY on PostgreSQL — O(G × log N)
    """

    # Recompute query — uses idx_orders_group_key composite index
    # PostgreSQL FILTER clause provides clean conditional aggregation
    RECOMPUTE_QUERY = """
        SELECT
            restaurant_id,
            DATE(created_at) AS date,
            country,
            COUNT(*)                                         AS total_orders,
            COUNT(*) FILTER (WHERE status = 'completed')     AS completed_orders,
            COUNT(*) FILTER (WHERE status = 'cancelled')     AS cancelled_orders,
            COALESCE(
                SUM(amount) FILTER (WHERE status = 'completed'), 0
            )                                                AS gross_revenue
        FROM orders
        WHERE (restaurant_id, DATE(created_at), country) IN (
            SELECT unnest(%(restaurant_ids)s),
                   unnest(%(dates)s)::date,
                   unnest(%(countries)s)
        )
        GROUP BY restaurant_id, DATE(created_at), country
    """

    def __init__(self, pg_client: PostgresClient) -> None:
        self._pg = pg_client

    def identify_affected_groups(
        self, orders: list[Order]
    ) -> set[tuple[str, date, str]]:
        """Identify unique aggregation groups affected by the current batch.

        Each order's group key is (restaurant_id, DATE(created_at), country).
        An order placed on Jan 15 but cancelled on Jan 20 affects the Jan 15
        group — we use created_at for the date, not updated_at.

        Args:
            orders: Validated Order objects from the Extract stage.

        Returns:
            Set of (restaurant_id, date, country) tuples to recompute.
        """
        groups: set[tuple[str, date, str]] = set()
        for order in orders:
            groups.add(order.group_key)

        logger.info(
            "Identified %d affected groups from %d orders",
            len(groups),
            len(orders),
        )
        return groups

    def recompute_metrics(
        self, affected_groups: set[tuple[str, date, str]]
    ) -> list[DailyMetric]:
        """Recompute full aggregates for affected groups from PostgreSQL.

        This is the key correctness mechanism. Instead of incrementally
        adjusting metrics ($inc), we re-query ALL orders for each affected
        group and compute the complete aggregate. This guarantees:

        - Correctness: status changes on old orders are reflected
        - Idempotency: re-running produces identical output
        - Consistency: total_orders always equals the actual count

        The query uses PostgreSQL's FILTER clause for conditional aggregation
        and the idx_orders_group_key composite index for efficient lookups.

        Args:
            affected_groups: Set of (restaurant_id, date, country) tuples.

        Returns:
            List of fully computed DailyMetric objects ready for upsert.
        """
        if not affected_groups:
            return []

        # Decompose tuples into parallel arrays for PostgreSQL IN clause
        # Using unnest() with parallel arrays is the efficient way to do
        # multi-column IN queries in PostgreSQL
        restaurant_ids = []
        dates = []
        countries = []
        for rid, d, country in affected_groups:
            restaurant_ids.append(rid)
            dates.append(d.isoformat())
            countries.append(country)

        logger.info("Recomputing metrics for %d groups", len(affected_groups))

        rows = self._pg.execute(
            self.RECOMPUTE_QUERY,
            {
                "restaurant_ids": restaurant_ids,
                "dates": dates,
                "countries": countries,
            },
        )

        metrics: list[DailyMetric] = []
        for row in rows:
            metric = DailyMetric(
                restaurant_id=row["restaurant_id"],
                date=row["date"].isoformat() if isinstance(row["date"], date) else str(row["date"]),
                country=row["country"],
                total_orders=row["total_orders"],
                completed_orders=row["completed_orders"],
                cancelled_orders=row["cancelled_orders"],
                gross_revenue=row["gross_revenue"],
            )

            # Sanity check — catch data quality issues early
            if not metric.validate_consistency():
                logger.warning(
                    "Inconsistent metrics for group (%s, %s, %s): "
                    "total=%d, completed=%d, cancelled=%d",
                    metric.restaurant_id,
                    metric.date,
                    metric.country,
                    metric.total_orders,
                    metric.completed_orders,
                    metric.cancelled_orders,
                )

            metrics.append(metric)

        logger.info("Computed %d metric aggregates", len(metrics))
        return metrics
