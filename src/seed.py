"""
Test data generator for the EasyEat ETL pipeline.

Generates realistic restaurant order data in PostgreSQL with
configurable volume. The generated data includes:
- Multiple restaurants across multiple countries
- Realistic status distributions (70% completed, 10% cancelled, 20% placed)
- Orders spanning multiple dates with realistic timestamps
- Some orders with updated_at != created_at (simulating late status changes)

Usage:
    python -m src.seed                    # Default: 10,000 rows
    python -m src.seed --count 1000000    # 1M rows
    python -m src.seed --count 50000000   # 50M rows (takes ~10 min)
"""

import argparse
import logging
import random
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import psycopg2.extras

from src.config import get_settings

logger = logging.getLogger(__name__)

# Realistic data distributions
COUNTRIES = ["IN", "US", "UK", "AE", "SG", "AU"]
STATUSES = ["placed", "completed", "cancelled"]
STATUS_WEIGHTS = [0.20, 0.70, 0.10]  # 20% placed, 70% completed, 10% cancelled

# Number of restaurants — affects group cardinality
NUM_RESTAURANTS = 500


def generate_restaurant_ids(count: int) -> list[str]:
    """Pre-generate restaurant IDs for consistent data."""
    return [f"rest_{i:04d}" for i in range(count)]


def generate_batch(
    restaurant_ids: list[str],
    batch_size: int,
    start_date: datetime,
    date_range_days: int = 90,
) -> list[tuple]:
    """Generate a batch of order rows.

    Some orders have updated_at > created_at to simulate late
    status changes — this is critical for testing the pipeline's
    ability to handle late-arriving updates.
    """
    rows = []
    for _ in range(batch_size):
        order_id = str(uuid.uuid4())
        restaurant_id = random.choice(restaurant_ids)
        user_id = str(uuid.uuid4())
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        amount = Decimal(str(round(random.uniform(50, 5000), 2)))
        country = random.choice(COUNTRIES)

        # created_at: random time within the date range
        created_at = start_date + timedelta(
            days=random.randint(0, date_range_days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        # updated_at: usually same as created_at, but 15% of orders
        # have a later updated_at (simulating status changes)
        if status != "placed" and random.random() < 0.15:
            # Status changed 1-5 days after creation
            updated_at = created_at + timedelta(
                days=random.randint(1, 5),
                hours=random.randint(0, 23),
            )
        else:
            updated_at = created_at

        rows.append((
            order_id,
            restaurant_id,
            user_id,
            status,
            amount,
            country,
            created_at,
            updated_at,
        ))

    return rows


def seed_database(total_rows: int) -> None:
    """Create the orders table and populate with test data."""
    settings = get_settings()

    conn = psycopg2.connect(settings.postgres_dsn)
    conn.autocommit = True

    with conn.cursor() as cur:
        # Create table
        cur.execute("""
            DROP TABLE IF EXISTS orders;
            CREATE TABLE orders (
                order_id    VARCHAR(36) PRIMARY KEY,
                restaurant_id VARCHAR(36) NOT NULL,
                user_id     VARCHAR(36) NOT NULL,
                status      VARCHAR(20) NOT NULL
                    CHECK (status IN ('placed', 'completed', 'cancelled')),
                amount      DECIMAL(12, 2) NOT NULL,
                country     VARCHAR(2) NOT NULL,
                created_at  TIMESTAMP NOT NULL,
                updated_at  TIMESTAMP NOT NULL
            );
        """)
        logger.info("Created orders table")

    # Insert data in batches using execute_values for speed
    restaurant_ids = generate_restaurant_ids(NUM_RESTAURANTS)
    start_date = datetime(2024, 1, 1)
    batch_size = 10000
    inserted = 0

    conn.autocommit = False

    with conn.cursor() as cur:
        while inserted < total_rows:
            current_batch = min(batch_size, total_rows - inserted)
            rows = generate_batch(restaurant_ids, current_batch, start_date)

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO orders (
                    order_id, restaurant_id, user_id, status,
                    amount, country, created_at, updated_at
                ) VALUES %s
                ON CONFLICT (order_id) DO NOTHING
                """,
                rows,
                page_size=current_batch,
            )

            inserted += current_batch
            if inserted % 100000 == 0 or inserted == total_rows:
                conn.commit()
                logger.info("Inserted %d / %d rows (%.1f%%)",
                           inserted, total_rows, inserted / total_rows * 100)

    conn.commit()

    # Create indexes AFTER bulk insert (faster than indexing during insert)
    conn.autocommit = True
    with conn.cursor() as cur:
        logger.info("Creating indexes...")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_group_key "
            "ON orders(restaurant_id, created_at, country)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at)"
        )
        logger.info("Indexes created")

        # Report stats
        cur.execute("SELECT COUNT(*) FROM orders")
        count = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(DISTINCT restaurant_id) FROM orders"
        )
        restaurants = cur.fetchone()[0]
        cur.execute(
            "SELECT MIN(created_at), MAX(created_at) FROM orders"
        )
        min_date, max_date = cur.fetchone()

    logger.info("=" * 50)
    logger.info("Seed complete:")
    logger.info("  Total orders:  %d", count)
    logger.info("  Restaurants:   %d", restaurants)
    logger.info("  Date range:    %s to %s", min_date, max_date)
    logger.info("=" * 50)

    conn.close()


def main() -> None:
    """CLI entry point for the seed script."""
    parser = argparse.ArgumentParser(description="Generate test data for EasyEat ETL")
    parser.add_argument(
        "--count",
        type=int,
        default=10000,
        help="Number of order rows to generate (default: 10000)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )

    logger.info("Seeding database with %d orders...", args.count)
    seed_database(args.count)


if __name__ == "__main__":
    main()
