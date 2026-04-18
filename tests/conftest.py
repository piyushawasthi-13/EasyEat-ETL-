"""
Shared test fixtures for the ETL pipeline.

All tests run against REAL PostgreSQL and MongoDB instances.
No mocking of database interactions — integration tests catch
issues that mocks hide.

Fixtures handle:
- Database connection setup/teardown
- Test data insertion/cleanup
- Pipeline component initialization
"""

import os
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import psycopg2.extras
import pytest

from src.config import Settings
from src.db.postgres import PostgresClient
from src.db.mongo import MongoDBClient
from src.watermark import WatermarkManager
from src.extract.extractor import Extractor
from src.transform.transformer import Transformer
from src.load.loader import Loader


# ---------------------------------------------------------------------------
# Test settings — override with env vars in CI
# ---------------------------------------------------------------------------

def get_test_settings() -> Settings:
    """Build settings for the test environment."""
    return Settings(
        postgres_dsn=os.getenv(
            "POSTGRES_DSN",
            "postgresql://easyeat:easyeat_secret@localhost:5432/easyeat",
        ),
        mongo_uri=os.getenv(
            "MONGO_URI",
            "mongodb://easyeat:easyeat_secret@localhost:27017",
        ),
        mongo_db="easyeat_test",  # Separate test database
        batch_size=100,
        cursor_itersize=100,
        log_level="DEBUG",
        pipeline_id="test_pipeline",
    )


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def settings() -> Settings:
    """Pipeline settings for tests."""
    return get_test_settings()


@pytest.fixture(scope="session")
def pg_client(settings: Settings) -> PostgresClient:
    """PostgreSQL client — session-scoped to avoid reconnecting per test."""
    client = PostgresClient(settings)
    client.connect()
    _create_orders_table(client)
    client.ensure_indexes()
    yield client
    client.close()


@pytest.fixture(scope="session")
def mongo_client(settings: Settings) -> MongoDBClient:
    """MongoDB client — session-scoped."""
    client = MongoDBClient(settings)
    client.connect()
    client.ensure_indexes()
    yield client
    # Drop the test database on teardown
    client.database.client.drop_database(settings.mongo_db)
    client.close()


@pytest.fixture(autouse=True)
def clean_databases(pg_client: PostgresClient, mongo_client: MongoDBClient):
    """Clean all data before each test for isolation."""
    # Clear PostgreSQL orders
    with pg_client.cursor() as cur:
        cur.execute("DELETE FROM orders")

    # Clear MongoDB collections
    mongo_client.metrics_collection.delete_many({})
    mongo_client.state_collection.delete_many({})

    yield


# ---------------------------------------------------------------------------
# Component fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def extractor(pg_client: PostgresClient, settings: Settings) -> Extractor:
    return Extractor(pg_client, settings.batch_size)


@pytest.fixture
def transformer(pg_client: PostgresClient) -> Transformer:
    return Transformer(pg_client)


@pytest.fixture
def loader(mongo_client: MongoDBClient) -> Loader:
    return Loader(mongo_client)


@pytest.fixture
def watermark_mgr(mongo_client: MongoDBClient, settings: Settings) -> WatermarkManager:
    return WatermarkManager(mongo_client, settings.pipeline_id)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _create_orders_table(pg_client: PostgresClient) -> None:
    """Create the orders table if it doesn't exist."""
    conn = pg_client.connection
    old_autocommit = conn.autocommit
    conn.autocommit = True
    with pg_client.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id    VARCHAR(36) PRIMARY KEY,
                restaurant_id VARCHAR(36) NOT NULL,
                user_id     VARCHAR(36) NOT NULL,
                status      VARCHAR(20) NOT NULL
                    CHECK (status IN ('placed', 'completed', 'cancelled')),
                amount      DECIMAL(12, 2) NOT NULL,
                country     VARCHAR(2) NOT NULL,
                created_at  TIMESTAMP NOT NULL,
                updated_at  TIMESTAMP NOT NULL
            )
        """)
    conn.autocommit = old_autocommit


def insert_order(
    pg_client: PostgresClient,
    order_id: str | None = None,
    restaurant_id: str = "rest_001",
    user_id: str | None = None,
    status: str = "placed",
    amount: Decimal = Decimal("500.00"),
    country: str = "IN",
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> str:
    """Insert a single order into PostgreSQL for testing.

    Returns the order_id for reference in assertions.
    """
    if order_id is None:
        order_id = str(uuid.uuid4())
    if user_id is None:
        user_id = str(uuid.uuid4())
    if created_at is None:
        created_at = datetime(2024, 1, 15, 10, 30, 0)
    if updated_at is None:
        updated_at = created_at

    with pg_client.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders (
                order_id, restaurant_id, user_id, status,
                amount, country, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (order_id, restaurant_id, user_id, status,
             amount, country, created_at, updated_at),
        )

    return order_id


def update_order_status(
    pg_client: PostgresClient,
    order_id: str,
    new_status: str,
    updated_at: datetime | None = None,
) -> None:
    """Update an order's status and updated_at timestamp."""
    if updated_at is None:
        updated_at = datetime.utcnow()

    with pg_client.cursor() as cur:
        cur.execute(
            "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
            (new_status, updated_at, order_id),
        )
