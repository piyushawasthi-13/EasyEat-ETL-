"""
PostgreSQL connection management.

Provides connection pooling and server-side cursor support
for efficient streaming of large result sets.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extras

from src.config import Settings

logger = logging.getLogger(__name__)


class PostgresClient:
    """Manages PostgreSQL connections with server-side cursor support.

    Uses psycopg2 named cursors to stream results from PostgreSQL
    without loading the full result set into application memory.
    This is critical for handling 50M+ row tables.
    """

    def __init__(self, settings: Settings) -> None:
        self._dsn = settings.postgres_dsn
        self._itersize = settings.cursor_itersize
        self._conn: psycopg2.extensions.connection | None = None

    def connect(self) -> None:
        """Establish connection to PostgreSQL."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            # Use autocommit for read-only operations to avoid
            # holding long transactions during streaming
            self._conn.autocommit = True
            logger.info("Connected to PostgreSQL")

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            logger.info("PostgreSQL connection closed")

    @property
    def connection(self) -> psycopg2.extensions.connection:
        """Get the active connection, reconnecting if necessary."""
        if self._conn is None or self._conn.closed:
            self.connect()
        assert self._conn is not None
        return self._conn

    @contextmanager
    def cursor(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        """Standard client-side cursor for small queries."""
        cur = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def server_cursor(
        self, name: str = "etl_stream"
    ) -> Generator[psycopg2.extensions.cursor, None, None]:
        """Named server-side cursor for streaming large result sets.

        PostgreSQL holds the result set on the server. Python fetches
        rows in chunks of `itersize` — only that many rows are in
        memory at once.

        Args:
            name: Unique cursor name. PostgreSQL requires named cursors
                  for server-side streaming.
        """
        # Server-side cursors require a transaction (no autocommit)
        old_autocommit = self.connection.autocommit
        self.connection.autocommit = False
        cur = self.connection.cursor(
            name=name,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        cur.itersize = self._itersize
        try:
            yield cur
        finally:
            cur.close()
            self.connection.rollback()  # Clean up the read-only transaction
            self.connection.autocommit = old_autocommit

    def execute(self, query: str, params: Any = None) -> list[dict]:
        """Execute a query and return all results.

        Use only for small result sets (recompute queries, metadata).
        For large extractions, use server_cursor().
        """
        with self.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()  # type: ignore[return-value]

    def ensure_indexes(self) -> None:
        """Create required indexes on the orders table if they don't exist.

        Three indexes serve three distinct query patterns:
        1. idx_orders_updated_at  — incremental extract (watermark query)
        2. idx_orders_group_key   — group recomputation (affected groups)
        3. idx_orders_created_at  — backfill mode (date range query)
        """
        indexes = [
            (
                "idx_orders_updated_at",
                "CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at)",
            ),
            (
                "idx_orders_group_key",
                "CREATE INDEX IF NOT EXISTS idx_orders_group_key "
                "ON orders(restaurant_id, created_at, country)",
            ),
            (
                "idx_orders_created_at",
                "CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at)",
            ),
        ]
        # Need non-autocommit for DDL
        old_autocommit = self.connection.autocommit
        self.connection.autocommit = True
        with self.cursor() as cur:
            for name, ddl in indexes:
                cur.execute(ddl)
                logger.info("Ensured index: %s", name)
        self.connection.autocommit = old_autocommit
