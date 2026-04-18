"""
Pipeline orchestrator — ties Extract → Transform → Load together.

Supports three operational modes:
1. Incremental (default): Process records changed since last watermark
2. Backfill: Reprocess a specific date range (--start-date / --end-date)
3. Full: Complete historical rebuild

The orchestrator manages the batch loop, watermark checkpointing,
and structured logging for observability.

Usage:
    python -m src.pipeline                          # Incremental
    python -m src.pipeline --mode backfill \\
        --start-date 2024-01-01 --end-date 2024-01-31
    python -m src.pipeline --mode full              # Full rebuild
    python -m src.pipeline --dry-run                # Extract + transform only
"""

import argparse
import logging
import os
import sys
import time

from datetime import date, datetime

import psutil

from src.config import Settings, get_settings
from src.db.mongo import MongoDBClient
from src.db.postgres import PostgresClient
from src.extract.extractor import Extractor
from src.load.loader import Loader
from src.transform.transformer import Transformer
from src.watermark import WatermarkManager

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Orchestrates the Extract → Transform → Load pipeline.

    Manages the batch loop for incremental processing and
    delegates to individual stages for each step.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

        # Initialize database clients
        self._pg = PostgresClient(settings)
        self._mongo = MongoDBClient(settings)

        # Components are initialized in setup() so that tests can inject
        # alternative db clients before setup() is called.
        self._extractor: Extractor | None = None
        self._transformer: Transformer | None = None
        self._loader: Loader | None = None
        self._watermark_mgr: WatermarkManager | None = None

    def setup(self) -> None:
        """Connect to databases and ensure indexes exist."""
        logger.info("Setting up pipeline...")
        self._pg.connect()
        self._mongo.connect()

        # Ensure indexes on both source and target
        self._pg.ensure_indexes()
        self._mongo.ensure_indexes()

        # Initialize pipeline components using the current (possibly injected) clients
        self._extractor = Extractor(self._pg, self._settings.batch_size)
        self._transformer = Transformer(self._pg)
        self._loader = Loader(self._mongo)
        self._watermark_mgr = WatermarkManager(
            self._mongo, self._settings.pipeline_id
        )
        logger.info("Pipeline setup complete")

    def teardown(self) -> None:
        """Close database connections."""
        self._pg.close()
        self._mongo.close()
        logger.info("Pipeline teardown complete")

    @property
    def watermark_mgr(self) -> WatermarkManager:
        assert self._watermark_mgr is not None, "Call setup() first"
        return self._watermark_mgr

    def run_incremental(self, dry_run: bool = False) -> dict:
        """Run the pipeline in incremental mode.

        Processes batches in a loop until no more changed rows are found.
        Each batch:
        1. Extract changed rows since watermark
        2. Identify affected groups
        3. Recompute full aggregates for those groups
        4. Bulk upsert to MongoDB
        5. Advance watermark (only after successful load)

        Args:
            dry_run: If True, skip the load stage and don't advance watermark.

        Returns:
            Summary statistics for the entire run.
        """
        logger.info("Starting incremental pipeline run")
        total_rows = 0
        total_groups = 0
        total_upserts = 0
        batch_count = 0
        run_start = time.time()
        _process = psutil.Process(os.getpid())
        peak_memory_mb = 0.0

        # Read watermark once outside the loop.
        # In dry-run mode we advance it in-memory so the loop terminates
        # without writing anything to MongoDB.
        assert self._extractor is not None and self._transformer is not None and self._loader is not None, "Call setup() first"
        watermark = self.watermark_mgr.get()

        while True:
            batch_start = time.time()

            # === EXTRACT ===
            result = self._extractor.extract_incremental(watermark)

            if result.row_count == 0:
                logger.info("No more rows to process — pipeline is current")
                break

            # === TRANSFORM ===
            affected_groups = self._transformer.identify_affected_groups(result.orders)
            metrics = self._transformer.recompute_metrics(affected_groups)

            # === LOAD ===
            assert result.max_updated_at is not None
            if dry_run:
                logger.info(
                    "[DRY RUN] Would upsert %d metrics — skipping load",
                    len(metrics),
                )
                # Advance watermark in-memory so the loop moves forward
                watermark = result.max_updated_at
            else:
                upsert_count = self._loader.bulk_upsert(metrics)
                total_upserts += upsert_count

                # Advance watermark ONLY after successful load
                self.watermark_mgr.save(
                    watermark=result.max_updated_at,
                    rows_processed=result.row_count,
                    groups_affected=len(affected_groups),
                )
                watermark = result.max_updated_at

            # Track stats
            batch_count += 1
            total_rows += result.row_count
            total_groups += len(affected_groups)
            batch_duration = time.time() - batch_start
            current_memory_mb = _process.memory_info().rss / 1024 / 1024
            peak_memory_mb = max(peak_memory_mb, current_memory_mb)

            logger.info(
                "Batch %d complete: rows=%d, groups=%d, duration=%.2fs",
                batch_count,
                result.row_count,
                len(affected_groups),
                batch_duration,
            )

        run_duration = time.time() - run_start
        summary = {
            "mode": "incremental",
            "dry_run": dry_run,
            "batches": batch_count,
            "total_rows": total_rows,
            "total_groups": total_groups,
            "total_upserts": total_upserts,
            "duration_seconds": round(run_duration, 2),
            "peak_memory_mb": round(peak_memory_mb, 1),
        }
        logger.info("Pipeline run complete: %s", summary)
        return summary

    def run_backfill(
        self, start_date: date, end_date: date, dry_run: bool = False
    ) -> dict:
        """Run the pipeline in backfill mode for a date range.

        Reprocesses all orders with created_at in [start_date, end_date].
        Does NOT affect the incremental watermark — backfill is an
        independent operation.

        Args:
            start_date: Start of reprocessing range (inclusive).
            end_date: End of reprocessing range (inclusive).
            dry_run: If True, skip the load stage.

        Returns:
            Summary statistics.
        """
        logger.info(
            "Starting backfill: %s to %s",
            start_date.isoformat(),
            end_date.isoformat(),
        )
        run_start = time.time()
        assert self._extractor is not None and self._transformer is not None and self._loader is not None, "Call setup() first"

        # === EXTRACT ===
        result = self._extractor.extract_backfill(start_date, end_date)

        if result.row_count == 0:
            logger.info("No orders found in date range — nothing to backfill")
            return {"mode": "backfill", "total_rows": 0}

        # === TRANSFORM ===
        affected_groups = self._transformer.identify_affected_groups(result.orders)
        metrics = self._transformer.recompute_metrics(affected_groups)

        # === LOAD ===
        upsert_count = 0
        if dry_run:
            logger.info(
                "[DRY RUN] Would upsert %d metrics — skipping load",
                len(metrics),
            )
        else:
            upsert_count = self._loader.bulk_upsert(metrics)
            # NOTE: Backfill does NOT advance the incremental watermark

        run_duration = time.time() - run_start
        summary = {
            "mode": "backfill",
            "dry_run": dry_run,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "total_rows": result.row_count,
            "total_groups": len(affected_groups),
            "total_upserts": upsert_count,
            "duration_seconds": round(run_duration, 2),
        }
        logger.info("Backfill complete: %s", summary)
        return summary

    def run_full(self, dry_run: bool = False) -> dict:
        """Run a full historical rebuild.

        Resets the watermark to epoch and processes all records
        via the normal incremental loop. After completion, the
        watermark reflects the latest data.

        Args:
            dry_run: If True, skip the load stage.

        Returns:
            Summary statistics.
        """
        logger.info("Starting full rebuild — resetting watermark")

        if not dry_run:
            self.watermark_mgr.reset()

        return self.run_incremental(dry_run=dry_run)


def configure_logging(level: str) -> None:
    """Set up structured logging."""
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="EasyEat ETL Pipeline — Restaurant Order Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.pipeline                                          # Incremental
  python -m src.pipeline --mode backfill --start-date 2024-01-01 --end-date 2024-01-31
  python -m src.pipeline --mode full                              # Full rebuild
  python -m src.pipeline --dry-run                                # No writes
        """,
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "backfill", "full"],
        default="incremental",
        help="Pipeline execution mode (default: incremental)",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Backfill start date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Backfill end date (YYYY-MM-DD, inclusive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run extract + transform only, skip load",
    )

    args = parser.parse_args()

    # Validate backfill arguments
    if args.mode == "backfill":
        if not args.start_date or not args.end_date:
            parser.error("Backfill mode requires --start-date and --end-date")
        if args.start_date > args.end_date:
            parser.error("--start-date must be before --end-date")

    return args


def main() -> None:
    """Main entry point for the ETL pipeline."""
    args = parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)

    logger.info("=" * 60)
    logger.info("EasyEat ETL Pipeline — Starting")
    logger.info("Mode: %s | Dry run: %s", args.mode, args.dry_run)
    logger.info("=" * 60)

    pipeline = ETLPipeline(settings)

    try:
        pipeline.setup()

        if args.mode == "incremental":
            summary = pipeline.run_incremental(dry_run=args.dry_run)
        elif args.mode == "backfill":
            summary = pipeline.run_backfill(
                start_date=args.start_date,
                end_date=args.end_date,
                dry_run=args.dry_run,
            )
        elif args.mode == "full":
            summary = pipeline.run_full(dry_run=args.dry_run)
        else:
            raise ValueError(f"Unknown mode: {args.mode}")

        logger.info("=" * 60)
        logger.info("Pipeline finished successfully")
        logger.info("Summary: %s", summary)
        logger.info("=" * 60)

    except Exception:
        logger.exception("Pipeline failed with error")
        sys.exit(1)
    finally:
        pipeline.teardown()


if __name__ == "__main__":
    main()
