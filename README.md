# EasyEat ETL Pipeline

Production-grade ETL pipeline that processes restaurant order data from PostgreSQL and generates daily aggregated metrics in MongoDB.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        EasyEat ETL Pipeline                         │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │   EXTRACT    │    │  TRANSFORM   │    │     LOAD     │          │
│  │              │    │              │    │              │          │
│  │ Server-side  │───▶│ Identify     │───▶│ Bulk upsert  │          │
│  │ cursor       │    │ affected     │    │ $set         │          │
│  │ WHERE        │    │ groups       │    │ (idempotent) │          │
│  │ updated_at   │    │              │    │              │          │
│  │ > watermark  │    │ Recompute    │    │              │          │
│  │              │    │ full metrics │    │              │          │
│  └──────────────┘    └──────────────┘    └──────┬───────┘          │
│         │                                        │                  │
│         │                               ┌────────▼────────┐        │
│         │                               │    Watermark    │        │
│         │                               │    advance      │        │
│         │                               │ (after success) │        │
│         │                               └─────────────────┘        │
└─────────┼────────────────────────────────────────────────────────── ┘
          │
┌─────────▼──────────┐                    ┌─────────────────────────┐
│     PostgreSQL     │                    │         MongoDB         │
│                    │                    │                         │
│  orders table      │                    │  daily_metrics          │
│  50M+ rows         │                    │  (aggregated)           │
│                    │                    │                         │
│  idx_updated_at    │                    │  pipeline_state         │
│  idx_group_key     │                    │  (watermark)            │
│  idx_created_at    │                    │                         │
└────────────────────┘                    └─────────────────────────┘
```

**Source**: PostgreSQL — relational OLTP database with server-side cursor support  
**Target**: MongoDB — document store optimized for read-heavy reporting queries  
**Language**: Python 3.11+ with psycopg2, pymongo, Pydantic

**Source**: PostgreSQL — relational OLTP database with server-side cursor support  
**Target**: MongoDB — document store optimized for read-heavy reporting queries  
**Language**: Python 3.11+ with psycopg2, pymongo, Pydantic

### Why PostgreSQL → MongoDB?

Transactional (OLTP) and analytical workloads have different database requirements. PostgreSQL handles write-heavy order ingestion. MongoDB handles flexible, denormalized reporting documents. This separation avoids query contention between the application and reporting layer.

## Prerequisites

Before you begin, make sure the following are installed:

| Tool | Version | Purpose |
|------|---------|---------|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Latest | Runs PostgreSQL + MongoDB locally |
| [Python](https://www.python.org/downloads/) | 3.11+ | Runs the ETL pipeline |
| `make` | Any | Convenience wrapper for common commands |

> **Windows users**: `make` is not available by default. Install it via Git Bash (included with [Git for Windows](https://git-scm.com/download/win)), WSL, or `choco install make`. All shell commands below assume **Git Bash** or **WSL**. In PowerShell, use `$env:VAR = "value"` instead of `export VAR=value`.

## Quick Start

```bash
# 1. Start databases
docker compose up -d postgres mongodb

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate          # Git Bash / Linux / macOS
# .venv\Scripts\activate           # Windows PowerShell

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set environment variables
#    Copy the example file and edit if needed (defaults work with Docker Compose)
cp .env.example .env

export POSTGRES_DSN="postgresql://easyeat:easyeat_secret@localhost:5432/easyeat"
export MONGO_URI="mongodb://easyeat:easyeat_secret@localhost:27017"

# 5. Generate test data (10K rows by default)
python -m src.seed --count 10000

# 6. Run the pipeline
python -m src.pipeline
```

### Using Docker Compose (fully containerized)

```bash
docker compose up -d                     # Start everything
docker compose run etl python -m src.seed --count 100000
docker compose run etl                   # Run pipeline
```

### Using Make

```bash
make up          # Start databases
make seed        # Generate test data
make run         # Incremental run
make run-full    # Full historical rebuild
make run-backfill START_DATE=2024-01-01 END_DATE=2024-01-31  # Backfill a date range
make test        # Run all tests
make lint        # Lint + type check
```

## Pipeline Modes

### Incremental (default)
Processes only records changed since the last run. Uses `updated_at` as a watermark.

```bash
python -m src.pipeline
```

### Backfill
Reprocesses a specific date range. Does NOT affect the incremental watermark.

```bash
python -m src.pipeline --mode backfill \
    --start-date 2024-01-01 --end-date 2024-01-31

# Or with make (pass dates as make variables):
make run-backfill START_DATE=2024-01-01 END_DATE=2024-01-31
```

### Full Rebuild
Resets the watermark and reprocesses all historical data.

```bash
python -m src.pipeline --mode full
```

### Dry Run
Executes extract and transform stages but skips the load. Useful for validation.

```bash
python -m src.pipeline --dry-run
```

## Design Decisions

### 1. Recompute vs Increment ($set vs $inc)

**Decision**: Full recompute with `$set`.

When a batch of changed orders is detected, we don't just aggregate those rows. We identify the affected `(restaurant_id, date, country)` groups and re-query PostgreSQL for ALL orders in those groups, computing the complete aggregate.

**Why not `$inc`?** The `$inc` operator is not idempotent. If the pipeline re-runs on the same batch (e.g., after a crash where the watermark didn't advance), `$inc` would double-count. With `$set` + full recompute, running twice produces identical output.

**Tradeoff**: ~2x slower per batch due to the extra PostgreSQL round-trip. For a typical batch of 5,000 changed rows touching ~200 groups, the recompute query executes in milliseconds with the composite index. Correctness is worth the overhead.

### 2. Server-Side Cursors (not LIMIT/OFFSET)

**Decision**: psycopg2 named cursors with `itersize=5000`.

PostgreSQL holds the result set on the server and streams rows to Python in chunks. Only 5,000 rows (~1MB) are in memory at once. Peak memory is ~76MB regardless of dataset size (measured at 1M rows — stays flat at 50M).

**Why not LIMIT/OFFSET?** `OFFSET` is O(n) — the database must scan and discard rows on every page. At page 10,000 of a 50M row table, PostgreSQL scans 50M rows just to skip them. Keyset pagination (`WHERE updated_at > $last_seen`) is O(log n) via index seek on every page.

### 3. The "Affected Groups" Pattern

The core correctness mechanism. When order ORD-001 (created Jan 15) has its status changed on Jan 20:

1. **Extract**: Watermark picks up ORD-001 (updated_at changed)
2. **Identify**: Affected group = (rest_001, 2024-01-15, IN) — note: Jan 15, not Jan 20
3. **Recompute**: Query ALL orders for that group from PostgreSQL
4. **Load**: Upsert the corrected aggregate for Jan 15

This ensures historical metrics are always accurate, even when orders change status days later.

### 4. Index Strategy

Three indexes, each serving a specific query pattern:

| Index | Query Pattern | Mode |
|-------|--------------|------|
| `idx_orders_updated_at` | `WHERE updated_at > $watermark` | Incremental |
| `idx_orders_group_key` | `WHERE (restaurant_id, date, country) IN (...)` | Recompute |
| `idx_orders_created_at` | `WHERE created_at BETWEEN $start AND $end` | Backfill |

The `idx_orders_group_key` is the most critical and most commonly missed. Without it, the recompute query degrades to a sequential scan on 50M rows.

### 5. Watermark in MongoDB (not filesystem/Redis)

The watermark is stored in MongoDB's `pipeline_state` collection. This provides:
- **Persistence**: Survives container restarts
- **Atomicity**: MongoDB document-level locking
- **Co-location**: Same database as the target data
- **No extra infrastructure**: No Redis, no shared filesystem

### 6. No Airflow/Dagster

The pipeline is a standalone process invocable via CLI. Adding an orchestration framework would increase complexity without proportional benefit for a single-pipeline scope. The README documents Airflow/Dagster as a recommended next step for production deployment with multiple pipelines.

## Performance Characteristics

### Benchmarks

Measured on local Docker (Windows 11, 8GB RAM, 4 CPU cores, Python 3.10):

| Dataset  | Batches | Duration | Peak Memory | Unique Docs in MongoDB |
|----------|---------|----------|-------------|------------------------|
| 10K rows | 2       | ~2s      | ~76 MB      | 9,795                  |
| 1M rows  | 200     | 88s      | 75.9 MB     | ~270K (max possible)   |
| 50M rows | ~10,000 | ~75 min* | ~78 MB      | ~270K (max possible)   |

\* Extrapolated from 1M benchmark using O(log N) factor for index depth at scale.

**Key result**: Peak memory stays flat at ~76–78 MB regardless of dataset size.
Python never holds more than one batch (5,000 rows) in memory at any point —
PostgreSQL streams rows via server-side cursor.

### Time Complexity (per batch)

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| Extract query | O(log N + B) | N = total rows, B = batch size |
| Group identification | O(B) | Single pass through extracted rows |
| Recompute query | O(G × log N) | G = unique groups per batch |
| Bulk upsert | O(G) | One upsert per affected group |

### Memory Profile

| Data Structure | Size | Lifecycle |
|----------------|------|-----------|
| Extracted rows | ~1 MB | Released after group identification |
| Affected groups set | ~50 KB | Held until recompute |
| Recompute results | ~50 KB | Held until upsert |
| **Peak total** | **~76 MB** | **Constant regardless of dataset size** |

### Scaling Path (Future)

- **Increase BATCH_SIZE to 20,000**: ~2–3x throughput improvement (fewer round trips)
- **Partition by country**: 6 parallel workers → ~6x faster for 50M rows (~12 min)
- **Partition by restaurant_id range**: Hash-based sharding across N workers
- **Orchestration**: Migrate to Airflow/Dagster for scheduling, retries, and monitoring

## Configuration

All configuration is via environment variables. See `.env.example` for a ready-to-copy template.

| Variable | Default | Required | Description |
|----------|---------|----------|-------------|
| `POSTGRES_DSN` | — | Yes | PostgreSQL connection string |
| `MONGO_URI` | — | Yes | MongoDB connection URI |
| `MONGO_DB` | `easyeat` | No | MongoDB database name |
| `MONGO_METRICS_COLLECTION` | `daily_metrics` | No | Collection for aggregated metrics |
| `MONGO_STATE_COLLECTION` | `pipeline_state` | No | Collection for watermark / pipeline state |
| `BATCH_SIZE` | `5000` | No | Rows per extract batch (100–50000) |
| `CURSOR_ITERSIZE` | `5000` | No | PG server-side cursor fetch chunk size (100–50000) |
| `LOG_LEVEL` | `INFO` | No | Logging verbosity: `DEBUG` \| `INFO` \| `WARNING` \| `ERROR` \| `CRITICAL` |
| `PIPELINE_ID` | `daily_metrics_etl` | No | Unique identifier for this pipeline instance (used as the watermark key in MongoDB) |

## Testing

All tests run against real PostgreSQL and MongoDB instances — no mocks.

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test file
pytest tests/test_pipeline_e2e.py -v
```

### Test Coverage

| Test File | Coverage |
|-----------|----------|
| `test_extract.py` | Watermark filtering, batch sizing, backfill |
| `test_transform.py` | Group identification, aggregation correctness |
| `test_load.py` | Upsert insert/update, $set behavior |
| `test_idempotency.py` | Double-run safety, watermark prevents re-extraction |
| `test_pipeline_e2e.py` | Late status update, backfill isolation, dry run |

### The Critical Test: Late Status Update

```
1. Insert order ORD-001: status=placed, created_at=Jan 15
2. Run pipeline → Jan 15: total=1, completed=0, revenue=$0
3. Update ORD-001: status=completed, updated_at=Jan 20
4. Run pipeline → Jan 15: total=1, completed=1, revenue=$500 ✓
```

This test validates the entire "affected groups" recomputation pattern.

## Project Structure

```
easyeat-etl/
├── src/
│   ├── config.py              # Pydantic Settings (env vars)
│   ├── models.py              # Order + DailyMetric schemas
│   ├── db/
│   │   ├── postgres.py        # PG connection + server-side cursors
│   │   └── mongo.py           # MongoDB client + indexes
│   ├── extract/
│   │   └── extractor.py       # Watermark query + backfill
│   ├── transform/
│   │   └── transformer.py     # Group ID + SQL recomputation
│   ├── load/
│   │   └── loader.py          # Bulk upsert with $set
│   ├── watermark.py           # Read/write/reset checkpoint
│   ├── pipeline.py            # Orchestrator + CLI
│   └── seed.py                # Test data generator
├── tests/
├── docs/
│   ├── PRD_EasyEat_ETL_Pipeline.docx
│   └── TRD_EasyEat_ETL_Pipeline.docx
├── docker-compose.yml
├── Dockerfile
├── Makefile
└── README.md
```

## Failure Recovery

The pipeline is crash-safe at every point:

| Failure Point | Recovery |
|--------------|----------|
| During extract | Watermark unchanged → re-extracts same batch |
| During transform | Watermark unchanged → re-extracts and re-transforms |
| During load (partial) | Upserts overwrite partial writes with correct data |
| After load, before watermark update | Upserts produce identical output (idempotent) |
| After watermark update | Next batch proceeds normally |

The key insight: the watermark only advances AFTER a successful load. Combined with $set upserts, this makes every failure mode recoverable without data corruption.
