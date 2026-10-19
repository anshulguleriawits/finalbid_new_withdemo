#!/usr/bin/env python3
"""
export_csv.py — export the finalbid.com Postgres tables to CSV
===================================================================

Connects to the Postgres database and writes three timestamped CSV files:

    finalbid_data_<YYYYMMDD_HHMMSS>.csv      <- from the `lots` table
    bid_data_finalbid_<YYYYMMDD_HHMMSS>.csv  <- from the `bid_history` table
    lot_sales_finalbid_<YYYYMMDD_HHMMSS>.csv <- from the `lot_sales` table

Column names are expanded into readable names and the jsonb columns
(`insight`, `bid_history`) are flattened into ordinary CSV columns, so the
files open cleanly in Excel / pandas / Power BI without any JSON parsing.

DSN resolution: `--dsn`, then `$DSN`, then `DSN=` in `.env` next to this file,
then the local docker-compose default.

Requires:  pip install "psycopg[binary]"

Usage
-----
    python export_csv.py                          # export all three tables
    python export_csv.py --lots-only              # just the lots file
    python export_csv.py --bid-only               # just the bid-history file
    python export_csv.py --sales-only             # just the per-lot sale history
    python export_csv.py --include-specs          # also keep detail_specs JSON
    python export_csv.py --limit 500              # sample N rows per table
    python export_csv.py --dsn postgresql://user:pass@host:5432/db
    python export_csv.py --outdir D:\\exports
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

DEFAULT_DSN = "postgresql://auction:auction@localhost:5433/finalbid"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

# ---------------------------------------------------------------------------
# `lots` -> finalbid_data_<time>.csv
# ---------------------------------------------------------------------------
LOTS_SQL = """
SELECT
    l.id                        AS lot_id,
    l.source                    AS platform,
    l.lot_number                AS lot_number,
    l.vin                       AS vin,
    l.year                      AS model_year,
    l.make                      AS make,
    l.model                     AS model,
    l.trim                      AS trim,
    l.title_status              AS title_status,
    l.primary_damage            AS primary_damage,
    l.secondary_damage          AS secondary_damage,
    l.odometer                  AS odometer_miles,
    l.odometer_actual           AS odometer_actual,
    l.price_usd                 AS sale_price_usd,
    l.sale_date                 AS sale_date,
    l.location                  AS auction_location,
    l.engine                    AS engine,
    l.engine_displacement_l     AS engine_displacement_l,
    l.engine_power_hp           AS engine_power_hp,
    l.cylinders                 AS cylinders,
    l.fuel                      AS fuel,
    l.transmission              AS transmission,
    l.drive                     AS drive,
    l.vehicle_type              AS vehicle_type,
    l.body_color                AS body_color,
    l.interior_color            AS interior_color,
    l.condition                 AS condition,
    l.keys_present              AS keys_present,
    l.airbags                   AS airbags,
    l.loss_type                 AS loss_type,
    l.salvage_number            AS salvage_number,
    l.seller                    AS seller,
    l.sold_status               AS sold_status,
    -- the detail page fills the promoted columns; insight is the fallback for
    -- rows scraped before init/04-detail-fields.sql
    COALESCE(l.avg_final_bid,
             NULLIF(l.insight->>'avg_final_bid','')::int) AS avg_final_bid_usd,
    COALESCE(l.est_repair,
             NULLIF(l.insight->>'est_repair','')::int)    AS est_repair_usd,
    l.auctions_on_record        AS auctions_on_record,
    l.first_seen                AS first_seen_year,
    l.flagged                   AS flagged,
    l.suspect_price             AS suspect_price,
    l.scraped                   AS scraped,
    l.detailed                  AS detailed,
    l.history_captured          AS history_captured,
    l.detail_attempts           AS detail_attempts,
    l.detail_error              AS detail_error,
    COALESCE(l.retail_usd,
             NULLIF(l.insight->>'retail_usd','')::int)    AS estimated_retail_usd,
    CASE WHEN COALESCE(l.retail_usd,
                       NULLIF(l.insight->>'retail_usd','')::int) > 0
         THEN round(l.price_usd::numeric * 100
                    / COALESCE(l.retail_usd,
                               NULLIF(l.insight->>'retail_usd','')::int), 1)
    END                         AS pct_of_retail,
    l.public_token              AS public_token,
    l.url                       AS lot_url,
    l.created_at                AS source_created_at,
    l.ingested_at               AS ingested_at{SPECS_COL}
FROM lots l
ORDER BY l.sale_date DESC NULLS LAST, l.id
"""

# ---------------------------------------------------------------------------
# `bid_history` -> bid_data_finalbid_<time>.csv
# ---------------------------------------------------------------------------
BID_SQL = """
SELECT
    b.vin                    AS vin,
    b.sale_count             AS sale_count,
    b.has_bid_history        AS has_bid_history,
    b.latest_bid_price       AS latest_bid_price_usd,
    b.latest_bid_date        AS latest_bid_date,
    b.latest_lot_id          AS latest_lot_id,
    lp.platform              AS latest_platform,
    b.first_bid_price        AS first_bid_price_usd,
    b.first_bid_date         AS first_bid_date,
    b.first_lot_id           AS first_lot_id,
    fp.platform              AS first_platform,
    CASE WHEN b.first_bid_price IS NOT NULL AND b.first_bid_price > 0
         THEN round(((b.latest_bid_price::numeric - b.first_bid_price)
                      / b.first_bid_price) * 100, 1)
    END                      AS price_change_pct,
    x.first_sold_date        AS first_sold_date,
    x.last_sold_date         AS last_sold_date,
    x.platforms              AS platforms,
    x.lot_numbers            AS lot_numbers,
    x.bid_prices             AS bid_prices_usd,
    x.titles                 AS titles,
    x.damages                AS damages,
    x.statuses               AS statuses,
    b.bid_history::text      AS bids_json,
    b.created_at             AS record_created_at,
    b.updated_at             AS record_updated_at
FROM bid_history b
LEFT JOIN LATERAL (
    SELECT
        min((e->>'sold_date')::date)  AS first_sold_date,
        max((e->>'sold_date')::date)  AS last_sold_date,
        array_agg(DISTINCT e->>'platform') AS platforms,
        array_agg(e->>'lot_number')   AS lot_numbers,
        array_agg((e->>'price')::int) AS bid_prices,
        array_agg(e->>'title')        AS titles,
        array_agg(e->>'damage')       AS damages,
        array_agg(e->>'status')       AS statuses
    FROM jsonb_array_elements(COALESCE(b.bid_history, '[]'::jsonb)) AS e
) x ON TRUE
LEFT JOIN LATERAL (
    SELECT e->>'platform' AS platform
    FROM jsonb_array_elements(COALESCE(b.bid_history, '[]'::jsonb)) AS e
    ORDER BY (e->>'sold_date')::date DESC, e->>'lot_number' DESC
    LIMIT 1
) lp ON TRUE
LEFT JOIN LATERAL (
    SELECT e->>'platform' AS platform
    FROM jsonb_array_elements(COALESCE(b.bid_history, '[]'::jsonb)) AS e
    ORDER BY (e->>'sold_date')::date ASC, e->>'lot_number' ASC
    LIMIT 1
) fp ON TRUE
ORDER BY b.sale_count DESC, b.latest_bid_date DESC NULLS LAST, b.vin
"""

# ---------------------------------------------------------------------------
# `lot_sales` -> lot_sales_finalbid_<time>.csv
# ---------------------------------------------------------------------------
SALES_SQL = """
SELECT
    s.vin                AS vin,
    s.source             AS platform,
    s.lot_number         AS lot_number,
    s.sale_date          AS sale_date,
    s.final_bid          AS final_bid_usd,
    s.status             AS status,
    s.auctions_on_record AS auctions_on_record,
    s.first_seen         AS first_seen,
    s.ingested_at        AS ingested_at
FROM lot_sales s
ORDER BY s.vin, s.sale_date, s.lot_number
"""


def load_env(path: str = ENV_FILE) -> dict:
    """Read KEY=VALUE lines into a dict. Real environment vars win."""
    env: dict = {}
    try:
        with open(path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    for k in list(env):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def resolve_dsn(cli_dsn: str | None) -> str:
    """--dsn > $DSN > DSN= in .env > docker-compose default."""
    return (cli_dsn or os.environ.get("DSN")
            or load_env().get("DSN") or DEFAULT_DSN)


def connect(dsn: str):
    try:
        import psycopg
    except ImportError:
        print("psycopg is not installed. Run:\n    pip install \"psycopg[binary]\"",
              file=sys.stderr)
        sys.exit(2)
    try:
        conn = psycopg.connect(dsn, autocommit=True)
    except Exception as e:
        print(f"Could not connect to Postgres:\n  {e}", file=sys.stderr)
        print(f"\nIs the Postgres in DSN running?", file=sys.stderr)
        sys.exit(3)
    return conn


def export(conn, sql: str, path: str, label: str, specs_col: str = "",
           limit: int | None = None) -> int:
    """Run a COPY ... TO STDOUT CSV query straight into `path`."""
    sql = sql.replace("{SPECS_COL}", specs_col)
    if limit is not None:
        sql = f"SELECT * FROM ({sql.strip().rstrip(';')}) AS _q LIMIT {int(limit)}"
    print(f"[export] {label} -> {os.path.basename(path)}", flush=True)
    with conn.cursor() as cur:
        with cur.copy(f"COPY ({sql.strip().rstrip(';')}) TO STDOUT "
                      f"WITH (FORMAT CSV, HEADER TRUE)") as copy:
            with open(path, "w", newline="", encoding="utf-8-sig") as fh:
                for chunk in copy:
                    if isinstance(chunk, memoryview):
                        chunk = chunk.tobytes()
                    if isinstance(chunk, bytes):
                        chunk = chunk.decode("utf-8")
                    fh.write(chunk)
    with open(path, "r", newline="", encoding="utf-8-sig") as fh:
        rows = max(sum(1 for _ in fh) - 1, 0)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"[export] {label}: {rows:,} rows, {size_mb:,.1f} MB -> {path}", flush=True)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Export lots + bid_history + lot_sales to CSV")
    ap.add_argument("--dsn", default=None,
                    help="Postgres DSN (default: $DSN, then DSN= in .env)")
    ap.add_argument("--outdir", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--lots-only", action="store_true")
    ap.add_argument("--bid-only", action="store_true")
    ap.add_argument("--sales-only", action="store_true",
                    help="export only the lot_sales (per-lot sale history) file")
    ap.add_argument("--include-specs", action="store_true",
                    help="also emit the verbatim detail_specs JSON column")
    ap.add_argument("--limit", type=int, default=None,
                    help="export at most N rows per table (smoke test / sample)")
    args = ap.parse_args()

    args.dsn = resolve_dsn(args.dsn)

    os.makedirs(args.outdir, exist_ok=True)

    lots_path = os.path.join(args.outdir, f"finalbid_data_{STAMP}.csv")
    bid_path = os.path.join(args.outdir, f"bid_data_finalbid_{STAMP}.csv")
    sales_path = os.path.join(args.outdir, f"lot_sales_finalbid_{STAMP}.csv")

    print(f"[db] connecting to {args.dsn}")
    conn = connect(args.dsn)

    print(f"[db] server time: {_scalar(conn, 'SELECT now()')}")
    print(f"[db] lots rows:       {_scalar(conn, 'SELECT count(*) FROM lots')}")
    print(f"[db] bid_history rows:{_scalar(conn, 'SELECT count(*) FROM bid_history')}")
    print(f"[db] lot_sales rows:  {_scalar(conn, 'SELECT count(*) FROM lot_sales')}")

    # bid_history is derived; catch the "dropped DB / run stopped early" case so
    # the export doesn't silently write an empty bid-history file.
    if not args.lots_only and _scalar(conn, "SELECT count(*) FROM bid_history") == "0" \
            and _scalar(conn, "SELECT count(*) FROM lots") != "0":
        print("\n  !! bid_history is empty but lots is not. Rebuild it first:\n"
              "       python run_scraper.py --refresh\n")
    print()

    specs_col = (",\n    l.detail_specs::text AS detail_specs_json"
                 if args.include_specs else "")

    if not (args.bid_only or args.sales_only):
        export(conn, LOTS_SQL, lots_path, "lots", specs_col, args.limit)
    if not (args.lots_only or args.sales_only):
        export(conn, BID_SQL, bid_path, "bid_history", limit=args.limit)
    if not (args.lots_only or args.bid_only):
        export(conn, SALES_SQL, sales_path, "lot_sales", limit=args.limit)

    conn.close()
    print("\n[done]")
    return 0


def _scalar(conn, sql: str) -> str:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        return str(row[0]) if row else "?"


if __name__ == "__main__":
    sys.exit(main())