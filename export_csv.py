#!/usr/bin/env python3
"""
export_csv.py — export the finalbid.com Postgres tables to CSV
===================================================================

Connects to the Postgres database and writes two timestamped CSV files:

    finalbid_data_<YYYYMMDD_HHMMSS>.csv   <- from the `lots` table
    bid_data_finalbid_<YYYYMMDD_HHMMSS>.csv <- from the `bid_history` table

Column names are expanded into readable names and the jsonb columns
(`insight`, `photos`, `bid_history`) are flattened into ordinary CSV columns,
so the files open cleanly in Excel / pandas / Power BI without any JSON parsing.

Requires:  pip install "psycopg[binary]"

Usage
-----
    python export_csv.py                          # export both tables
    python export_csv.py --lots-only              # just the lots file
    python export_csv.py --bid-only               # just the bid-history file
    python export_csv.py --include-raw            # also keep the full raw JSON
    python export_csv.py --dsn postgresql://user:pass@host:5432/db
    python export_csv.py --outdir D:\\exports
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime

DEFAULT_DSN = "postgresql://auction:auction@localhost:5433/finalbid"
PHOTO_BASE = "https://api.finalbid.com/photos/"
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

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
    l.avg_final_bid             AS avg_final_bid_usd,
    l.est_repair                AS est_repair_usd,
    l.auctions_on_record        AS auctions_on_record,
    l.first_seen                AS first_seen_year,
    l.flagged                   AS flagged,
    l.suspect_price             AS suspect_price,
    l.scraped                   AS scraped,
    l.detailed                  AS detailed,
    l.history_captured          AS history_captured,
    l.retail_usd                AS estimated_retail_usd,
    NULLIF(l.insight->>'pct','')::int       AS pct_of_retail,
    NULLIF(l.insight->>'avg_usd','')::int   AS pool_avg_price_usd,
    NULLIF(l.insight->>'top','')::int       AS price_percentile,
    NULLIF(l.insight->>'mpy','')::int       AS miles_per_year,
    NULLIF(l.insight->>'avg_mpy','')::int   AS archive_avg_miles_per_year,
    NULLIF(l.insight->>'n','')::bigint      AS archive_size_at_insight,
    NULLIF(l.insight->>'pool','')           AS insight_pool,
    NULLIF(l.insight->>'at','')::date       AS insight_generated_date,
    COALESCE(jsonb_array_length(l.photos), 0) AS photo_count,
    p.photo_urls                          AS photo_urls,
    l.url                          AS lot_url,
    l.created_at                   AS source_created_at,
    l.ingested_at                  AS ingested_at
    {RAW_COL}
FROM lots l
LEFT JOIN LATERAL (
    SELECT string_agg('{PHOTO_BASE}' || x, ' ') AS photo_urls
    FROM jsonb_array_elements_text(COALESCE(l.photos, '[]'::jsonb)) AS x
) p ON TRUE
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
        array_agg(e->>'damage')       AS damages
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


def export(conn, sql: str, path: str, label: str, raw_col: str = "") -> int:
    """Run a COPY ... TO STDOUT CSV query straight into `path`."""
    sql = sql.replace("{RAW_COL}", raw_col)
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
    ap = argparse.ArgumentParser(description="Export lots + bid_history to CSV")
    ap.add_argument("--dsn", default=os.environ.get("DSN", DEFAULT_DSN))
    ap.add_argument("--outdir", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--lots-only", action="store_true")
    ap.add_argument("--bid-only", action="store_true")
    ap.add_argument("--sales-only", action="store_true",
                    help="export only the lot_sales (per-lot sale history) file")
    ap.add_argument("--include-raw", action="store_true",
                    help="also emit the full source record as a raw_json column")
    args = ap.parse_args()

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

    raw_col = ",\n    l.raw::text AS raw_json" if args.include_raw else ""

    if not (args.bid_only or args.sales_only):
        export(conn, LOTS_SQL, lots_path, "lots", raw_col)
    if not (args.lots_only or args.sales_only):
        export(conn, BID_SQL, bid_path, "bid_history")
    if not (args.lots_only or args.bid_only):
        export(conn, SALES_SQL, sales_path, "lot_sales")

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