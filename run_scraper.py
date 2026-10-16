#!/usr/bin/env python3
"""
run_scraper.py — config-driven launcher for finalbid_placeholder.py
======================================================================

Reads configuration from `.env` (and any real environment variables, which
take precedence), builds the right scraper command, and runs it.

    python run_scraper.py                # use .env as-is
    python run_scraper.py --make TOYOTA  # override one thing on the fly
    python run_scraper.py --shard        # run MAKES=... one make at a time
    python run_scraper.py --init-db      # just (re)apply init/*.sql + show counts
    python run_scraper.py --stats        # current row counts, then exit

Because every run is tracked in `scrape_pages` and resumes automatically, it is
safe to run this repeatedly: finished pages are never re-fetched.

All database access goes through the `DSN` connection string with psycopg — there
is no Docker dependency. The Postgres server (native install, container, or hosted
instance) is managed separately; this script only checks that it is reachable.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / ".env"
SCRAPER = HERE / "scrape_finalbid.py"
INIT_DIR = HERE / "init"


# ---------------------------------------------------------------------------
# .env loading (no third-party dependency)
# ---------------------------------------------------------------------------
def load_env(path: Path = ENV_FILE) -> dict:
    """Read KEY=VALUE lines into a dict. Existing env vars win."""
    env: dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    # real environment overrides .env
    for k in list(env):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


def truthy(v) -> bool:
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def get(env: dict, key: str, default: str = "") -> str:
    """Config value: .env first, then the real environment, then `default`.

    The os.environ fallback matters when there is no .env at all (e.g. DSN
    exported by CI or a service manager) — load_env only merges variables whose
    key is already present in the file.
    """
    v = env.get(key)
    if v is None:
        v = os.environ.get(key)
    return default if v is None else str(v)


# ---------------------------------------------------------------------------
# Argument building
# ---------------------------------------------------------------------------
# SCRAPE_X in .env  ->  --x  on the scraper CLI
SCRAPE_FLAGS = [
    ("SCRAPE_Q", "--q"),
    ("SCRAPE_MAKE", "--make"),
    ("SCRAPE_MODEL", "--model"),
    ("SCRAPE_TITLE", "--title"),
    ("SCRAPE_DAMAGE", "--damage"),
    ("SCRAPE_SOURCE", "--source"),
    ("SCRAPE_VIN", "--vin"),
    ("SCRAPE_PRICE_MIN", "--price-min"),
    ("SCRAPE_PRICE_MAX", "--price-max"),
]


def build_args(env: dict, overrides: dict | None = None) -> list[str]:
    """Assemble the scraper CLI from the env config + CLI overrides."""
    o = overrides or {}
    argv: list[str] = [sys.executable, str(SCRAPER)]

    # filters
    for env_key, flag in SCRAPE_FLAGS:
        val = o.get(env_key) if env_key in o else get(env, env_key)
        if val:
            argv += [flag, str(val)]

    # scrape behaviour
    def opt(env_key: str, flag: str, default=None, cast=str):
        val = o.get(env_key) if env_key in o else get(env, env_key, "")
        if val == "" or val is None:
            val = default
        if val not in ("", None):
            argv.extend([flag, str(cast(val))])

    opt("WORKERS", "--workers", 3, int)
    opt("CONCURRENCY", "--concurrency", 12, int)
    opt("PER_PAGE", "--per-page", 100, int)
    opt("SPACING", "--spacing", 0.05, float)
    opt("RETRIES", "--retries", 5, int)
    opt("TIMEOUT", "--timeout", 45, float)
    opt("LIMIT", "--limit", 0, int)
    opt("OUT", "--out", "")

    opt("OVERLAP", "--overlap", 15, int)
    opt("START_PAGE", "--start-page", 0, int)
    opt("STOP_AFTER", "--stop-after", 2, int)
    opt("MAX_ERROR_STREAK", "--max-error-streak", 5, int)
    opt("BID_HISTORY_MINUTES", "--bid-history-minutes", 5, float)
    opt("SLOW_SECONDS", "--slow-seconds", 3, float)
    opt("RATE_EVERY", "--rate-every", 10, int)
    opt("DETAIL_RETRY_PASSES", "--detail-retry-passes", 1, int)

    full = truthy(o.get("FULL", get(env, "FULL", "0")))
    fresh = truthy(o.get("FRESH", get(env, "FRESH", "0")))
    re_detail = truthy(o.get("RE_DETAIL", get(env, "RE_DETAIL", "0")))
    if full and not re_detail:
        argv.append("--full")

    # sink / tracking
    if truthy(o.get("LOAD_DB", get(env, "LOAD_DB", "1"))):
        argv.append("--load-db")
    if truthy(o.get("TRACK_PAGES", get(env, "TRACK_PAGES", "1"))):
        argv.append("--track-pages")

    # modes: --re-detail > --full > --fresh > --resume (default)
    if truthy(o.get("RE_DETAIL", get(env, "RE_DETAIL", "0"))):
        argv.append("--re-detail")
        opt("RE_DETAIL_BATCH", "--re-detail-batch", 250, int)
    else:
        if fresh:
            argv.append("--fresh")
        if not full and not fresh:
            argv.append("--resume")

    # dsn
    dsn = o.get("DSN") or get(env, "DSN")
    if dsn:
        argv += ["--dsn", dsn]

    # refresh bid_history only on the final run of a sequence
    if truthy(o.get("REFRESH_BID_HISTORY", get(env, "REFRESH_BID_HISTORY", "1"))):
        argv.append("--refresh-bid-history")
    if truthy(o.get("DETAIL", get(env, "DETAIL", "0"))):
        argv.append("--detail")

    return argv


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run_scraper(argv: list[str]) -> int:
    print("\n\033[1m>> " + " ".join(argv[2:]) + "\033[0m", flush=True)
    return subprocess.call(argv)


def connect(env: dict):
    """Open a psycopg connection to the Postgres named by DSN.

    DSN is a plain connection string (`postgresql://user:pass@host:port/db`) —
    a native server, a container published on a port, or a hosted instance;
    nothing here is Docker-specific. Returns None if psycopg is missing or the
    server refuses the connection.
    """
    dsn = get(env, "DSN")
    if not dsn:
        print("  DSN is not set in .env (or the environment) — cannot reach Postgres.")
        return None
    try:
        import psycopg
    except ImportError:
        print('  psycopg is not installed. Run:\n    pip install "psycopg[binary]"',
              file=sys.stderr)
        return None
    try:
        conn = psycopg.connect(dsn, autocommit=True, connect_timeout=10)
    except Exception as e:
        print(f"  cannot connect to Postgres: {e}")
        return None
    return conn


def query(env: dict, sql: str):
    """Run one SQL statement over DSN; rows as psql-style text ('|' separated).

    None on any failure and "" for a non-SELECT, so callers keep their
    `if out:` success checks.
    """
    conn = connect(env)
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:      # DDL / non-SELECT: nothing to show
                return ""
            rows = cur.fetchall()
        return "\n".join(
            "|".join("" if v is None else str(v) for v in r) for r in rows
        ).strip()
    except Exception as e:
        print(f"  SQL failed: {e}", file=sys.stderr)
        return None
    finally:
        conn.close()


def refresh_bid_history(env: dict) -> None:
    """Rebuild the per-VIN bid_history table from lots (SQL only, no scraping)."""
    print("\n\033[1m########## final: refresh bid_history ##########\033[0m")
    out = query(env, "SELECT refresh_bid_history();")
    if out:
        print(f"  bid_history rebuilt ({out.splitlines()[-1].strip()} VINs)")
    else:
        print("  (refresh failed — is the DB up and is refresh_bid_history() defined?)")


def show_stats(env: dict) -> None:
    print("\n\033[1m=== database ===\033[0m")
    counts = query(env,
        "SELECT 'lots        '||count(*) FROM lots "
        "UNION ALL SELECT 'undetailed   '||count(*) FROM lots WHERE NOT (detailed AND history_captured) "
        "UNION ALL SELECT 'lot_sales    '||count(*) FROM lot_sales "
        "UNION ALL SELECT 'scrape_pages '||count(*) FROM scrape_pages "
        "UNION ALL SELECT 'bid_history  '||count(*) FROM bid_history "
        "UNION ALL SELECT 'multi-sale   '||count(*) FROM bid_history WHERE has_bid_history;")
    print(counts if counts else "  (could not query — is the DB up?)")
    progress = query(env, "SELECT * FROM scrape_progress;")
    if progress:
        print("\n\033[1m=== crawl progress ===\033[0m")
        for line in progress.splitlines():
            print(" ", line)


def init_db(env: dict) -> None:
    """Apply init/*.sql over the DSN connection (ordered by name, idempotent)."""
    files = sorted(INIT_DIR.glob("*.sql"))
    if not files:
        print(f"  no .sql files in {INIT_DIR}")
    conn = connect(env)
    if conn is None:
        return
    print("\033[1m== applying init/*.sql ==\033[0m")
    try:
        for f in files:
            sql = f.read_text(encoding="utf-8")
            try:
                with conn.cursor() as cur:
                    # No bind parameters -> libpq's simple query protocol, which
                    # (unlike the extended one) accepts a whole multi-statement
                    # script in a single round trip.
                    cur.execute(sql)
                print(f"  {f.name}: OK")
            except Exception as e:
                print(f"  {f.name}: FAILED")
                print(f"    {e}")
    finally:
        conn.close()
    # bid_history is derived and dropped by 02; rebuild it so the DB is usable
    # (and an immediate export is not empty) right after the schema is applied.
    out = query(env, "SELECT refresh_bid_history();")
    if out:
        print(f"  bid_history rebuilt ({out.splitlines()[-1].strip()} VINs)")
    show_stats(env)


# ---------------------------------------------------------------------------
# Is the Postgres in DSN reachable?
# ---------------------------------------------------------------------------
def db_query_ok(env: dict) -> bool:
    """Real check: can we run a trivial query over DSN?"""
    conn = connect(env)
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone()[0] == 1
    except Exception:
        return False
    finally:
        conn.close()


def ensure_db(env: dict, wait_seconds: int = 0) -> bool:
    """Check that the Postgres in DSN answers; optionally wait for it to come up.

    The server itself is managed outside this script (native install, Docker,
    or a hosted instance) — we only make sure it is reachable before the
    scraper starts writing to it.
    """
    if db_query_ok(env):
        return True
    if not get(env, "DSN"):
        return False
    if wait_seconds <= 0:
        return False
    print(f"\033[1m[db] waiting up to {wait_seconds}s for Postgres...\033[0m")
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        time.sleep(2.0)
        if db_query_ok(env):
            print("  Postgres is ready.")
            return True
    print("  Postgres did not become ready in time.")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description="Run the FinalBid scraper from .env")
    ap.add_argument("--make")
    ap.add_argument("--model")
    ap.add_argument("--source")
    ap.add_argument("--title")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--workers", type=int)
    ap.add_argument("--concurrency", type=int,
                    help="parallel detail requests (shared keep-alive pool)")
    ap.add_argument("--detail", action="store_true")
    ap.add_argument("--resume", action="store_true",
                    help="top-up newest pages, then continue from last page - OVERLAP (default)")
    ap.add_argument("--fresh", "--new-data", dest="fresh", action="store_true",
                    help="scan from page 1 for new lots; stop at the first repeated page")
    ap.add_argument("--full", action="store_true",
                    help="walk page 1 to the end, ignoring the checkpoint (keeps data)")
    ap.add_argument("--start-page", type=int,
                    help="start the archive walk at this page (overrides resume/full)")
    ap.add_argument("--shard", action="store_true",
                    help="run each make in MAKES= one after another")
    ap.add_argument("--stats", action="store_true", help="show DB counts and exit")
    ap.add_argument("--refresh", action="store_true",
                    help="rebuild bid_history from lots, then exit")
    ap.add_argument("--init-db", action="store_true", help="apply init/*.sql then show counts")
    ap.add_argument("--re-detail", action="store_true",
                    help="re-fetch lot pages for rows whose detail is missing, "
                         "straight from the DB (backfill after a parser change)")
    ap.add_argument("--re-detail-batch", type=int,
                    help="rows pulled from the DB per --re-detail batch")
    ap.add_argument("--db-wait", type=int, default=0, metavar="SECONDS",
                    help="if the Postgres in DSN is not answering yet, retry for "
                         "this many seconds before giving up (0 = fail fast)")
    args = ap.parse_args()

    env = load_env()

    # Make sure the DB is reachable before doing anything that needs it.
    needs_db = True  # everything except --count flows through the DB
    db_wait = args.db_wait or int(get(env, "DB_WAIT", "0") or 0)
    if needs_db and not ensure_db(env, db_wait):
        print("\033[31mCannot reach the Postgres in DSN. Check DSN in .env "
              "(or the environment) and that the server is running.\033[0m",
              file=sys.stderr)
        return 3

    if args.init_db:
        init_db(env)
        return 0
    if args.refresh:
        refresh_bid_history(env)
        show_stats(env)
        return 0
    if args.stats:
        show_stats(env)
        return 0

    overrides = {k: v for k, v in {
        "SCRAPE_MAKE": args.make,
        "SCRAPE_MODEL": args.model,
        "SCRAPE_SOURCE": args.source,
        "SCRAPE_TITLE": args.title,
        "LIMIT": args.limit,
        "WORKERS": args.workers,
        "CONCURRENCY": args.concurrency,
        "FULL": "1" if args.full else None,
        "START_PAGE": args.start_page,
        "RE_DETAIL": "1" if args.re_detail else None,
        "RE_DETAIL_BATCH": args.re_detail_batch,
    }.items() if v is not None}
    if args.detail:
        overrides["DETAIL"] = "1"
    if args.fresh:
        overrides["FRESH"] = "1"

    # ---- sharded mode: one run per make ----------------------------------
    makes = get(env, "MAKES")
    if args.shard:
        if not makes:
            print("MAKES is empty in .env — nothing to shard. "
                  "Example: MAKES=TOYOTA,FORD,HONDA", file=sys.stderr)
            return 2
        targets = [m.strip() for m in makes.split(",") if m.strip()]
        print(f"\033[1m== sharded run over {len(targets)} make(s): "
              f"{', '.join(targets)} ==\033[0m")
        failures = 0
        for i, make in enumerate(targets, 1):
            print(f"\n\033[1m########## [{i}/{len(targets)}] {make} ##########\033[0m")
            o = dict(overrides)
            o["SCRAPE_MAKE"] = make
            # refresh bid_history only once, at the very end
            o["REFRESH_BID_HISTORY"] = "0"
            rc = run_scraper(build_args(env, o))
            if rc != 0:
                failures += 1
                print(f"    !! {make} exited {rc}")
        # Rebuild bid_history once at the end — do NOT re-scrape for this.
        if truthy(get(env, "REFRESH_BID_HISTORY", "1")):
            refresh_bid_history(env)
        show_stats(env)
        return 1 if failures else 0

    # ---- single run -------------------------------------------------------
    rc = run_scraper(build_args(env, overrides))
    show_stats(env)
    return rc


if __name__ == "__main__":
    sys.exit(main())