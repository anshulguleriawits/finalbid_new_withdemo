# FinalBid.vin Scraper

Scrapes sold Copart/IAAI lots from [finalbid.vin](https://finalbid.vin/en/archive)
(newest-first) and stores them in Postgres (or JSONL). Each lot can be enriched
from its detail page: photos / engine / fuel plus the page's
**"Sale history of this VIN"** table (every re-listing of that lot).

## Requirements

- Python 3.10+ and `pip install -r requirements.txt` (`httpx` + `psycopg[binary]`)
- A reachable Postgres 12+. **The connection string in `DSN=` is all that is
  needed** — a native install, a container published on a port, or a hosted
  instance all work. `docker-compose.yml` is included as one convenient option,
  but nothing in the code depends on Docker.

## Database connection

```bash
# .env (copy .env.example)
DSN=postgresql://user:password@localhost:5432/finalbid
```

`run_scraper.py` connects to `DSN` directly with psycopg for `--init-db`,
`--stats`, `--refresh` and the crawler itself. If the server is not up yet,
`python run_scraper.py --db-wait 60` (or `DB_WAIT=60`) waits for it instead of
failing immediately. Real environment variables override `.env`.

## Quick start

```bash
cd "/Users/anshul/Desktop/Cars Scraper/FinalBid"
source "/Users/anshul/Desktop/Cars Scraper/.venv/bin/activate"
pip install -r requirements.txt

# lighter: JSONL only, no DB
python scrape_finalbid.py --out data/finalbid.jsonl --limit 100

# with Postgres
python run_scraper.py --init-db        # apply init/*.sql (schema + lot_sales + flags)
python run_scraper.py                  # resume: top-up newest, then deep-crawl
python export_csv.py                   # lots + bid_history + lot_sales CSVs
```

## Demo mode (scrape for real, no database)

```bash
python demo.py                       # LIVE: 50 lots from finalbid.vin, no DB
python run_scraper.py --demo --demo-limit 200
python demo.py --limit 200           # more lots
python demo.py --limit 20 --no-detail   # listing rows only (1 req/lot)
python demo.py --out data/demo.jsonl    # keep the raw rows
python demo.py --offline              # synthetic pages, no network at all
```

Live demo mode runs the real scraper with **every database feature switched
off** — no `--load-db`, no `--track-pages`, no `--refresh-bid-history`, no
DSN — so nothing can touch Postgres even if a DSN is configured. Lots are
mirrored to a JSONL file (`data/demo_live_<timestamp>.jsonl`, truncated at the
start of each run), then summarised: a sample table, a fill-rate bar per
detail column, and counts for price / odometer / sale-history rows.

With no `scrape_pages` checkpoint there is nothing to resume from, so a demo
run walks from page 1 and stops at `--limit`. The limit is applied after each
page's detail phase, so the effective count is rounded up to a page boundary
(24 lots/page).

`--offline` swaps in synthetic pages and needs neither the network, Postgres
nor `httpx` — a `DemoClient` with the same `get_text` / `fetch_many` shape as
`AsyncFetcher` serves fixtures to the same parsers. The fixtures deliberately
include the awkward cases the real site produces: a 14-character VIN, a card
whose `copart-<n>[-<vin>]` key cannot be parsed (must be skipped, not written
with a blank source), a `0 mi · not actual` odometer, and a detail page with no
ld+json block.

## Modes

| Command | Behaviour |
|---|---|
| `python run_scraper.py` | **resume (default).** Top-up the newest pages, then continue the deep crawl from `(last page − OVERLAP)` to the end of the archive. |
| `python run_scraper.py --fresh` | Scan from page 1 and stop at the first repeated page — cheap new-arrivals check. Uses its own checkpoint, so it never disturbs the deep crawl. |
| `python run_scraper.py --full` | Walk page 1 to the end, ignoring the checkpoint. Upserts; nothing is deleted. |
| `python run_scraper.py --detail` | Fetch lot detail pages (photos/engine + sale history). |
| `python run_scraper.py --start-page 900` | Start the archive walk at page 900 instead of page 1 / the resume checkpoint. Overrides `--resume` / `--fresh` / `--full`. Also settable as `START_PAGE=` in `.env`. |
| `python run_scraper.py --re-detail` | Backfill: re-fetch the lot page of every row whose detail is missing, straight from the DB — no archive walk. Use after a parser change (see Notes). |

## How a lot is handled

- `lots.scraped` — set once the row is persisted from a listing page.
- `lots.detailed` / `lots.history_captured` — set once the detail page was
  fetched and its sale-history table parsed.
- Both flags are **sticky / monotonic**: the upsert only ever turns them on
  (`x = lots.x OR EXCLUDED.x`). A plain listing re-fetch emits them as `false`,
  and a detail fetch that throws leaves them `false`, so without the OR the next
  page load would silently clear them and every run would re-detail the same
  lots. Keep both flags in the sticky set in `Db.load_lots`.
- The detail phase fetches a lot only when `NOT (detailed AND history_captured)`.
  A page of 24 with 10 already done fires just 14 detail requests, and a fully
  done page prints `detail skipped: page=N lots=24 all done` and makes 0 requests.
- Parsed sale rows land in **`lot_sales`** `(source, lot_number, sale_date)`.
- `bid_history` is generated **per detailed lot, immediately**: right after a
  page's lots are persisted, `refresh_bid_history(vins)` rebuilds the row for
  each VIN just detailed, from that detail page's own sale data (`lots +
  lot_sales`; captured history wins per lot, so earlier re-listing bids are not
  lost). Log line: `bid_history: page=N vins=K`.
- A full sweep still runs every `BID_HISTORY_MINUTES` (default 5) and once at the
  end as a safety net (also garbage-collects VINs that no longer exist). Ctrl-C
  finishes in-flight pages, rebuilds, then exits. `run_scraper.py --refresh`
  forces a full sweep.

## Detail-page fields

Every field on a lot page is parsed from its `<dt>/<dd>` spec list (plus the
`application/ld+json` block) into `lots`:

| Lot page | Column |
|---|---|
| Sale document (`Salvage title` → `salvage`) | `title_status` |
| Sale status | `sold_status` |
| Location | `location` |
| Odometer (`92,047 mi`, `0 mi · not actual`) | `odometer`, `odometer_actual` |
| Primary / secondary damage | `primary_damage`, `secondary_damage` |
| Engine (`2.4L · 4 cyl · 260 hp`) | `engine`, `cylinders`, `engine_displacement_l`, `engine_power_hp` |
| Fuel, Transmission, Drive, Vehicle type | `fuel`, `transmission`, `drive`, `vehicle_type` |
| Body color / Interior color | `body_color`, `interior_color` |
| Condition, Loss type | `condition`, `loss_type` |
| Keys, Airbags (`Yes`/`No`) | `keys_present`, `airbags` |
| Salvage #, Seller | `salvage_number`, `seller` |
| Actual cash value | `retail_usd` (+ `insight.retail_usd`) |
| Avg final bid, Est. repair | `avg_final_bid`, `est_repair` |
| Auctions on record, First seen | `auctions_on_record`, `first_seen` |
| Photos | `photos` |
| Every spec row, verbatim | `detail_specs` |

Trims come from the ld+json `name` ("2018 Ford Focus SE" → `SE`), falling back to
the leftover words of the listing `model` after its URL slug. `detail_specs`
keeps the verbatim `{label: value}` map, so nothing the page shows is lost.

**Data columns never downgrade.** `Db.load_lots` writes every data column as
`COALESCE(EXCLUDED.col, lots.col)` and protects `photos` from a listing re-walk's
`[]`, so a plain listing re-fetch can't blank a field the detail page filled
(a non-NULL value still overwrites, so real updates land). Columns are added by
`init/04-detail-fields.sql` (idempotent; applied by `run_scraper.py --init-db`).

## Flags

- `--detail` fetch lot pages for JSON-LD + sale history (1 request/lot).
- `--concurrency N` parallel detail requests over one keep-alive pool
  (default 12; this is the throughput knob — see Performance).
- `--workers N` parallel listing-page prefetchers (cheap, 1 request each).
- `--spacing S` minimum gap between request starts, whole run (default 0.05s).
- `--detail-retry-passes N` extra concurrent passes for lots whose detail failed
  (default 1). Still-failing lots stay un-detailed and are refetched next run.
- `--limit N` stop after N lots.
- `--overlap N` pages to re-scan before a resume point (default 15).
- `--stop-after N` consecutive repeated pages that end a top-up/fresh pass (default 2).
- `--bid-history-minutes N` periodic `bid_history` rebuild interval (default 5).
- `--slow-seconds S` flag any single request slower than this (default 3).
- `--rate-every N` print a rolling throughput line every N pages (default 10).
- `--out path` append a JSONL mirror; `--dsn` override the DB URL.
- `--track-pages --load-db` enable DB features (passed by `run_scraper.py`).

## Performance

Requests go through one `httpx.AsyncClient` on a background event loop with a
shared keep-alive connection pool (a sync bridge keeps the rest of the scraper
simple). Per-request TCP+TLS setup and server-side connection serialisation are
what made the old urllib/thread path slow.

Measured on the detail phase (24 lots/page):

| path                         | req/s | 24 details |
|------------------------------|-------|------------|
| urllib + threads (old)       | ~0.7  | 15–60 s    |
| httpx async, concurrency 12  | ~12   | ~2 s       |

Tuning: `CONCURRENCY` is the main lever; the site saturates around 13 req/s, so
values beyond ~16–20 stop helping (and get throttled). `SPACING` caps the whole
run's request rate; keep it small (0.05). A slow detail page shows up as
`SLOW detail[...]` and a dropped one as `FAIL detail[...]`.

## Retrying failed details

A detail fetch that fails leaves `detailed`/`history_captured` false (the upsert
only ever turns them on), so the lot stays in `detail_todo_ids` and the next run
picks it up automatically. Failures also record `lots.detail_attempts` (count,
reset to 0 on success) and `lots.detail_error` (last error), so they can be
queried, e.g.:

```sql
-- any psql session pointed at DSN=
SELECT id, detail_attempts, detail_error FROM lots
 WHERE detail_error IS NOT NULL ORDER BY detail_attempts DESC;
```

The run summary reports `fail=<n>` and `undetailed=<n>` so you can see how much
is still outstanding.

## Export

```bash
python export_csv.py                 # all three files
python export_csv.py --lots-only
python export_csv.py --bid-only
python export_csv.py --sales-only    # just the per-lot sale history
python export_csv.py --include-raw
```

Writes timestamped CSVs from `lots`, `bid_history` and `lot_sales`.

## Notes

- Archive: `https://finalbid.vin/en/archive?page=N`, ~24 lots/page.
- DB is separate from AuctionHistory's `auctionhistory` database (`finalbid` DB
  on the same instance, port 5433).
- Detail pages are allowed by robots.txt; `--detail` is polite but slower.
- Backfill: rows stored before the sale-history feature have
  `history_captured = false`, so the next `--detail` run re-fetches them once to
  fill `lot_sales`, then skips them thereafter.
- Detail-page fields: rows scraped before `init/04-detail-fields.sql` exist have
  `detailed = true` but the new columns empty, so the normal resume walk will
  never re-fetch them (it only walks forward from the checkpoint). Backfill them
  with the DB-driven mode — it ignores the archive entirely and walks the rows
  themselves, so it is bounded, resumable and unaffected by the live archive
  re-paginating:
  ```bash
  psql ... -c "UPDATE lots SET detailed = false;"   # queue everything
  python run_scraper.py --re-detail                 # or --re-detail --limit 500
  ```
  Rows are marked `detailed` as they succeed, so Ctrl-C / a reboot just means
  re-running the same command. Throughput is ~8 lots/s at the default
  `--concurrency 12`, so the full 312k table takes ~10–12 h. `--re-detail-batch`
  (default 250) sizes the DB batch; every data column is `COALESCE`-protected, so
  a partial sweep keeps whatever it filled.
- The archive is **not** bounded: `?page=40000` still returns lots. A `--full`
  walk therefore never terminates on its own — use `--start-page`, `--limit`, or
  `--re-detail` when you want a finite pass.
- **VINs are not always 17 chars.** Some lots (older / re-tagged vehicles) have a
  shorter VIN, e.g. `copart-68151656-11603312069222` (14). `lots.vin` /
  `lot_sales.vin` / `bid_history.vin` are `text`, and the archive parser reads
  the `<source>-<lot>[-<vin>]` key from the card/URL without assuming a length.
  A card whose key cannot be parsed is skipped with a warning rather than being
  written with a blank `source` (which fails `lots_source_chk` and would drop
  the whole page's batch).
