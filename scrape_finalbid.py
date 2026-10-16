#!/usr/bin/env python3
"""FinalBid.vin archive scraper — parallel, retried, resumable, Postgres sink.

Modes
-----
* default / --resume : top-up the newest pages, then continue the deep crawl
                       from `last_page - OVERLAP` to the end of the archive.
* --fresh            : scan from page 1 and stop at the first page with no new
                       lots (picks up new arrivals cheaply).
* --full             : walk page 1 to the end, ignoring the checkpoint
                       (existing rows are upserted, nothing is deleted).

Per lot: the listing fills the base columns; the detail page fills photos /
engine and the "Sale history of this VIN" table (-> lot_sales). A lot is only
re-fetched when it is not yet `detailed` AND `history_captured`.
"""
from __future__ import annotations

import argparse, asyncio, html, json, logging, os, random, re, signal, sys, threading, time
import urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# Reuse the colour logging + helpers from the sibling AuctionHistory scraper.
# NOTE: this exec also defines API_BASE / DEFAULT_UA etc; we override ours below.
exec(open(os.path.join(os.path.dirname(__file__), "..", "AuctionHistory", "scrape_auctionhistory.py")).read().split("class HttpError")[0])

ARCHIVE_BASE = "https://finalbid.vin/en/archive"
PER_PAGE = 24
DEFAULT_UA = "finalbid-research-scraper/1.0 (data research; contact: you@example.com)"

# main() re-binds this; this module-level default keeps parse helpers usable
# (e.g. from tests / one-off scripts) without a running main().
LOG = logging.getLogger("scraper")


class HttpError(Exception):
    def __init__(self, status, msg):
        super().__init__(msg); self.status = status


class ApiClient:
    def __init__(self, ua=DEFAULT_UA, timeout=45.0, retries=5, backoff=1.5, spacing=0.5, concurrency=8):
        self.ua, self.timeout, self.retries, self.backoff, self.spacing = ua, timeout, retries, backoff, spacing
        self.concurrency = concurrency
        self._lock = threading.Lock(); self._last = 0.0
    def _throttle(self):
        with self._lock:
            now = time.time(); wait = self._last + self.spacing - now
            if wait > 0: time.sleep(wait)
            self._last = time.time()
    def get_text(self, url):
        for a in range(1, self.retries + 1):
            self._throttle()
            try:
                req = urllib.request.Request(url, headers={"User-Agent": self.ua, "Accept": "text/html"})
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    if r.getcode() == 200:
                        return r.read().decode("utf-8", "ignore")
            except Exception:
                pass
            time.sleep(min(self.backoff * (2 ** (a - 1)), 30))
        raise HttpError(None, f"GET {url} failed")

    def fetch_many(self, urls: List[str]) -> Dict[str, tuple]:
        """Thread-based fallback with the same shape as AsyncFetcher.fetch_many."""
        urls = [u for u in urls if u]
        if not urls:
            return {}
        def one(u):
            t0 = time.time()
            try:
                return u, (self.get_text(u), time.time() - t0, None)
            except Exception as e:
                return u, (None, time.time() - t0, str(e))
        out = {}
        with ThreadPoolExecutor(max_workers=max(1, self.concurrency)) as ex:
            for u, r in ex.map(one, urls):
                out[u] = r
        return out

    def close(self):
        pass


class AsyncFetcher:
    """Concurrent HTTP on one keep-alive connection pool.

    httpx.AsyncClient runs on a private event loop in a background thread; the
    rest of the scraper talks to it through a synchronous bridge (`get_text`,
    `fetch_many`). A shared connection pool is what makes this ~15-20x faster
    than urllib/thread-based fetching, where every request pays a fresh TCP+TLS
    setup and the server appears to serialise them.
    """

    def __init__(self, timeout=45.0, retries=5, backoff=1.5, spacing=0.05,
                 concurrency=12, ua=DEFAULT_UA):
        import httpx
        self._httpx = httpx
        self.timeout = timeout
        self.retries = max(1, retries)
        self.backoff = backoff
        self.spacing = spacing
        self.concurrency = max(1, concurrency)
        self.ua = ua
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._err = None
        self._th = threading.Thread(target=self._run, name="async-fetch", daemon=True)
        self._th.start()
        self._ready.wait()
        if self._err:
            raise self._err
        LOG.info(f"http: httpx async, concurrency={self.concurrency}, "
                 f"spacing={spacing:g}s, keep-alive pool")

    def _run(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._setup())
        except Exception as e:          # never let __init__ hang on this
            self._err = e
            self._ready.set()
            return
        self._ready.set()
        self._loop.run_forever()

    async def _setup(self):
        # room for the detail batch plus the listing prefetch threads
        pool = self.concurrency + 16
        limits = self._httpx.Limits(max_connections=pool,
                                    max_keepalive_connections=pool)
        self._client = self._httpx.AsyncClient(
            headers={"User-Agent": self.ua, "Accept": "text/html",
                     "Accept-Language": "en"},
            timeout=self.timeout, limits=limits, follow_redirects=True)
        self._last = 0.0
        self._gate = asyncio.Lock()

    async def _throttle(self):
        if self.spacing <= 0:
            return
        async with self._gate:
            now = self._loop.time()
            wait = self._last + self.spacing - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = self._loop.time()

    async def _one(self, url):
        t0 = self._loop.time()
        for a in range(1, self.retries + 1):
            await self._throttle()
            try:
                r = await self._client.get(url)
                if r.status_code == 200:
                    return r.text, self._loop.time() - t0, None
                if r.status_code in (404, 410):
                    return None, self._loop.time() - t0, f"http {r.status_code}"
            except Exception as e:
                last = e
            else:
                last = f"http {r.status_code}"
            await asyncio.sleep(min(self.backoff * (2 ** (a - 1)), 30))
        return None, self._loop.time() - t0, str(last)

    async def _many(self, urls):
        sem = asyncio.Semaphore(self.concurrency)

        async def guarded(u):
            async with sem:
                return u, await self._one(u)

        return await asyncio.gather(*(guarded(u) for u in urls))

    def fetch_many(self, urls: List[str]) -> Dict[str, tuple]:
        """Fetch `urls` concurrently -> {url: (text|None, seconds, error|None)}."""
        urls = [u for u in urls if u]
        if not urls:
            return {}
        res = asyncio.run_coroutine_threadsafe(self._many(urls), self._loop).result()
        return {u: (t, s, e) for u, (t, s, e) in res}

    def get_text(self, url: str) -> str:
        """Single blocking GET; raises HttpError on failure (listing helper)."""
        text, _secs, err = self.fetch_many([url]).get(url, (None, 0.0, "no result"))
        if text is None:
            raise HttpError(None, f"GET {url} failed: {err}")
        return text

    def close(self):
        try:
            asyncio.run_coroutine_threadsafe(self._client.aclose(),
                                             self._loop).result(timeout=5)
        except Exception:
            pass
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Postgres sink: lots + scrape_pages checkpoints + lot_sales
# ---------------------------------------------------------------------------
class Db:
    LOTS_COLS = [
        "id", "source", "lot_number", "vin", "year", "make", "model", "trim",
        "title_status", "primary_damage", "secondary_damage", "odometer",
        "odometer_actual", "price_usd", "sale_date", "location", "flagged",
        "photos", "created_at", "public_token", "engine", "fuel", "sold_status",
        "insight", "retail_usd", "suspect_price", "raw",
        "scraped", "detailed", "history_captured",
        # --- full lot-page fields (init/04-detail-fields.sql) --------------
        "condition", "avg_final_bid", "est_repair", "body_color",
        "interior_color", "transmission", "drive", "vehicle_type", "loss_type",
        "keys_present", "airbags", "salvage_number", "seller", "cylinders",
        "engine_displacement_l", "engine_power_hp", "auctions_on_record",
        "first_seen", "detail_specs",
    ]

    # Every other column is written as COALESCE(new, existing) so a listing-only
    # re-walk can never blank a field the detail page filled — see _assign_sql().

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._psycopg = None
        self._local = threading.local()
        try:
            import psycopg  # type: ignore
            from psycopg.rows import dict_row  # noqa: F401
            self._psycopg = psycopg
            with psycopg.connect(dsn, connect_timeout=10) as probe:
                with probe.cursor() as c:
                    c.execute("SELECT 1"); c.fetchone()
            LOG.info(f"postgres connected ({dsn.split('@')[-1]})")
        except Exception as e:
            LOG.error(f"cannot connect to Postgres: {e}; DB features disabled")
            self._psycopg = None

    @property
    def enabled(self) -> bool:
        return self._psycopg is not None

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None or conn.closed:
            conn = self._psycopg.connect(self.dsn, autocommit=True)
            self._local.conn = conn
        return conn

    # -- progress -----------------------------------------------------------
    def mark_page(self, filter_key, filters, page, per_page, status,
                  total=None, found=0, written=0, error=None):
        if not self.enabled:
            return
        try:
            with self._conn().cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scrape_pages
                        (filter_key, filters, page, per_page, status,
                         total, lots_found, lots_written, attempts, last_error,
                         started_at, finished_at)
                    VALUES (%s,%s::jsonb,%s,%s,%s,%s,%s,%s,1,%s,now(),now())
                    ON CONFLICT (filter_key, page) DO UPDATE SET
                        status       = EXCLUDED.status,
                        total        = COALESCE(EXCLUDED.total, scrape_pages.total),
                        lots_found   = EXCLUDED.lots_found,
                        lots_written = EXCLUDED.lots_written,
                        attempts     = scrape_pages.attempts + 1,
                        last_error   = EXCLUDED.last_error,
                        finished_at  = now()
                    """,
                    (filter_key, json.dumps(filters), page, per_page, status,
                     total, found, written, (error or "")[:500] or None),
                )
        except Exception as e:
            LOG.error(f"DB mark_page failed (page {page}): {e}")

    def known_ids(self, ids: List[str]) -> set:
        """Subset of `ids` that already exist in `lots` (id is the stable key)."""
        if not self.enabled or not ids:
            return set()
        try:
            with self._conn().cursor() as cur:
                cur.execute("SELECT id FROM lots WHERE id = ANY(%s)", (list(ids),))
                return {r[0] for r in cur.fetchall()}
        except Exception as e:
            LOG.error(f"DB known_ids failed: {e}")
            return set()

    def detail_todo_ids(self, ids: List[str]) -> set:
        """Subset of `ids` that still need a detail fetch.

        Needs it when the row is missing, or not (detailed AND history_captured).
        So a lot already scraped but never history-captured gets one extra fetch.
        """
        if not self.enabled or not ids:
            return set(ids)
        try:
            with self._conn().cursor() as cur:
                cur.execute(
                    "SELECT id FROM lots "
                    "WHERE id = ANY(%s) AND detailed AND history_captured",
                    (list(ids),),
                )
                done = {r[0] for r in cur.fetchall()}
            return set(ids) - done
        except Exception as e:
            LOG.error(f"DB detail_todo_ids failed: {e}")
            return set(ids)

    def detail_backlog(self, after_id: str, limit: int) -> list:
        """Rows still needing a detail fetch, in id order (keyset paging).

        Returns [(id, url, raw_json_text)]. `after_id` advances past rows we
        already processed, so a batch that keeps failing can't stall the run.
        """
        if not self.enabled:
            return []
        try:
            with self._conn().cursor() as cur:
                cur.execute(
                    "SELECT id, url, raw::text FROM lots "
                    "WHERE url IS NOT NULL "
                    "  AND NOT (detailed AND history_captured) "
                    "  AND id > %s "
                    "ORDER BY id LIMIT %s",
                    (after_id or "", limit),
                )
                return cur.fetchall()
        except Exception as e:
            LOG.error(f"DB detail_backlog failed: {e}")
            return []

    def mark_detail_results(self, ok_ids, failed):
        """Record detail outcomes: clear errors on success, count failures.

        `ok_ids` resets detail_attempts and clears detail_error; each `(id, err)`
        in `failed` bumps detail_attempts and stores the last error. The rows
        themselves keep detailed/history_captured false via the sticky upsert, so
        `detail_todo_ids` returns them again on the next run.
        """
        if not self.enabled or (not ok_ids and not failed):
            return
        try:
            with self._conn().cursor() as cur:
                if ok_ids:
                    cur.execute(
                        "UPDATE lots SET detail_attempts = 0, detail_error = NULL "
                        "WHERE id = ANY(%s)", (list(ok_ids),))
                for lot_id, err in failed:
                    cur.execute(
                        "UPDATE lots SET detail_attempts = detail_attempts + 1, "
                        "detail_error = %s WHERE id = %s", (err, lot_id))
        except Exception as e:
            LOG.error(f"DB mark_detail_results failed: {e}")

    def undetailed_count(self) -> int:
        """Lots still missing a completed detail pass (i.e. queued for refetch)."""
        if not self.enabled:
            return 0
        try:
            with self._conn().cursor() as cur:
                cur.execute("SELECT count(*) FROM lots "
                            "WHERE NOT (detailed AND history_captured)")
                return cur.fetchone()[0]
        except Exception as e:
            LOG.error(f"DB undetailed_count failed: {e}")
            return 0

    def first_incomplete_page(self, filter_key):
        """(max_page, first_incomplete) for a filter set. Both None if unknown."""
        if not self.enabled:
            return (None, None)
        try:
            with self._conn().cursor() as cur:
                cur.execute(
                    """
                    WITH m AS (
                        SELECT max(page) AS mp FROM scrape_pages WHERE filter_key = %s
                    )
                    SELECT m.mp,
                           (SELECT min(g.p)
                              FROM generate_series(1, COALESCE(m.mp, 0)) AS g(p)
                             WHERE NOT EXISTS (
                                   SELECT 1 FROM scrape_pages sp
                                    WHERE sp.filter_key = %s
                                      AND sp.page = g.p
                                      AND sp.status IN ('done','empty')))
                    FROM m
                    """,
                    (filter_key, filter_key),
                )
                row = cur.fetchone()
                if not row:
                    return (None, None)
                return (row[0], row[1])
        except Exception as e:
            LOG.error(f"DB first_incomplete_page failed: {e}")
            return (None, None)

    def archive_end_reached(self, filter_key) -> bool:
        """True once a walk has seen an EMPTY page (walked past the archive end)."""
        if not self.enabled:
            return False
        try:
            with self._conn().cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM scrape_pages "
                    "WHERE filter_key = %s AND status = 'empty' LIMIT 1",
                    (filter_key,),
                )
                return cur.fetchone() is not None
        except Exception as e:
            LOG.error(f"DB archive_end_reached failed: {e}")
            return False

    # -- data ---------------------------------------------------------------
    @staticmethod
    def _assign_sql(c: str) -> str:
        """SQL for one column's ON CONFLICT DO UPDATE assignment.

        Every data column is written as COALESCE(new, existing): a plain listing
        re-fetch may parse a field as NULL (or send photos=[]) and must never
        blank a value the detail page already filled. A non-NULL value still
        overwrites normally, so genuine updates land. Only the two "have we
        already done this" flags are monotonic.
        """
        if c == "detailed":
            return "detailed = lots.detailed OR EXCLUDED.detailed"
        if c == "history_captured":
            return ("history_captured = lots.history_captured"
                    " OR EXCLUDED.history_captured")
        if c == "photos":
            # a listing-only re-walk sends photos=[] — keep what we have
            return ("photos = COALESCE(NULLIF(EXCLUDED.photos, '[]'::jsonb),"
                    " lots.photos)")
        return f"{c} = COALESCE(EXCLUDED.{c}, lots.{c})"

    # Postgres allows 65535 bind parameters per statement. With one bind per
    # column that caps a single INSERT at ~1200 rows, so batches are chunked.
    INSERT_CHUNK = 500

    def load_lots(self, lots: List[Dict[str, Any]]) -> int:
        """Upsert lots in chunks; `detailed`/`history_captured` never downgrade."""
        if not self.enabled or not lots:
            return 0
        total = 0
        for i in range(0, len(lots), self.INSERT_CHUNK):
            total += self._load_lots_chunk(lots[i:i + self.INSERT_CHUNK])
        return total

    def _load_lots_chunk(self, lots: List[Dict[str, Any]]) -> int:
        rows = [self._to_row(l) for l in lots]
        try:
            cols = ", ".join(self.LOTS_COLS + ["url"])
            # The two "have we already done this" flags are monotonic: a plain
            # listing re-fetch emits them false and must never clear them. Every
            # data column is COALESCE(new, existing) so a listing re-walk cannot
            # blank a field the detail page filled — see _assign_sql().
            assigns = ",\n                    ".join(
                self._assign_sql(c) for c in self.LOTS_COLS if c != "id"
            )
            per_row = "(" + ", ".join(
                f"{'%s::jsonb' if c in ('photos', 'insight', 'raw', 'detail_specs') else '%s'}"
                for c in (self.LOTS_COLS + ["url"])
            ) + ")"
            values_sql = ", ".join([per_row] * len(rows))
            sql = f"""
                INSERT INTO lots ({cols})
                VALUES {values_sql}
                ON CONFLICT (id) DO UPDATE SET
                    {assigns},
                    url = EXCLUDED.url,
                    ingested_at = now()
            """
            flat = [v for row in rows for v in row]
            with self._conn().cursor() as cur:
                cur.execute(sql, flat)
            return len(rows)
        except Exception as e:
            LOG.error(f"DB load_lots failed ({len(rows)} rows): {e}")
            return 0

    def load_sales(self, sales: List[Dict[str, Any]]) -> int:
        """Upsert parsed per-lot sale-history rows into `lot_sales`."""
        if not self.enabled or not sales:
            return 0
        sql = """
            INSERT INTO lot_sales
                (source, lot_number, sale_date, vin, final_bid, status,
                 auctions_on_record, first_seen, raw)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            ON CONFLICT (source, lot_number, sale_date) DO UPDATE SET
                vin                = EXCLUDED.vin,
                final_bid          = EXCLUDED.final_bid,
                status             = EXCLUDED.status,
                auctions_on_record = EXCLUDED.auctions_on_record,
                first_seen         = EXCLUDED.first_seen,
                raw                = EXCLUDED.raw,
                ingested_at        = now()
        """
        try:
            with self._conn().cursor() as cur:
                for s in sales:
                    cur.execute(sql, (
                        s.get("source"), s.get("lot_number"), s.get("sale_date"),
                        s.get("vin"), s.get("final_bid"), s.get("status"),
                        s.get("auctions_on_record"), s.get("first_seen"),
                        json.dumps(s),
                    ))
            return len(sales)
        except Exception as e:
            LOG.error(f"DB load_sales failed ({len(sales)} rows): {e}")
            return 0

    @staticmethod
    def _to_row(l: Dict[str, Any]) -> tuple:
        insight = l.get("insight")
        photos = l.get("photos") or []
        odo_actual = l.get("odometer_actual")
        specs = l.get("detail_specs")
        return (
            l.get("id"), l.get("source"), l.get("lot_number"), l.get("vin"),
            l.get("year"), l.get("make"), l.get("model"), l.get("trim"),
            l.get("title_status"), l.get("primary_damage"),
            l.get("secondary_damage"), l.get("odometer"),
            None if odo_actual is None else bool(odo_actual), l.get("price_usd"),
            l.get("sale_date"), l.get("location"), bool(l.get("flagged")),
            json.dumps(photos), l.get("created_at"),
            l.get("public_token"), l.get("engine"), l.get("fuel"),
            l.get("sold_status"), json.dumps(insight) if insight else None,
            (insight or {}).get("retail_usd"), bool(l.get("suspect_price")),
            json.dumps(l),
            True,                                  # scraped: we are persisting it now
            bool(l.get("detailed")),
            bool(l.get("history_captured")),
            # --- full lot-page fields (order must match LOTS_COLS) ---------
            l.get("condition"), l.get("avg_final_bid"), l.get("est_repair"),
            l.get("body_color"), l.get("interior_color"), l.get("transmission"),
            l.get("drive"), l.get("vehicle_type"), l.get("loss_type"),
            l.get("keys_present"), l.get("airbags"), l.get("salvage_number"),
            l.get("seller"), l.get("cylinders"), l.get("engine_displacement_l"),
            l.get("engine_power_hp"), l.get("auctions_on_record"),
            l.get("first_seen"), json.dumps(specs) if specs else None,
            l.get("url"),
        )

    def refresh_bid_history(self, vins: Optional[List[str]] = None) -> Optional[int]:
        """Rebuild bid_history for `vins` (or every VIN when vins is None)."""
        if not self.enabled:
            return None
        try:
            with self._conn().cursor() as cur:
                if vins:
                    cur.execute("SELECT refresh_bid_history(%s::text[])", (list(vins),))
                else:
                    cur.execute("SELECT refresh_bid_history()")
                n = cur.fetchone()[0]
            return n
        except Exception as e:
            LOG.error(f"refresh_bid_history failed: {e}")
            return None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
CARD_RE = re.compile(r'<article.*?</article>', re.S)
# finalbid lot keys look like `copart-68151656-11603312069222`. The trailing VIN is
# NOT always 17 chars (older / re-tagged vehicles can be shorter), so do not
# hard-code 17 — otherwise the whole match fails and the lot gets a blank
# source/id and is rejected by lots_source_chk.
LOT_KEY_RE = re.compile(r'(copart|iaai)-(\d+)(?:-([A-Za-z0-9]+))?', re.I)


def parse_lot_key(card, url=None):
    """(source, lot_number, vin) from a card (its href contains the key).

    Falls back to the detail URL, then gives up with ("", "", ""). VIN may be
    shorter than 17 chars or empty.
    """
    m = LOT_KEY_RE.search(card) or (LOT_KEY_RE.search(url) if url else None)
    if not m:
        return "", "", ""
    return m.group(1).lower(), m.group(2), (m.group(3) or "").upper()


# ---------------------------------------------------------------------------
# Lot-page (detail) field parsing
# ---------------------------------------------------------------------------
# The lot page renders a <dt>label</dt><dd>value</dd> spec list. Everything the
# page shows is parsed from it: sale document/status, odometer (+ "not actual"),
# engine, transmission, drive, colours, keys, airbags, salvage #, seller,
# condition, actual cash value / avg final bid / est. repair, loss type, ...
TAG_RE = re.compile(r"<[^>]+>")
SPEC_ROW_RE = re.compile(r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", re.S)
LDJSON_RE = re.compile(r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', re.S)


def _clean(s: str) -> str:
    """Strip tags, unescape entities (&amp; etc.), collapse whitespace."""
    return re.sub(r"\s+", " ", TAG_RE.sub(" ", html.unescape(s or ""))).strip()


def _slug_words(s: str) -> str:
    return "-".join(w for w in re.split(r"[^a-z0-9]+", (s or "").lower()) if w)


def url_model_slug(url):
    """The <model> slug from /en/<make>/<model>[/<year>/<lot>]."""
    m = re.search(r"/en/[^/]+/([^/]+)/", url or "")
    return m.group(1) if m else ""


def extract_trim(model, url):
    """Trim = the words of `model` left after its URL-slug prefix.

    The listing title flattens "year make model trim" into `model`, so
    "GRAND CHEROKEE LAREDO" with slug "grand-cherokee" -> "LAREDO".
    """
    if not model:
        return None
    slug = url_model_slug(url)
    if not slug:
        return None
    words = model.split()
    for n in range(1, len(words)):
        if _slug_words(" ".join(words[:n])) == slug:
            return " ".join(words[n:]) or None
    return None


def trim_from_ldjson(jd):
    """ld+json `name` = "2018 Ford Focus SE" -> trim ("SE")."""
    if not isinstance(jd, dict):
        return None
    name = (jd.get("name") or "").strip()
    if not name:
        return None
    brand = jd.get("brand")
    if isinstance(brand, dict):
        brand = brand.get("name")
    for part in (jd.get("modelDate"), brand, jd.get("model")):
        if part:
            name = re.sub(r"^\s*" + re.escape(str(part)) + r"\b", "", name).strip()
    return name or None


def _int_from(s):
    if s is None:
        return None
    m = re.search(r"\d[\d,]*", str(s))
    return int(m.group(0).replace(",", "")) if m else None


def parse_money(s):
    """'$1,500' -> 1500 ; '' / None -> None."""
    if not s:
        return None
    m = re.search(r"\$\s*([\d,]+)", s) or re.search(r"([\d,]+)", s)
    return int(m.group(1).replace(",", "")) if m else None


def parse_odometer(s):
    """'92,047 mi' -> (92047, True); '0 mi · not actual' -> (0, False)."""
    if not s:
        return (None, None)
    miles = _int_from(s)
    if miles is None:
        return (None, None)
    return (miles, not re.search(r"not\s*actual", s, re.I))


def parse_bool_yn(s):
    if s is None:
        return None
    t = s.strip().lower()
    if t in ("yes", "y", "true"):
        return True
    if t in ("no", "n", "false"):
        return False
    return None


def norm_title_status(v):
    """'Salvage title' -> 'salvage' (matches the listing-card vocabulary)."""
    if not v:
        return None
    s = re.sub(r"\s+", " ", v).strip().lower()
    for k in ("clean", "salvage", "rebuilt"):
        if s.startswith(k):
            return k
    return s.replace(" title", "").strip() or None


def parse_cylinders(engine_text):
    m = re.search(r"(\d+)\s*cyl", engine_text or "", re.I)
    return int(m.group(1)) if m else None


def parse_specs(text):
    """The lot page's <dt>/<dd> spec list -> {label.lower(): value}."""
    specs = {}
    for dt, dd in SPEC_ROW_RE.findall(text or ""):
        label = _clean(dt)
        if not label or len(label) > 40:
            continue                      # skips the "Auctions on record" header
        val = _clean(dd)
        if not val or len(val) > 160:     # skips its giant value blob
            continue
        specs.setdefault(label.lower(), val)
    return specs


def parse_archive(html_text):
    lots = []
    for card in CARD_RE.findall(html_text):
        murl = re.search(r'href="(https://finalbid\.vin/en/([^"]+))"', card)
        url = murl.group(1) if murl else None
        mtitle = re.search(r'<h3[^>]*>\s*<a[^>]*>(.*?)</a>', card, re.S)
        title = html.unescape(re.sub(r'<.*?>', '', mtitle.group(1))).strip() if mtitle else ""
        parts = title.split()
        year = int(parts[0]) if parts and parts[0].isdigit() else None
        make = parts[1].upper() if len(parts) > 1 else ""
        model = " ".join(parts[2:]).upper() if len(parts) > 2 else ""
        source, lot_number, vin = parse_lot_key(card, url)
        if not source or not lot_number:
            LOG.warning(f"skipping unparseable archive card (url={url!r})")
            continue
        price_m = re.search(r'\$([\d,]+)', card)
        price = int(price_m.group(1).replace(',', '')) if price_m else None
        loc_m = re.search(r'([\w /&]+?)\s*·\s*([\d,]+)\s*mi\s*·\s*([^<]+)', card)
        dmg = loc_m.group(1).strip() if loc_m else ""
        odo = int(loc_m.group(2).replace(',', '')) if loc_m else None
        location = loc_m.group(3).strip() if loc_m else ""
        ts_m = re.search(r'(Clean|Salvage|Rebuilt)\s+title', card, re.I)
        title_status = ts_m.group(1).strip().lower() if ts_m else None
        dm = re.search(r'([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})', card)
        sale_date = None
        if dm:
            try:
                sale_date = datetime.strptime(dm.group(1), "%b %d, %Y").strftime("%Y-%m-%d")
            except Exception:
                sale_date = dm.group(1)
        ss_m = re.search(r'\b(Sold by Buy now|Sold by Bid|Sold|Unsold|Not Sold|Ended)\b', card)
        sold_status = ss_m.group(1) if ss_m else None
        rck_m = re.search(r'>([^<>]*(?:title|Keys|Run ?& ? Drive|Keys ?& ? Run)[^<>]*)<', card)
        lots.append({
            "id": f"{source}-{lot_number}",
            "source": source,
            "lot_number": lot_number,
            "vin": vin,
            "year": year,
            "make": make,
            "model": model,
            "trim": extract_trim(model, url),
            "title_status": title_status,
            "primary_damage": dmg,
            "secondary_damage": None,
            "odometer": odo,
            "odometer_actual": None,
            "price_usd": price,
            "sale_date": sale_date,
            "location": location,
            "flagged": False,
            "photos": [],
            "created_at": None,
            "public_token": None,
            "engine": None,
            "fuel": None,
            "sold_status": sold_status,
            "insight": None,
            "retail_usd": None,
            "suspect_price": False,
            "detailed": False,
            "history_captured": False,
            "raw": {"listing": card, "sold_status": sold_status,
                    "footer": (rck_m.group(1).strip() if rck_m else None),
                    "detail_url": url},
            "url": url,
        })
    return lots


# "Sale history of this VIN": Date | Platform | Lot # | Final bid | Status
SALE_ROW_RE = re.compile(r'<tr class="border-b border-line">(.*?)</tr>', re.S)
SALE_CELL_RE = re.compile(r'<td[^>]*>(.*?)</td>', re.S)


def parse_sale_history(text, vin):
    """Parse the detail page's 'Sale history of this VIN' table."""
    i = text.find("Sale history of this VIN")
    if i < 0:
        return []
    seg = text[i:i + 12000]
    end = seg.find("</table>")
    if end >= 0:
        seg = seg[:end]

    aor = re.search(r'text-ink">(\d+)</dd>\s*<dt[^>]*>\s*Auctions on record', seg)
    fs = re.search(r'text-ink">(\d{4})</dd>\s*<dt[^>]*>\s*First seen', seg)
    out = []
    for row in SALE_ROW_RE.findall(seg):
        cells = [re.sub(r'\s+', ' ', html.unescape(re.sub(r'<.*?>', '', c))).strip()
                 for c in SALE_CELL_RE.findall(row)]
        if len(cells) < 5:
            continue
        date_s, plat, lotno, bid_s, status = cells[0], cells[1], cells[2], cells[3], cells[4]
        try:
            d = datetime.strptime(date_s, "%b %d, %Y").strftime("%Y-%m-%d")
        except Exception:
            continue
        bm = re.search(r'([\d,]+)', bid_s)
        out.append({
            "source": (plat or "").strip().lower(),
            "lot_number": lotno.strip(),
            "sale_date": d,
            "vin": vin,
            "final_bid": int(bm.group(1).replace(",", "")) if bm else None,
            "status": status or None,
            "auctions_on_record": int(aor.group(1)) if aor else None,
            "first_seen": int(fs.group(1)) if fs else None,
        })
    return out


def apply_detail(lot, text):
    """Parse an already-fetched detail page into `lot` (all page fields + sales).

    Does no I/O — the batch fetch happens in detail_page(), so 24 lots can be
    in flight at once. A None text (failed fetch) leaves the flags untouched so
    the lot stays on the todo list for a later run.
    """
    if text is None or not text:
        return lot
    try:
        lot.setdefault("raw", {})
        md_m = re.search(r'<meta name="description" content="(.*?)"', text, re.S)
        if md_m:
            lot["raw"]["meta"] = html.unescape(md_m.group(1))

        # --- ld+json: photos + the structured vehicle block ----------------
        jd = None
        m = LDJSON_RE.search(text)
        if m:
            try:
                jd = json.loads(m.group(1))
            except Exception:
                jd = None
        if isinstance(jd, dict):
            lot["photos"] = jd.get("image", []) or []
            lot["raw"]["ldjson"] = jd
        else:
            jd = {}

        # --- the page's <dt>/<dd> spec list (every field the page shows) ---
        specs = parse_specs(text)
        if specs:
            lot["detail_specs"] = specs

        def sp(label):
            return specs.get(label)

        # identity / trim: prefer ld+json name, fall back to the listing URL slug
        if not lot.get("trim"):
            lot["trim"] = (trim_from_ldjson(jd)
                           or extract_trim(lot.get("model") or "", lot.get("url")))

        title = norm_title_status(sp("sale document"))
        if title:
            lot["title_status"] = title
        for label, col in (
            ("primary damage", "primary_damage"),
            ("secondary damage", "secondary_damage"),
            ("location", "location"),
            ("sale status", "sold_status"),
            ("fuel", "fuel"),
            ("engine", "engine"),
            ("condition", "condition"),
            ("transmission", "transmission"),
            ("drive", "drive"),
            ("body color", "body_color"),
            ("loss type", "loss_type"),
            ("salvage #", "salvage_number"),
            ("seller", "seller"),
        ):
            if sp(label):
                lot[col] = sp(label)
        lot["interior_color"] = (sp("interior color")
                                 or jd.get("vehicleInteriorColor"))
        lot["vehicle_type"] = sp("vehicle type") or jd.get("bodyType")
        lot["keys_present"] = parse_bool_yn(sp("keys"))
        lot["airbags"] = parse_bool_yn(sp("airbags"))

        # odometer + the "not actual" marker ("0 mi · not actual")
        miles, actual = parse_odometer(sp("odometer"))
        if miles is not None:
            lot["odometer"] = miles
            lot["odometer_actual"] = actual
        elif lot.get("odometer") is None:
            val = (jd.get("mileageFromOdometer") or {}).get("value")
            if val is not None:
                lot["odometer"] = int(val)
                lot["odometer_actual"] = True

        lot["cylinders"] = parse_cylinders(lot.get("engine"))
        eng = jd.get("vehicleEngine") or {}
        disp = (eng.get("engineDisplacement") or {}).get("value")
        pwr = (eng.get("enginePower") or {}).get("value")
        if disp is not None:
            lot["engine_displacement_l"] = disp
        if pwr is not None:
            lot["engine_power_hp"] = pwr

        # money block -> columns (+ keep it in `insight`)
        insight = lot.get("insight") or {}
        for label, col in (("actual cash value", "retail_usd"),
                           ("avg final bid", "avg_final_bid"),
                           ("est. repair", "est_repair")):
            amount = parse_money(sp(label))
            if amount is not None:
                lot[col] = amount
                insight[col] = amount
        if insight:
            lot["insight"] = insight

        if not lot.get("price_usd"):
            sp_m = re.search(r'(?:Sold|final bid)[^$]*\$([\d,]+)', text, re.I)
            if sp_m:
                lot["price_usd"] = int(sp_m.group(1).replace(',', ''))

        # per-lot sale history (re-listings of this lot) + page summary
        lot["sales"] = parse_sale_history(text, lot.get("vin"))
        lot["raw"]["sales"] = lot["sales"]
        if lot["sales"]:
            lot["auctions_on_record"] = lot["sales"][0].get("auctions_on_record")
            lot["first_seen"] = lot["sales"][0].get("first_seen")
        lot["detailed"] = True
        lot["history_captured"] = True
    except Exception:
        pass
    return lot


def fetch_detail(client, lot):
    """Single-lot detail fetch (kept for callers/tests outside detail_page)."""
    if not lot.get("url"):
        return lot
    try:
        return apply_detail(lot, client.get_text(lot["url"]))
    except Exception:
        return lot


# ---------------------------------------------------------------------------
# Crawl plan
# ---------------------------------------------------------------------------
STOP = threading.Event()
_SIGINT = threading.Event()


def _sigint(_s, _f):
    if not _SIGINT.is_set():
        _SIGINT.set(); STOP.set()


signal.signal(signal.SIGINT, _sigint)


def plan_crawl(db, full, fresh, overlap, track, load_db):
    """Return ([(start_page, frontier_stop), ...], reason)."""
    if full:
        return [(1, False)], "full re-crawl from page 1 to the end"
    if fresh:
        return [(1, True)], "fresh scan from page 1, stop at first repeated page"
    if not (db and db.enabled and track and load_db):
        return [(1, False)], "full walk (no checkpoint)"
    mp, fi = db.first_incomplete_page("finalbid")
    if mp is None:
        return [(1, False)], "full walk (no checkpoint recorded yet)"
    if fi is not None:
        rs = max(1, fi - overlap)
        if rs <= 1:
            return [(1, False)], f"full walk (gap at page {fi}, overlap covers the top)"
        return ([(1, True), (rs, False)],
                f"top-up + resume from page {rs} (gap at {fi}, overlap {overlap})")
    # everything recorded up to mp is done
    if db.archive_end_reached("finalbid"):
        return [(1, True)], f"top-up only (complete through page {mp}, archive end reached)"
    rs = max(1, mp - overlap)
    if rs <= 1:
        return [(1, False)], f"full walk (deepest page {mp}, archive end not reached)"
    return ([(1, True), (rs, False)],
            f"top-up + resume from page {rs} (last page {mp}, overlap {overlap})")


# ---------------------------------------------------------------------------
def main():
    global LOG
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--verbose", action="store_true")
    ap.add_argument("--log-file", default="scrape.log")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--out", default="")
    ap.add_argument("--per-page", type=int, default=PER_PAGE)
    ap.add_argument("--workers", type=int, default=4,
                    help="listing prefetch depth (pages in flight)")
    ap.add_argument("--concurrency", type=int, default=12,
                    help="parallel detail requests (shared keep-alive pool)")
    ap.add_argument("--spacing", type=float, default=0.05)
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--retries", type=int, default=5)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--count", action="store_true")
    ap.add_argument("--resume", action="store_true",
                    help="default: top-up newest pages, then resume the deep crawl")
    ap.add_argument("--fresh", "--new-data", dest="fresh", action="store_true",
                    help="scan from page 1; stop at the first repeated page")
    ap.add_argument("--full", action="store_true",
                    help="walk page 1 to the end, ignoring the checkpoint")
    ap.add_argument("--overlap", type=int, default=15)
    ap.add_argument("--start-page", type=int, default=0,
                    help="start the archive walk at this page instead of page 1 / "
                         "the resume checkpoint (overrides --full/--fresh/--resume)")
    ap.add_argument("--stop-after", type=int, default=2)
    ap.add_argument("--max-error-streak", type=int, default=5,
                    help="abort after this many consecutive failed pages")
    ap.add_argument("--bid-history-minutes", type=float, default=5.0,
                    help="rebuild bid_history every N minutes during the run")
    ap.add_argument("--slow-seconds", type=float, default=3.0,
                    help="flag any single request slower than this")
    ap.add_argument("--rate-every", type=int, default=10,
                    help="print a throughput line every N pages")
    ap.add_argument("--detail-retry-passes", type=int, default=1,
                    help="extra concurrent passes for lots whose detail failed")
    ap.add_argument("--dsn", default=os.environ.get("DSN", "postgresql://auction:auction@localhost:5433/finalbid"))
    ap.add_argument("--track-pages", action="store_true")
    ap.add_argument("--load-db", action="store_true")
    ap.add_argument("--refresh-bid-history", action="store_true")
    ap.add_argument("--detail", action="store_true", help="fetch lot detail pages")
    ap.add_argument("--re-detail", action="store_true",
                    help="re-fetch lot pages for rows whose detail is missing, "
                         "straight from the DB (no archive walk). Use after a "
                         "parser change to backfill existing rows.")
    ap.add_argument("--re-detail-batch", type=int, default=250,
                    help="rows pulled from the DB per --re-detail batch")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%H:%M:%S")
    LOG = logging.getLogger("scraper")

    try:
        client = AsyncFetcher(timeout=args.timeout, retries=args.retries,
                              spacing=args.spacing, concurrency=args.concurrency)
    except Exception as e:
        LOG.error(f"httpx async unavailable ({e}); falling back to urllib threads "
                  f"(slower)")
        client = ApiClient(timeout=args.timeout, retries=args.retries,
                           spacing=args.spacing, concurrency=args.concurrency)
    db = Db(args.dsn) if (args.dsn and (args.track_pages or args.load_db)) else None

    filters = {}
    # --fresh keeps its own checkpoint key so its short scan never pollutes the
    # deep-crawl coverage record (and vice versa).
    fkey = "finalbid:fresh" if args.fresh else "finalbid"

    pages = 999999
    STOP.clear()
    stats = {"pages_ok": 0, "pages_empty": 0, "lots": 0, "db_rows": 0,
             "pages_failed": 0, "new_lots": 0, "detail_fetched": 0, "sales": 0,
             "bh_vins": 0, "fetch_time": 0.0, "detail_time": 0.0,
             "page_time": 0.0, "slows": 0, "pages_seen": 0,
             "detail_failed": 0}
    lock = threading.Lock()
    t0 = time.time()
    last_refresh = [t0]

    plan, why = plan_crawl(db, args.full, args.fresh, args.overlap,
                           args.track_pages, args.load_db)
    if args.start_page and args.start_page > 1:
        plan = [(args.start_page, False)]
        why = f"explicit --start-page {args.start_page} (walk to the end)"
    LOG.info(f"plan: {why}")

    def refresh_history(force=False):
        if not (db and db.enabled and args.refresh_bid_history):
            return
        if not force and (time.time() - last_refresh[0]) < args.bid_history_minutes * 60:
            return
        last_refresh[0] = time.time()
        n = db.refresh_bid_history()
        if n is not None:
            LOG.info(f"bid_history full rebuild ({n} VINs, every {args.bid_history_minutes:g}m)")

    def fetch(page):
        # Listing only — detail happens in walk() at the point the page is kept.
        t0 = time.time()
        try:
            if STOP.is_set():
                return (200, [], time.time() - t0)
            lots = parse_archive(client.get_text(f"{ARCHIVE_BASE}?page={page}"))
            return (200, lots, time.time() - t0)
        except HttpError as e:
            return (e.status, [], time.time() - t0)

    detail_report = []          # (lot_id, error|None) flushed after the page load

    def re_detail_pass():
        """Backfill detail fields for rows already in the DB, no archive walk.

        Walks the DB backlog in id order so the whole table is covered exactly
        once, regardless of how the live archive re-paginates. Each row's stored
        `raw` dict is reloaded so the re-parsed lot is a superset of what was
        there (nothing else is lost), then written back through the normal
        upsert.
        """
        done = 0
        failed_total = 0
        after = ""
        while not STOP.is_set():
            rows = db.detail_backlog(after, args.re_detail_batch)
            if not rows:
                break
            after = rows[-1][0]
            payload = []
            for rid, url, raw_text in rows:
                try:
                    lot = json.loads(raw_text) if raw_text else {}
                except Exception:
                    lot = {}
                if not isinstance(lot, dict):
                    lot = {}
                if not isinstance(lot.get("raw"), dict):
                    lot["raw"] = {}
                lot["id"] = rid
                lot["url"] = url
                payload.append(lot)

            fetched = client.fetch_many([l["url"] for l in payload])
            lots, failed, vins = [], [], set()
            for lot in payload:
                text, secs, err = fetched.get(
                    lot["url"], (None, 0.0, "no result"))
                if text is None:
                    failed.append((lot["id"], err))
                    continue
                apply_detail(lot, text)
                if not lot.get("detailed"):
                    failed.append((lot["id"], "detail parse failed"))
                    continue
                lots.append(lot)
                if lot.get("vin"):
                    vins.add(lot["vin"])

            if lots:
                db.load_lots(lots)
                sales = [s for l in lots for s in (l.get("sales") or [])]
                if sales:
                    stats["sales"] += db.load_sales(sales)
                db.mark_detail_results([l["id"] for l in lots], [])
                if vins:
                    db.refresh_bid_history(sorted(vins))
            if failed:
                db.mark_detail_results([], failed)
                failed_total += len(failed)

            done += len(lots)
            stats["lots"] += len(lots)
            stats["detail_fetched"] += len(lots)
            stats["new_lots"] += len(lots)
            print(f"  re-detail {done:,} lots done, {failed_total:,} failed "
                  f"(backlog cursor {after})", flush=True)
            if args.limit and done >= args.limit:
                print(f"  re-detail limit {args.limit} reached -> stop")
                break
            refresh_history()
        refresh_history(force=True)
        print(f"re-detail finished: {done:,} lots refreshed {failed_total:,} failed")

    if args.re_detail:
        if not (db and db.enabled):
            LOG.error("--re-detail needs the database (DSN unset or unreachable)")
            return 2
        LOG.info("re-detail: re-fetching lot pages straight from the DB "
                 "(no archive walk)")
        re_detail_pass()
        client.close()
        undetailed = db.undetailed_count()
        print(f"DONE ok=re-detail lots={stats['lots']:,} "
              f"detail={stats['detail_fetched']:,} sales={stats['sales']:,} "
              f"undetailed={undetailed:,}")
        print(f"TIME {time.time() - t0:.0f}s  lots={stats['lots']:,}")
        return 0

    def detail_page(page, lots):
        """Detail the lots not yet (detailed AND history-captured).

        All of a page's detail URLs are fetched at once through the shared
        keep-alive pool, then any failures get `--detail-retry-passes` extra
        passes. A lot that still fails keeps `detailed`/`history_captured`
        false, so the next run picks it up again; its error/attempt count is
        recorded in the DB for targeted refetch.
        """
        if not lots:
            return lots
        todo = lots
        if db and db.enabled and args.load_db:
            need = db.detail_todo_ids([l.get("id") for l in lots])
            todo = [l for l in lots if l.get("id") in need]
            if not todo:
                print(f"  detail skipped: page={page} lots={len(lots)} all done")
                return lots
        print(f"  detail phase start: page={page} todo={len(todo)}/{len(lots)} "
              f"concurrency={args.concurrency}")

        t0 = time.time()
        pending = [l for l in todo if l.get("url")]
        failed = {l.get("id"): "no url" for l in todo if not l.get("url")}
        passes = max(0, args.detail_retry_passes) + 1
        for attempt in range(1, passes + 1):
            fetched = client.fetch_many([l["url"] for l in pending])
            still = []
            for l in pending:
                text, secs, err = fetched.get(l["url"], (None, 0.0, "no result"))
                if text is None:
                    failed[l.get("id")] = err
                    still.append(l)
                    continue
                apply_detail(l, text)
                failed.pop(l.get("id"), None)
                with lock:
                    stats["detail_fetched"] += 1
                    stats["detail_time"] += secs
                    i = stats["detail_fetched"]
                    if secs >= args.slow_seconds:
                        stats["slows"] += 1
                tag = "SLOW" if secs >= args.slow_seconds else "    "
                print(f"  {tag}detail[{i}] page-lot={l['lot_number']} "
                      f"vin={l['vin']} {secs:.1f}s")
            if not still:
                break
            if attempt < passes:
                print(f"  detail retry pass {attempt}: {len(still)} lot(s)")
                pending = still
            else:
                for l in still:
                    with lock:
                        stats["detail_failed"] += 1
                    print(f"  FAIL detail page-lot={l['lot_number']} "
                          f"vin={l['vin']} err={failed.get(l.get('id'))}")
                pending = []

        dt = time.time() - t0
        ok = len(todo) - len(failed)
        print(f"  detail phase done: page={page} ok={ok}/{len(todo)} in "
              f"{dt:.1f}s ({dt / max(len(todo), 1):.2f}s/lot, "
              f"failed={len(failed)})")
        if db and db.enabled and args.load_db:
            detail_report.append(([l["id"] for l in todo
                                   if l.get("id") not in failed],
                                  list(failed.items())))
        return lots

    ex = ThreadPoolExecutor(max_workers=args.workers)

    def walk(start, frontier):
        inflight = {}
        current = start
        known_run = 0
        err_run = 0
        while not STOP.is_set():
            while len(inflight) < args.workers:
                inflight[current] = ex.submit(fetch, current)
                current += 1
            p = min(inflight)
            fut = inflight.pop(p)
            try:
                status, lots, t_fetch = fut.result()
            except Exception:
                status, lots, t_fetch = None, [], 0.0
            t_page = time.time()
            if status == 200 and lots and args.detail and not STOP.is_set():
                lots = detail_page(p, lots)
            t_page = time.time() - t_page
            with lock:
                stats["fetch_time"] += t_fetch
                stats["page_time"] += t_page
                stats["pages_seen"] += 1
                n = len(lots)
                new = 0
                if status == 200 and n == 0:
                    stats["pages_empty"] += 1; st = "empty"
                elif status == 200:
                    stats["pages_ok"] += 1; stats["lots"] += n; st = "done"
                    if db and db.enabled and args.load_db:
                        new = n - len(db.known_ids([l.get("id") for l in lots]))
                    else:
                        new = n
                    stats["new_lots"] += new
                else:
                    stats["pages_failed"] += 1; st = "failed"
                print(f"page {p:>5} http={status} lots={n} new={new} {st} "
                      f"list={t_fetch:.1f}s detail={t_page:.1f}s "
                      f"ok={stats['pages_ok']} empty={stats['pages_empty']} "
                      f"fail={stats['pages_failed']} got={stats['lots']:,}")
                if db and db.enabled:
                    written = 0
                    if args.load_db and lots:
                        written = db.load_lots(lots); stats["db_rows"] += written
                        # detail success/failure bookkeeping (rows now exist)
                        while detail_report:
                            ok_ids, bad = detail_report.pop(0)
                            db.mark_detail_results(ok_ids, bad)
                        sales = [s for l in lots for s in (l.get("sales") or [])]
                        if sales:
                            stats["sales"] += db.load_sales(sales)
                        # bid_history is generated right here for every VIN we
                        # just detailed, from the detail page's own sale data.
                        vins = sorted({l["vin"] for l in lots
                                       if l.get("detailed") and l.get("vin")})
                        if vins:
                            db.refresh_bid_history(vins)
                            stats["bh_vins"] += len(vins)
                            print(f"  bid_history: page={p} vins={len(vins)}")
                    if args.track_pages:
                        db.mark_page(fkey, filters, p, args.per_page, st,
                                     None, n, written or n, None)
                if args.out and lots:
                    with open(args.out, "a") as fh:
                        for l in lots:
                            fh.write(json.dumps(l) + "\n")
                if args.limit and stats["lots"] >= args.limit:
                    STOP.set()
            # rolling throughput line
            if args.rate_every and stats["pages_seen"] % args.rate_every == 0:
                el = max(time.time() - t0, 0.001)
                seen = stats["pages_seen"]
                det = stats["detail_fetched"]
                avg_det = (stats["detail_time"] / det) if det else 0.0
                print(f"  rate: {seen} pages / {el:.0f}s  "
                      f"lots/min={stats['lots'] * 60 / el:.1f}  "
                      f"detail/min={det * 60 / el:.1f}  "
                      f"avg page={stats['page_time'] / seen:.1f}s  "
                      f"avg detail={avg_det:.2f}s  slow={stats['slows']}")
            # periodic bid_history rebuild
            refresh_history()
            # stop conditions
            if status == 200 and not lots:
                return                                  # past end of archive
            if status != 200:
                # a bad page is retried inside fetch(); a *streak* means the site
                # is down / blocking us — stop instead of spinning to page ∞.
                err_run += 1
                if err_run >= args.max_error_streak:
                    print(f"{err_run} failed pages in a row -> stop "
                          f"(site unreachable or rate-limited?)")
                    return
            else:
                err_run = 0
            if frontier and status == 200 and lots and new == 0:
                known_run += 1
                if known_run >= args.stop_after:
                    print(f"repeated {known_run} page(s) in a row -> stop")
                    return
            else:
                known_run = 0

    for start, frontier in plan:
        if STOP.is_set():
            break
        walk(start, frontier)

    ex.shutdown(wait=True)
    client.close()
    if _SIGINT.is_set():
        print("interrupted — finishing in-flight pages, then rebuilding bid_history…")
    refresh_history(force=True)
    el = time.time() - t0
    seen = max(stats["pages_seen"], 1)
    det = stats["detail_fetched"]
    avg_det = (stats["detail_time"] / det) if det else 0.0
    undetailed = db.undetailed_count() if (db and db.enabled) else None
    print(f"DONE ok={stats['pages_ok']} empty={stats['pages_empty']} "
          f"lots={stats['lots']:,} new={stats['new_lots']:,} "
          f"detail={stats['detail_fetched']:,} fail={stats['detail_failed']:,} "
          f"sales={stats['sales']:,} bh_vins={stats['bh_vins']:,}"
          + (f" undetailed={undetailed:,}" if undetailed is not None else ""))
    print(f"TIME {el:.0f}s  pages={stats['pages_seen']}  "
          f"lots/min={stats['lots'] * 60 / el:.1f}  "
          f"detail/min={det * 60 / el:.1f}  "
          f"avg page={stats['page_time'] / seen:.1f}s  "
          f"avg list={stats['fetch_time'] / seen:.2f}s  "
          f"avg detail={avg_det:.2f}s  slow(>={args.slow_seconds:g}s)={stats['slows']}")


if __name__ == "__main__":
    main()
